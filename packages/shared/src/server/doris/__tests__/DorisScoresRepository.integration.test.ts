import { afterAll, beforeAll, describe, expect, it } from "vitest";

import { DorisPoCMysqlClient } from "../../doris-poc/mysqlClient";
import { readDorisScoresForPublicApi } from "../../repositories/telemetry/doris/publicScores";
import { DorisScoresRepository } from "../../repositories/telemetry/doris/scores";
import type { DorisTrace } from "../../repositories/telemetry/doris/traces";

const enabled = process.env.DORIS_POC_ENABLED === "1";
const describeDoris = enabled ? describe : describe.skip;
const projectId = "doris-score-read-integration";

describeDoris("Doris score repository", () => {
  let db: DorisPoCMysqlClient;
  let repository: DorisScoresRepository;

  beforeAll(async () => {
    db = new DorisPoCMysqlClient({
      host: process.env.DORIS_POC_FE_HOST ?? "127.0.0.1",
      port: Number(process.env.DORIS_POC_FE_MYSQL_PORT ?? "9031"),
      user: process.env.DORIS_POC_USER ?? "root",
      password: process.env.DORIS_POC_PASSWORD ?? "",
      database: "langfuse_poc",
    });
    const query = async <T extends object>(
      sql: string,
      params?: readonly unknown[],
    ) => (await db.query(sql, params)) as readonly T[];
    repository = new DorisScoresRepository({
      query,
      locateScore: async ({ projectId: scopeProjectId, scoreId }) =>
        scopeProjectId === projectId && scoreId === "score-numeric"
          ? [{ partitionDate: "2026-07-17", scoreId }]
          : [],
    });

    const insertScore = (input: {
      projectId: string;
      scoreId: string;
      traceId: string;
      name: string;
      dataType: string;
      value: number;
      stringValue?: string | null;
      booleanValue?: boolean | null;
      timestamp: string;
    }) =>
      db.execute(
        `INSERT INTO scores_current
          (project_id, score_date, score_id, version_token, trace_id,
           observation_id, session_id, name, source, data_type, value,
           string_value, boolean_value, environment, metadata, timestamp,
           created_at, updated_at)
         VALUES (?, '2026-07-17', ?, 1000, ?, 'span-1', 'session-1', ?,
           'API', ?, ?, ?, ?, 'production', CAST('{"region":"eu"}' AS VARIANT),
           ?, ?, ?)`,
        [
          input.projectId,
          input.scoreId,
          input.traceId,
          input.name,
          input.dataType,
          input.value,
          input.stringValue ?? null,
          input.booleanValue ?? null,
          input.timestamp,
          input.timestamp,
          input.timestamp,
        ],
      );

    await insertScore({
      projectId,
      scoreId: "score-numeric",
      traceId: "trace-1",
      name: "quality",
      dataType: "NUMERIC",
      value: 0.9,
      timestamp: "2026-07-17 10:00:00.000000",
    });
    await insertScore({
      projectId,
      scoreId: "score-categorical",
      traceId: "trace-1",
      name: "label|with|delimiter",
      dataType: "CATEGORICAL",
      value: 0,
      stringValue: "great",
      timestamp: "2026-07-17 10:00:01.000000",
    });
    await insertScore({
      projectId,
      scoreId: "score-boolean",
      traceId: "trace-2",
      name: "approved",
      dataType: "BOOLEAN",
      value: 1,
      stringValue: "True",
      booleanValue: true,
      timestamp: "2026-07-17 10:00:02.000000",
    });
    await insertScore({
      projectId,
      scoreId: "score-text",
      traceId: "trace-2",
      name: "comment",
      dataType: "TEXT",
      value: 0,
      stringValue: "useful",
      timestamp: "2026-07-17 10:00:03.000000",
    });
    await insertScore({
      projectId: "doris-score-other-project",
      scoreId: "score-numeric",
      traceId: "trace-1",
      name: "other tenant",
      dataType: "NUMERIC",
      value: 1,
      timestamp: "2026-07-17 10:00:04.000000",
    });
    await insertScore({
      projectId: "doris-score-deleted-project",
      scoreId: "score-deleted",
      traceId: "deleted-trace",
      name: "deleted",
      dataType: "NUMERIC",
      value: 1,
      timestamp: "2026-07-17 10:00:05.000000",
    });
    await db.execute(
      `INSERT INTO trace_tombstones
        (project_id, trace_id, deletion_generation, created_at)
       VALUES ('doris-score-deleted-project', 'deleted-trace', 1, NOW())`,
    );
  }, 60_000);

  afterAll(async () => {
    await db?.end();
  });

  const range = {
    from: new Date("2026-07-17T00:00:00.000Z"),
    to: new Date("2026-07-18T00:00:00.000Z"),
  };

  it("paginates all score types without gaps, duplicates, or tenant leaks", async () => {
    const first = await repository.list({
      projectId,
      range,
      filters: [],
      limit: 2,
    });
    const second = await repository.list({
      projectId,
      range,
      filters: [],
      limit: 2,
      cursor: first.nextCursor ?? undefined,
    });

    expect(first.nextCursor).not.toBeNull();
    expect([...first.items, ...second.items].map(({ id }) => id)).toEqual([
      "score-text",
      "score-boolean",
      "score-categorical",
      "score-numeric",
    ]);
    expect(second.nextCursor).toBeNull();
  });

  it("decodes point reads and grouped score values through real Doris", async () => {
    await expect(
      repository.get({ projectId, scoreId: "score-numeric" }),
    ).resolves.toEqual(
      expect.objectContaining({
        id: "score-numeric",
        value: 0.9,
        metadata: { region: "eu" },
      }),
    );
    await expect(
      repository.aggregateGroups({
        projectId,
        range,
        filters: [],
        columns: ["name", "dataType", "stringValue"],
        limit: 100,
      }),
    ).resolves.toEqual(
      expect.arrayContaining([
        {
          name: "label|with|delimiter",
          dataType: "CATEGORICAL",
          stringValue: "great",
          count: 1,
        },
      ]),
    );
  });

  it("keeps tombstoned scores invisible", async () => {
    const deletedRepository = new DorisScoresRepository({
      query: async <T extends object>(
        sql: string,
        params?: readonly unknown[],
      ) => (await db.query(sql, params)) as readonly T[],
    });
    await expect(
      deletedRepository.count({
        projectId: "doris-score-deleted-project",
        range,
        filters: [],
      }),
    ).resolves.toBe(0);
  });

  it("filters trace properties before public API pagination", async () => {
    await expect(
      readDorisScoresForPublicApi(
        {
          projectId,
          page: 1,
          limit: 1,
          userId: "target-user",
          fields: ["score", "trace"],
        },
        "v2",
        {
          list: repository.list.bind(repository),
          count: repository.count.bind(repository),
          getTrace: async ({ traceId }) =>
            ({
              id: traceId,
              projectId,
              timestamp: new Date("2026-07-17T09:00:00.000Z"),
              endTime: new Date("2026-07-17T09:00:01.000Z"),
              name: "trace",
              userId: traceId === "trace-1" ? "target-user" : "other-user",
              tags: ["prod"],
              environment: "production",
              sessionId: "session-1",
              release: null,
              version: null,
              inputPreview: null,
              outputPreview: null,
              rootObservationId: "span-1",
              fallbackObservationId: "span-1",
              incomplete: false,
              observationCount: 1,
              totalInputTokens: 0,
              totalOutputTokens: 0,
              totalUsage: 0,
              totalCost: null,
              latency: 1,
            }) satisfies DorisTrace,
        },
      ),
    ).resolves.toEqual({
      items: [expect.objectContaining({ id: "score-categorical" })],
      count: 2,
    });
  });
});
