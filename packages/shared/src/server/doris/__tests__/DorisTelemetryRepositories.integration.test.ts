import { afterAll, beforeAll, describe, expect, it } from "vitest";

import { DorisPoCMysqlClient } from "../../doris-poc/mysqlClient";
import { DorisObservationsRepository } from "../../repositories/telemetry/doris/observations";

const ENABLED = process.env.DORIS_POC_ENABLED === "1";
const DB = "langfuse_poc";

describe.skipIf(!ENABLED)("Doris telemetry repositories", () => {
  let db: DorisPoCMysqlClient;
  let observations: DorisObservationsRepository;

  beforeAll(async () => {
    db = new DorisPoCMysqlClient({
      host: process.env.DORIS_POC_FE_HOST ?? "127.0.0.1",
      port: Number(process.env.DORIS_POC_FE_MYSQL_PORT ?? "9031"),
      user: process.env.DORIS_POC_USER ?? "root",
      password: process.env.DORIS_POC_PASSWORD ?? "",
      database: DB,
    });
    observations = new DorisObservationsRepository({
      query: async <T extends object>(
        sql: string,
        params?: readonly unknown[],
      ) => (await db.query(sql, params)) as readonly T[],
      locateObservation: async ({ projectId, observationId, traceId }) =>
        projectId === "repository-project" &&
        observationId === "span-b" &&
        (!traceId || traceId === "trace-1")
          ? [
              {
                partitionDate: "2026-07-17",
                traceId: "trace-1",
                observationId: "span-b",
              },
            ]
          : [],
    });
    await db.execute("TRUNCATE TABLE events_current");
    await db.execute("TRUNCATE TABLE trace_tombstones");
    await db.execute("TRUNCATE TABLE project_tombstones");

    const insert = async (input: {
      projectId: string;
      traceId: string;
      spanId: string;
      name: string;
      startTime: string;
    }) =>
      db.execute(
        `INSERT INTO events_current
          (project_id, partition_date, trace_id, span_id, version_token, type,
           environment, name, start_time, end_time, completion_start_time,
           created_at, updated_at, source, ingestion_sdk_name,
           ingestion_sdk_version, tags, metadata, usage_details, cost_details,
           tool_definitions, tool_calls, tool_call_names, input, output,
           input_preview, output_preview, total_input_tokens, total_output_tokens,
           total_cost)
         VALUES (?, '2026-07-17', ?, ?, 1000, 'GENERATION', 'production', ?, ?,
           '2026-07-17 10:00:02.500000', '2026-07-17 10:00:00.500000', ?, ?,
           'api', 'js', '5.0.0', ARRAY('prod'), CAST(? AS VARIANT),
           CAST(? AS VARIANT), CAST(? AS VARIANT), CAST(? AS VARIANT),
           ARRAY('search'), ARRAY('search'), ?, ?, ?, ?, 10, 5, 0.125)`,
        [
          input.projectId,
          input.traceId,
          input.spanId,
          input.name,
          input.startTime,
          input.startTime,
          input.startTime,
          JSON.stringify({ region: "eu" }),
          JSON.stringify({ input: 10, output: 5, total: 15 }),
          JSON.stringify({ input: 0.1, output: 0.025, total: 0.125 }),
          JSON.stringify({ search: "{}" }),
          JSON.stringify({ question: "价格" }),
          JSON.stringify({ answer: "ok" }),
          "input preview",
          "output preview",
        ],
      );

    await insert({
      projectId: "repository-project",
      traceId: "trace-1",
      spanId: "span-a",
      name: "a",
      startTime: "2026-07-17 10:00:00.000000",
    });
    await insert({
      projectId: "repository-project",
      traceId: "trace-1",
      spanId: "span-b",
      name: "b",
      startTime: "2026-07-17 10:00:00.000000",
    });
    await insert({
      projectId: "other-project",
      traceId: "trace-1",
      spanId: "span-b",
      name: "other tenant",
      startTime: "2026-07-17 10:00:00.000000",
    });
    await insert({
      projectId: "trace-deleted-project",
      traceId: "deleted-trace",
      spanId: "deleted-span",
      name: "deleted",
      startTime: "2026-07-17 10:00:00.000000",
    });
    await db.execute(
      `INSERT INTO trace_tombstones
        (project_id, trace_id, deletion_generation, created_at)
       VALUES ('trace-deleted-project', 'deleted-trace', 1, NOW())`,
    );
    await insert({
      projectId: "project-deleted-project",
      traceId: "orphan-trace",
      spanId: "orphan-span",
      name: "orphan",
      startTime: "2026-07-17 10:00:00.000000",
    });
    await db.execute(
      `INSERT INTO project_tombstones
        (project_id, deletion_generation, created_at)
       VALUES ('project-deleted-project', 1, NOW())`,
    );
  }, 60_000);

  afterAll(async () => {
    await db?.end();
  });

  it("paginates without gaps, duplicates, or cross-project rows", async () => {
    const query = {
      range: {
        from: new Date("2026-07-17T00:00:00.000Z"),
        to: new Date("2026-07-18T00:00:00.000Z"),
      },
      filters: [],
      limit: 1,
    };
    const first = await observations.list({
      ...query,
      projectId: "repository-project",
    });
    const second = await observations.list({
      ...query,
      projectId: "repository-project",
      cursor: first.nextCursor ?? undefined,
    });

    expect(first.nextCursor).not.toBeNull();
    expect([...first.items, ...second.items].map(({ id }) => id)).toEqual([
      "span-b",
      "span-a",
    ]);
  });

  it("uses the locator for full detail and decodes canonical fields", async () => {
    await expect(
      observations.get({
        projectId: "repository-project",
        observationId: "span-b",
      }),
    ).resolves.toEqual(
      expect.objectContaining({
        id: "span-b",
        traceId: "trace-1",
        input: { question: "价格" },
        output: { answer: "ok" },
        metadata: { region: "eu" },
        totalUsage: 15,
        totalCost: 0.125,
      }),
    );
  });

  it("keeps trace- and project-tombstoned rows invisible", async () => {
    const input = {
      range: {
        from: new Date("2026-07-17T00:00:00.000Z"),
        to: new Date("2026-07-18T00:00:00.000Z"),
      },
      filters: [],
      limit: 10,
    };
    await expect(
      observations.list({ ...input, projectId: "trace-deleted-project" }),
    ).resolves.toMatchObject({ items: [] });
    await expect(
      observations.list({ ...input, projectId: "project-deleted-project" }),
    ).resolves.toMatchObject({ items: [] });
  });
});
