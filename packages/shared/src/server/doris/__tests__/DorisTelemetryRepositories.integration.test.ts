import { afterAll, beforeAll, describe, expect, it } from "vitest";

import { DorisPoCMysqlClient } from "../../doris-poc/mysqlClient";
import { DorisObservationsRepository } from "../../repositories/telemetry/doris/observations";
import { DorisSessionsRepository } from "../../repositories/telemetry/doris/sessions";
import { DorisTracesRepository } from "../../repositories/telemetry/doris/traces";
import { DorisUsersRepository } from "../../repositories/telemetry/doris/users";

const ENABLED = process.env.DORIS_POC_ENABLED === "1";
const DB = "langfuse_poc";

describe.skipIf(!ENABLED)("Doris telemetry repositories", () => {
  let db: DorisPoCMysqlClient;
  let observations: DorisObservationsRepository;
  let sessions: DorisSessionsRepository;
  let traces: DorisTracesRepository;
  let users: DorisUsersRepository;

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
    traces = new DorisTracesRepository({
      query: async <T extends object>(
        sql: string,
        params?: readonly unknown[],
      ) => (await db.query(sql, params)) as readonly T[],
      locateTrace: async ({ projectId, traceId }) =>
        projectId === "trace-repository-project" && traceId === "rooted-trace"
          ? [
              {
                partitionDate: "2026-07-17",
                traceId: "rooted-trace",
                observationId: "root-span",
              },
              {
                partitionDate: "2026-07-17",
                traceId: "rooted-trace",
                observationId: "child-span",
              },
            ]
          : [],
    });
    const query = async <T extends object>(
      sql: string,
      params?: readonly unknown[],
    ) => (await db.query(sql, params)) as readonly T[];
    sessions = new DorisSessionsRepository({ query });
    users = new DorisUsersRepository({ query });
    await db.execute("TRUNCATE TABLE events_current");
    await db.execute("TRUNCATE TABLE trace_tombstones");
    await db.execute("TRUNCATE TABLE project_tombstones");

    const insert = async (input: {
      projectId: string;
      traceId: string;
      spanId: string;
      name: string;
      startTime: string;
      parentSpanId?: string | null;
      isAppRoot?: boolean;
      environment?: string;
      userId?: string;
      sessionId?: string;
    }) =>
      db.execute(
        `INSERT INTO events_current
          (project_id, partition_date, trace_id, span_id, parent_span_id,
           is_app_root, version_token, type, environment, name, user_id,
           session_id, start_time,
           end_time, completion_start_time,
           created_at, updated_at, source, ingestion_sdk_name,
           ingestion_sdk_version, tags, metadata, usage_details, cost_details,
           tool_definitions, tool_calls, tool_call_names, input, output,
           input_preview, output_preview, total_input_tokens, total_output_tokens,
         total_cost)
         VALUES (?, '2026-07-17', ?, ?, ?, ?, 1000, 'GENERATION', ?, ?, ?, ?, ?,
           ?, ?, ?, ?,
           'api', 'js', '5.0.0', ARRAY('prod'), CAST(? AS VARIANT),
           CAST(? AS VARIANT), CAST(? AS VARIANT), CAST(? AS VARIANT),
           ARRAY('search'), ARRAY('search'), ?, ?, ?, ?, 10, 5, 0.125)`,
        [
          input.projectId,
          input.traceId,
          input.spanId,
          input.parentSpanId ?? null,
          input.isAppRoot ?? false,
          input.environment ?? "production",
          input.name,
          input.userId ?? null,
          input.sessionId ?? null,
          input.startTime,
          input.startTime,
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
    await insert({
      projectId: "trace-repository-project",
      traceId: "rooted-trace",
      spanId: "root-span",
      name: "real root",
      startTime: "2026-07-17 12:00:00.000000",
      isAppRoot: true,
    });
    await insert({
      projectId: "trace-repository-project",
      traceId: "rooted-trace",
      spanId: "child-span",
      name: "child",
      startTime: "2026-07-17 12:00:01.000000",
      parentSpanId: "root-span",
    });
    await insert({
      projectId: "trace-repository-project",
      traceId: "fallback-trace",
      spanId: "fallback-span",
      name: "fallback",
      startTime: "2026-07-17 11:00:00.000000",
    });
    await insert({
      projectId: "derived-repository-project",
      traceId: "derived-trace-a",
      spanId: "derived-span-a",
      name: "derived a",
      startTime: "2026-07-17 13:00:00.000000",
      userId: "user-a",
      sessionId: "session-shared",
    });
    await insert({
      projectId: "user-filter-project",
      traceId: "production-trace",
      spanId: "production-span",
      name: "production",
      startTime: "2026-07-17 14:00:00.000000",
      environment: "production",
      userId: "multi-environment-user",
    });
    await insert({
      projectId: "user-filter-project",
      traceId: "staging-trace",
      spanId: "staging-span",
      name: "staging",
      startTime: "2026-07-17 14:00:01.000000",
      environment: "staging",
      userId: "multi-environment-user",
    });
    await insert({
      projectId: "derived-repository-project",
      traceId: "derived-trace-b",
      spanId: "derived-span-b",
      name: "derived b",
      startTime: "2026-07-17 13:00:01.000000",
      userId: "user-b",
      sessionId: "session-shared",
    });
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

  it("applies position filters after the bounded event filters", async () => {
    const input = {
      projectId: "repository-project",
      range: {
        from: new Date("2026-07-17T00:00:00.000Z"),
        to: new Date("2026-07-18T00:00:00.000Z"),
      },
      limit: 10,
    };

    const first = await observations.list({
      ...input,
      filters: [
        {
          type: "positionInTrace" as const,
          column: "startTime",
          operator: "=" as const,
          key: "first" as const,
        },
      ],
    });
    const last = await observations.list({
      ...input,
      filters: [
        {
          type: "positionInTrace" as const,
          column: "startTime",
          operator: "=" as const,
          key: "last" as const,
        },
      ],
    });

    expect(first.items.map(({ id }) => id)).toEqual(["span-a"]);
    expect(last.items.map(({ id }) => id)).toEqual(["span-b"]);
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

  it("derives real-root and incomplete traces with stable pagination", async () => {
    const input = {
      projectId: "trace-repository-project",
      range: {
        from: new Date("2026-07-17T00:00:00.000Z"),
        to: new Date("2026-07-18T00:00:00.000Z"),
      },
      filters: [],
      limit: 1,
    };
    const first = await traces.list(input);
    const second = await traces.list({
      ...input,
      cursor: first.nextCursor ?? undefined,
    });

    expect(first.items).toEqual([
      expect.objectContaining({
        id: "rooted-trace",
        rootObservationId: "root-span",
        incomplete: false,
        observationCount: 2,
        totalUsage: 30,
      }),
    ]);
    expect(second.items).toEqual([
      expect.objectContaining({
        id: "fallback-trace",
        rootObservationId: null,
        fallbackObservationId: "fallback-span",
        incomplete: true,
      }),
    ]);
    await expect(
      traces.get({
        projectId: "trace-repository-project",
        traceId: "rooted-trace",
      }),
    ).resolves.toEqual(
      expect.objectContaining({
        id: "rooted-trace",
        rootObservationId: "root-span",
        observationCount: 2,
      }),
    );
  });

  it("derives bounded session and user aggregates from visible events", async () => {
    const input = {
      projectId: "derived-repository-project",
      range: {
        from: new Date("2026-07-17T00:00:00.000Z"),
        to: new Date("2026-07-18T00:00:00.000Z"),
      },
      filters: [],
      limit: 10,
    };

    await expect(sessions.list(input)).resolves.toMatchObject({
      items: [
        {
          id: "session-shared",
          traceIds: ["derived-trace-a", "derived-trace-b"],
          userIds: ["user-a", "user-b"],
          traceCount: 2,
          observationCount: 2,
          totalUsage: 30,
          totalCost: 0.25,
        },
      ],
    });
    await expect(
      sessions.get({
        projectId: input.projectId,
        sessionId: "session-shared",
        range: input.range,
      }),
    ).resolves.toEqual(
      expect.objectContaining({ id: "session-shared", traceCount: 2 }),
    );
    await expect(
      sessions.list({
        ...input,
        sessionFilters: [
          {
            type: "arrayOptions",
            column: "userIds",
            operator: "all of",
            value: ["user-a", "user-b"],
          },
          {
            type: "arrayOptions",
            column: "traceTags",
            operator: "all of",
            value: ["prod"],
          },
          {
            type: "stringObject",
            column: "metadata",
            key: "region",
            operator: "=",
            value: "eu",
          },
        ],
      }),
    ).resolves.toMatchObject({
      items: [{ id: "session-shared" }],
    });
    await expect(
      sessions.count({
        ...input,
        sessionFilters: [
          {
            type: "arrayOptions",
            column: "userIds",
            operator: "none of",
            value: ["user-a"],
          },
        ],
      }),
    ).resolves.toBe(0);

    const userPage = await users.list(input);
    expect(userPage.items.map(({ id }) => id).sort()).toEqual([
      "user-a",
      "user-b",
    ]);
    await expect(
      users.get({
        projectId: input.projectId,
        userId: "user-a",
        range: input.range,
      }),
    ).resolves.toEqual(
      expect.objectContaining({
        id: "user-a",
        traceCount: 1,
        observationCount: 1,
        totalUsage: 15,
      }),
    );
  });

  it("aggregates user metrics only from events matching the filter", async () => {
    await expect(
      users.list({
        projectId: "user-filter-project",
        range: {
          from: new Date("2026-07-17T00:00:00.000Z"),
          to: new Date("2026-07-18T00:00:00.000Z"),
        },
        filters: [
          {
            type: "stringOptions",
            column: "environment",
            operator: "any of",
            value: ["production"],
          },
        ],
        limit: 10,
      }),
    ).resolves.toMatchObject({
      items: [
        {
          id: "multi-environment-user",
          environments: ["production"],
          traceCount: 1,
          observationCount: 1,
          totalUsage: 15,
        },
      ],
    });
  });
});
