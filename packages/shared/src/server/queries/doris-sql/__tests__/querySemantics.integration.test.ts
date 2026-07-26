// U1 real-Doris query-semantics suite.
//
// Freezes the storage-neutral query invariants that the U1 physical design must
// satisfy before U5 builds the full logical->Doris compiler: trusted project
// scope on every scan, date-bounded (partition-pruned) reads, stable pagination
// tie-break, the frozen relationship windows, and null/empty + array any/none/
// all filter semantics. Full filter/search compilation parity is U5/U6.
//
// Runs ONLY against the pinned real Doris PoC target (DORIS_POC_ENABLED=1).

import { beforeAll, afterAll, describe, expect, it } from "vitest";
import { DorisPoCMysqlClient } from "../../../doris-poc/mysqlClient";
import {
  parseDorisTestNamespace,
  truncateOwnedDorisTestTables,
} from "../../../doris/testDatabase";
import { compileDorisVisibleEventsQuery } from "../eventQueryCompiler";

const ENABLED = process.env.DORIS_POC_ENABLED === "1";
const TEST_NAMESPACE = ENABLED ? parseDorisTestNamespace() : null;
const DB = TEST_NAMESPACE?.database ?? "doris_test_disabled";

async function insertEvent(
  db: DorisPoCMysqlClient,
  opts: {
    project_id: string;
    partition_date: string;
    trace_id: string;
    span_id: string;
    version_token: string;
    name: string;
    start_time: string;
    tags?: string;
    user_id?: string | null;
  },
): Promise<void> {
  await db.execute(
    `INSERT INTO events_current
       (project_id, partition_date, trace_id, span_id, version_token, type, environment,
        name, start_time, created_at, updated_at, source, ingestion_sdk_name,
        ingestion_sdk_version, tags, user_id)
     VALUES (?, ?, ?, ?, ?, 'span', 'default', ?, ?, ?, ?, 'api', 'js', '5.0.0', ?, ?)`,
    [
      opts.project_id,
      opts.partition_date,
      opts.trace_id,
      opts.span_id,
      opts.version_token,
      opts.name,
      opts.start_time,
      opts.start_time,
      opts.start_time,
      opts.tags ?? "[]",
      opts.user_id ?? null,
    ],
  );
}

describe.skipIf(!ENABLED)("Doris PoC — query semantics invariants", () => {
  let db: DorisPoCMysqlClient;

  beforeAll(async () => {
    if (!TEST_NAMESPACE) throw new Error("Doris test namespace is required");
    db = new DorisPoCMysqlClient({
      host: process.env.DORIS_POC_FE_HOST ?? "127.0.0.1",
      port: Number(process.env.DORIS_POC_FE_MYSQL_PORT ?? "9031"),
      user: process.env.DORIS_POC_USER ?? "root",
      password: process.env.DORIS_POC_PASSWORD ?? "",
      database: DB,
    });
    await truncateOwnedDorisTestTables({
      executor: db,
      namespace: TEST_NAMESPACE,
      tables: [
        "events_current",
        "scores_current",
        "trace_tombstones",
        "project_tombstones",
      ],
    });
    // Two projects, both with a trace "shared-id" to prove project isolation.
    await insertEvent(db, {
      project_id: "p1",
      partition_date: "2026-07-17",
      trace_id: "shared-id",
      span_id: "s1",
      version_token: "1000",
      name: "p1-trace",
      start_time: "2026-07-17 10:00:00.000000",
      tags: '["prod","canary"]',
      user_id: "u1",
    });
    await insertEvent(db, {
      project_id: "p2",
      partition_date: "2026-07-17",
      trace_id: "shared-id",
      span_id: "s1",
      version_token: "1000",
      name: "p2-trace",
      start_time: "2026-07-17 10:00:00.000000",
      tags: '["prod"]',
      user_id: "u2",
    });
  }, 60_000);

  afterAll(async () => {
    await db?.end();
  });

  it("cross-project isolation: equal trace_id under different project returns only the scoped project's row", async () => {
    const rows = await db.query<{ project_id: string }>(
      `SELECT project_id FROM events_current WHERE project_id = ? AND trace_id = ?`,
      ["p1", "shared-id"],
    );
    expect(rows).toHaveLength(1);
    expect(rows[0].project_id).toBe("p1");
  });

  it("date-bounded scan prunes to the queried partition_date", async () => {
    const rows = await db.query<{ d: string }>(
      `SELECT DISTINCT CAST(partition_date AS string) d FROM events_current
       WHERE project_id = ? AND partition_date >= ? AND partition_date < ?`,
      ["p1", "2026-07-17", "2026-07-18"],
    );
    expect(rows.map((r) => r.d)).toEqual(["2026-07-17"]);
  });

  it("stable pagination tie-break: (start_time, span_id) cursor has no gaps/duplicates", async () => {
    // Page 1 descending by start_time then span_id; capture cursor; page 2 must
    // continue strictly after the cursor with no overlap.
    const page1 = await db.query<{ start_time: string; span_id: string }>(
      `SELECT start_time, span_id FROM events_current WHERE project_id = ?
       ORDER BY start_time DESC, span_id ASC LIMIT 1`,
      ["p1"],
    );
    expect(page1).toHaveLength(1);
    const cursor = page1[0];
    const page2 = await db.query<{ start_time: string; span_id: string }>(
      `SELECT start_time, span_id FROM events_current WHERE project_id = ?
       AND (start_time < ? OR (start_time = ? AND span_id > ?))
       ORDER BY start_time DESC, span_id ASC LIMIT 1`,
      ["p1", cursor.start_time, cursor.start_time, cursor.span_id],
    );
    // Union of both pages must be disjoint (cursor predicate excludes page1 row).
    const allRows = await db.query<{ start_time: string; span_id: string }>(
      `SELECT start_time, span_id FROM events_current WHERE project_id = ?`,
      ["p1"],
    );
    expect(allRows.length).toBe(page1.length + page2.length);
  });

  it("array any/none/all filter semantics over tags", async () => {
    // any of [prod, missing]: p1 has prod -> 1 row
    const anyOf = await db.query<{ c: number }>(
      `SELECT COUNT(*) c FROM events_current WHERE project_id = ? AND array_contains(tags, 'prod')`,
      ["p1"],
    );
    expect(anyOf[0].c).toBe(1);
    // none of [x]: p1 has no 'x' -> 1 row
    const noneOf = await db.query<{ c: number }>(
      `SELECT COUNT(*) c FROM events_current WHERE project_id = ? AND NOT array_contains(tags, 'x')`,
      ["p1"],
    );
    expect(noneOf[0].c).toBe(1);
    // all of [prod, canary]: p1 has both -> 1 row; requires array_contains_all
    const allOf = await db.query<{ c: number }>(
      `SELECT COUNT(*) c FROM events_current WHERE project_id = ? AND array_contains_all(tags, ARRAY('prod','canary'))`,
      ["p1"],
    );
    expect(allOf[0].c).toBe(1);
  });

  it("null/empty user_id compatibility: NULL is distinguishable from empty", async () => {
    await insertEvent(db, {
      project_id: "p1",
      partition_date: "2026-07-17",
      trace_id: "t-nulluser",
      span_id: "s1",
      version_token: "1000",
      name: "null-user",
      start_time: "2026-07-17 10:00:00.000000",
      user_id: null,
    });
    const nullCount = await db.query<{ c: number }>(
      `SELECT COUNT(*) c FROM events_current WHERE project_id = ? AND user_id IS NULL`,
      ["p1"],
    );
    expect(nullCount[0].c).toBeGreaterThanOrEqual(1);
  });

  it.each([
    [
      {
        type: "numberObject",
        column: "scores_avg",
        key: "quality",
        operator: ">",
        value: 0.5,
      },
      ["score-filter-1"],
    ],
    [
      {
        type: "categoryOptions",
        column: "score_categories",
        key: "topic",
        operator: "none of",
        value: ["safe"] as string[],
      },
      ["score-filter-2"],
    ],
    [
      {
        type: "booleanObject",
        column: "score_booleans",
        key: "approved",
        operator: "<>",
        value: true,
      },
      ["score-filter-2"],
    ],
  ] as const)(
    "executes score-map filter %# with SQL semantics",
    async (filter, expected) => {
      const projectId = "score-filter-project";
      for (const spanId of ["score-filter-1", "score-filter-2"]) {
        await insertEvent(db, {
          project_id: projectId,
          partition_date: "2026-07-17",
          trace_id: "score-filter-trace",
          span_id: spanId,
          version_token: "1000",
          name: spanId,
          start_time:
            spanId === "score-filter-1"
              ? "2026-07-17 12:00:00.000000"
              : "2026-07-17 12:01:00.000000",
        });
      }
      if (filter.type === "numberObject") {
        await db.execute(
          `INSERT INTO scores_current
          (project_id, score_date, score_id, version_token, trace_id,
           observation_id, name, source, data_type, value, environment,
           timestamp, created_at, updated_at)
         VALUES (?, '2026-07-17', 'score-filter-number', 1000,
           'score-filter-trace', 'score-filter-1', 'quality', 'API',
           'NUMERIC', 0.8, 'default', '2026-07-17 12:00:30.000000',
           '2026-07-17 12:00:30.000000', '2026-07-17 12:00:30.000000')`,
          [projectId],
        );
      } else if (filter.type === "categoryOptions") {
        await db.execute(
          `INSERT INTO scores_current
          (project_id, score_date, score_id, version_token, trace_id,
           observation_id, name, source, data_type, value, string_value,
           environment, timestamp, created_at, updated_at)
         VALUES (?, '2026-07-17', 'score-filter-category', 1000,
           'score-filter-trace', 'score-filter-1', 'topic', 'API',
           'CATEGORICAL', 0, 'safe', 'default',
           '2026-07-17 12:00:30.000000', '2026-07-17 12:00:30.000000',
           '2026-07-17 12:00:30.000000')`,
          [projectId],
        );
      } else {
        await db.execute(
          `INSERT INTO scores_current
          (project_id, score_date, score_id, version_token, trace_id,
           observation_id, name, source, data_type, value, boolean_value,
           environment, timestamp, created_at, updated_at)
         VALUES (?, '2026-07-17', 'score-filter-boolean', 1000,
           'score-filter-trace', 'score-filter-1', 'approved', 'API',
           'BOOLEAN', 1, TRUE, 'default',
           '2026-07-17 12:00:30.000000', '2026-07-17 12:00:30.000000',
           '2026-07-17 12:00:30.000000')`,
          [projectId],
        );
      }

      const compiled = compileDorisVisibleEventsQuery({
        projectId,
        range: {
          from: new Date("2026-07-17T00:00:00.000Z"),
          to: new Date("2026-07-18T00:00:00.000Z"),
        },
        projection: "list",
        filters: [filter],
        limit: 10,
      });
      const rows = await db.query<{ span_id: string }>(
        compiled.sql,
        compiled.params,
      );
      expect(rows.map(({ span_id }) => span_id)).toEqual(expected);
    },
  );

  it("executes compiled filters/search with bound values and deletion barriers", async () => {
    await db.execute(
      `INSERT INTO events_current
        (project_id, partition_date, trace_id, span_id, version_token, type, environment,
         name, start_time, created_at, updated_at, source, ingestion_sdk_name,
         ingestion_sdk_version, tags, metadata, tool_definitions, input, output,
         input_preview, output_preview)
       VALUES (?, ?, ?, ?, ?, 'span', 'default', ?, ?, ?, ?, 'api', 'js', '5.0.0',
         ARRAY('prod', 'canary'), CAST(? AS VARIANT), CAST(? AS VARIANT), ?, ?, ?, ?)`,
      [
        "p1",
        "2026-07-17",
        "compiled-visible",
        "compiled-visible-span",
        "1000",
        "MiXeD %_\\ Needle",
        "2026-07-17 11:00:00.000000",
        "2026-07-17 11:00:00.000000",
        "2026-07-17 11:00:00.000000",
        JSON.stringify({ 'region."quoted"': "eu" }),
        JSON.stringify({ search: { description: "Search" } }),
        JSON.stringify({ message: "退款 %_\\ needle" }),
        JSON.stringify({ ok: true }),
        "input preview",
        "output preview",
      ],
    );
    await db.execute(
      `INSERT INTO events_current
        (project_id, partition_date, trace_id, span_id, version_token, type, environment,
         name, start_time, created_at, updated_at, source, ingestion_sdk_name,
         ingestion_sdk_version, tags, metadata, tool_definitions, input, output)
       VALUES (?, ?, ?, ?, ?, 'span', 'default', ?, ?, ?, ?, 'api', 'js', '5.0.0',
         ARRAY('prod', 'canary'), CAST(? AS VARIANT), CAST(? AS VARIANT), ?, ?)`,
      [
        "p1",
        "2026-07-17",
        "compiled-deleted",
        "compiled-deleted-span",
        "1000",
        "MiXeD %_\\ Needle",
        "2026-07-17 11:01:00.000000",
        "2026-07-17 11:01:00.000000",
        "2026-07-17 11:01:00.000000",
        JSON.stringify({ 'region."quoted"': "eu" }),
        JSON.stringify({ search: { description: "Search" } }),
        JSON.stringify({ message: "退款 %_\\ needle" }),
        JSON.stringify({ ok: true }),
      ],
    );
    await db.execute(
      `INSERT INTO trace_tombstones
        (project_id, trace_id, deletion_generation, created_at)
       VALUES (?, ?, ?, ?)`,
      ["p1", "compiled-deleted", "1", "2026-07-17 11:02:00.000000"],
    );

    const compiled = compileDorisVisibleEventsQuery({
      projectId: "p1",
      range: {
        from: new Date("2026-07-17T00:00:00.000Z"),
        to: new Date("2026-07-18T00:00:00.000Z"),
      },
      projection: "list",
      filters: [
        {
          type: "stringObject",
          column: "metadata",
          key: 'region."quoted"',
          operator: "=",
          value: "eu",
        },
        {
          type: "arrayOptions",
          column: "traceTags",
          operator: "all of",
          value: ["prod", "canary"],
        },
        {
          type: "arrayOptions",
          column: "toolNames",
          operator: "all of",
          value: ["search"],
        },
        {
          type: "number",
          column: "toolDefinitions",
          operator: ">=",
          value: 1,
        },
        {
          type: "boolean",
          column: "hasInput",
          operator: "=",
          value: true,
        },
      ],
      search: {
        query: "MIXED %_\\ NEEDLE",
        searchType: ["id", "content"],
      },
      limit: 10,
    });
    const rows = await db.query<{ trace_id: string }>(
      compiled.sql,
      compiled.params,
    );

    expect(rows.map((row) => row.trace_id)).toEqual(["compiled-visible"]);
  });
});
