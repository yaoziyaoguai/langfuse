import { BatchExportFileFormat } from "@langfuse/shared";
import { describe, expect, it, vi } from "vitest";

vi.mock("@langfuse/shared/src/server", () => ({
  buildDorisTraceReadQuery: vi.fn(async (_projectId, filters) => ({
    range: {
      from: new Date("2026-07-01T00:00:00.000Z"),
      to: new Date("2026-07-21T00:00:00.000Z"),
    },
    filters,
    impossible: false,
  })),
  buildDorisLegacyObservationQuery: vi.fn((filters) => ({
    range: {
      from: new Date("2026-07-01T00:00:00.000Z"),
      to: new Date("2026-07-21T00:00:00.000Z"),
    },
    filters,
  })),
  buildDorisBatchExportScoreQuery: vi.fn((filters) => ({
    range: {
      from: new Date("2026-07-01T00:00:00.000Z"),
      to: new Date("2026-07-21T00:00:00.000Z"),
    },
    filters,
  })),
  buildDorisDerivedQuery: vi.fn((filters) => ({
    range: {
      from: new Date("2026-07-01T00:00:00.000Z"),
      to: new Date("2026-07-21T00:00:00.000Z"),
    },
    filters,
    sessionFilters: filters,
  })),
  getDatasetItems: vi.fn(),
  getDorisTelemetryRepositories: vi.fn(),
}));
vi.mock("@langfuse/shared/src/db", () => ({
  prisma: { auditLog: { findMany: vi.fn() } },
}));

import { DorisBatchExportIdentitySource } from "./DorisBatchExportIdentitySource";

async function collect<T>(values: AsyncIterable<T>): Promise<T[]> {
  const rows: T[] = [];
  for await (const value of values) rows.push(value);
  return rows;
}

const baseRequest = {
  projectId: "project-1",
  cutoffCreatedAt: new Date("2026-07-20T00:00:00.000Z"),
  filter: [],
  orderBy: null,
  fileFormat: BatchExportFileFormat.JSONL,
};

function source() {
  const scans = {
    traces: vi.fn(async function* () {
      yield { id: "trace-a" };
    }),
    observations: vi.fn(async function* () {
      yield { id: "span-a", traceId: "trace-a" };
    }),
    scores: vi.fn(async function* () {
      yield { id: "score-a" };
    }),
    sessions: vi.fn(async function* () {
      yield { id: "session-a" };
    }),
    datasetRunItems: vi.fn().mockResolvedValue([
      {
        id: "run-item-a",
      },
    ]),
  };
  const datasetItemIds = vi.fn().mockResolvedValue(["item-b", "item-a"]);
  const auditLogIds = vi.fn().mockResolvedValue(["audit-a"]);
  return {
    scans,
    datasetItemIds,
    auditLogIds,
    value: new DorisBatchExportIdentitySource({
      repositories: {
        traces: { scanIdentities: scans.traces },
        observations: { scanIdentities: scans.observations },
        scores: { scanIdentities: scans.scores },
        sessions: { scanIdentities: scans.sessions },
        datasetRunItems: { list: scans.datasetRunItems },
      } as never,
      datasetItemIds,
      auditLogIds,
    }),
  };
}

describe("DorisBatchExportIdentitySource", () => {
  it.each([
    ["traces", "traces", { id: "trace-a" }],
    ["observations", "observations", { id: "span-a", traceId: "trace-a" }],
    ["events", "observations", { id: "span-a", traceId: "trace-a" }],
    ["scores", "scores", { id: "score-a" }],
    ["sessions", "sessions", { id: "session-a" }],
  ] as const)(
    "routes %s through its canonical identity scan",
    async (tableName, repository, expected) => {
      const test = source();
      await expect(
        collect(test.value.scan({ ...baseRequest, tableName }, 123)),
      ).resolves.toEqual([expected]);
      expect(test.scans[repository]).toHaveBeenCalledWith(
        expect.objectContaining({ projectId: "project-1", limit: 123 }),
      );
    },
  );

  it("uses the smaller requested limit and sorts PostgreSQL dataset identities", async () => {
    const test = source();
    await expect(
      collect(
        test.value.scan(
          { ...baseRequest, tableName: "dataset_items", limit: 2 },
          100,
        ),
      ),
    ).resolves.toEqual([{ id: "item-a" }, { id: "item-b" }]);
    expect(test.datasetItemIds).toHaveBeenCalledWith(
      expect.objectContaining({ limit: 2 }),
    );
  });

  it("scans visible dataset-run identities in stable order", async () => {
    const test = source();
    await expect(
      collect(
        test.value.scan(
          { ...baseRequest, tableName: "dataset_run_items" },
          100,
        ),
      ),
    ).resolves.toEqual([{ id: "run-item-a" }]);
    expect(test.scans.datasetRunItems).toHaveBeenCalledWith(
      expect.objectContaining({
        projectId: "project-1",
        orderBy: { column: "id", order: "ASC" },
        limit: 100,
        includeIO: false,
      }),
    );
  });
});
