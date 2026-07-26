import Decimal from "decimal.js";
import { beforeEach, describe, expect, it, vi } from "vitest";

const mocks = vi.hoisted(() => ({
  getDistinctScoreNames: vi.fn(),
  getTracesByIds: vi.fn(),
  getTracesTable: vi.fn(),
  getTracesTableMetrics: vi.fn(),
  getScoresForTraces: vi.fn(),
  fetchCommentsForExport: vi.fn(),
}));

vi.mock("@langfuse/shared/src/db", () => ({
  prisma: {},
}));

vi.mock("@langfuse/shared/src/server", async () => {
  const { Readable } = await import("node:stream");
  class DatabaseReadStream<T> extends Readable {
    private offset = 0;
    private reading = false;

    constructor(
      private readonly delegate: (
        pageSize: number,
        offset: number,
      ) => Promise<T[]>,
      private readonly pageSize: number,
      private readonly maxRecords?: number,
    ) {
      super({ objectMode: true });
    }

    async _read() {
      if (this.reading) return;
      if (this.maxRecords && this.offset >= this.maxRecords) {
        this.push(null);
        return;
      }
      this.reading = true;
      try {
        const rows = await this.delegate(this.pageSize, this.offset);
        if (rows.length === 0) {
          this.push(null);
          return;
        }
        rows.forEach((row) => this.push(row));
        this.offset += this.pageSize;
      } catch (error) {
        this.destroy(error as Error);
      } finally {
        this.reading = false;
      }
    }
  }
  return {
    DatabaseReadStream,
    getDistinctScoreNames: mocks.getDistinctScoreNames,
    getScoresForTraces: mocks.getScoresForTraces,
    getTracesByIds: mocks.getTracesByIds,
    getTracesTable: mocks.getTracesTable,
    getTracesTableMetrics: mocks.getTracesTableMetrics,
  };
});

vi.mock("../../env", () => ({
  env: { BATCH_EXPORT_PAGE_SIZE: 2, BATCH_EXPORT_ROW_LIMIT: 100 },
}));

vi.mock("./fetchCommentsForExport", () => ({
  fetchCommentsForExport: mocks.fetchCommentsForExport,
}));

import { getDatabaseReadStreamPaginated } from "./getDatabaseReadStream";

const request = {
  projectId: "project-1",
  tableName: "traces" as const,
  cutoffCreatedAt: new Date("2026-07-20T00:00:00.000Z"),
  filter: [],
  orderBy: { column: "timestamp", order: "DESC" as const },
  rowLimit: 2,
};

describe("Doris trace export composition", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mocks.getDistinctScoreNames.mockResolvedValue(["quality"]);
    mocks.getTracesTable.mockResolvedValue([
      {
        id: "trace-1",
        projectId: "project-1",
        timestamp: new Date("2026-01-01T00:00:00.000Z"),
        tags: [],
        bookmarked: false,
        name: "trace",
        release: null,
        version: null,
        userId: null,
        environment: "production",
        sessionId: null,
        public: false,
      },
    ]);
    mocks.getTracesTableMetrics.mockResolvedValue([
      {
        id: "trace-1",
        projectId: "project-1",
        promptTokens: 12n,
        completionTokens: 5n,
        totalTokens: 17n,
        latency: 2,
        level: "ERROR",
        observationCount: 2n,
        calculatedTotalCost: new Decimal("0.3"),
        calculatedInputCost: new Decimal("0.1"),
        calculatedOutputCost: new Decimal("0.2"),
        usageDetails: {},
        costDetails: {},
        errorCount: 1n,
        warningCount: 0n,
        defaultCount: 1n,
        debugCount: 0n,
      },
    ]);
    mocks.getTracesByIds.mockResolvedValue([
      {
        id: "trace-1",
        input: { question: "old" },
        output: { answer: "found" },
        metadata: { source: "doris" },
      },
    ]);
    mocks.getScoresForTraces.mockResolvedValue([
      {
        traceId: "trace-1",
        name: "quality",
        value: 0.9,
        stringValue: null,
        dataType: "NUMERIC",
      },
    ]);
    mocks.fetchCommentsForExport.mockResolvedValue(
      new Map([
        [
          "trace-1",
          [
            {
              id: "comment-1",
              content: "reviewed",
            },
          ],
        ],
      ]),
    );
  });

  it("joins list, metrics, exact detail, scores, and comments without a legacy trace read", async () => {
    const stream = await getDatabaseReadStreamPaginated(request);
    const rows: unknown[] = [];
    for await (const row of stream) rows.push(row);

    expect(rows).toEqual([
      expect.objectContaining({
        id: "trace-1",
        input: { question: "old" },
        output: { answer: "found" },
        metadata: { source: "doris" },
        latency: 2,
        inputTokens: 12n,
        outputTokens: 5n,
        totalTokens: 17n,
        quality: [0.9],
        comments: [{ id: "comment-1", content: "reviewed" }],
      }),
    ]);
    expect(mocks.getTracesTableMetrics).toHaveBeenCalledWith(
      expect.objectContaining({
        filter: expect.arrayContaining([
          expect.objectContaining({
            column: "ID",
            value: ["trace-1"],
          }),
        ]),
      }),
    );
    expect(mocks.getTracesByIds).toHaveBeenCalledWith(
      ["trace-1"],
      "project-1",
      new Date("2026-01-01T00:00:00.000Z"),
      expect.any(Object),
    );
  });

  it("fails the stream when exact Doris detail loading fails", async () => {
    mocks.getTracesByIds.mockRejectedValueOnce(new Error("detail failed"));
    const stream = await getDatabaseReadStreamPaginated(request);

    await expect(
      (async () => {
        for await (const _row of stream) {
          // Drain until the stream errors.
        }
      })(),
    ).rejects.toThrow("detail failed");
  });
});
