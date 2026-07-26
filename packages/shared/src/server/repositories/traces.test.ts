import { describe, it, expect, beforeEach, vi } from "vitest";

const mockQueryClickhouse = vi.hoisted(() => vi.fn());
const mockShouldSkipObservationsFinal = vi.hoisted(() => vi.fn());
let charCounter = 0;

vi.mock("./clickhouse", () => ({
  queryClickhouse: mockQueryClickhouse,
  commandClickhouse: vi.fn(),
  queryClickhouseStream: vi.fn(),
  upsertClickhouse: vi.fn(),
  parseClickhouseUTCDateTimeFormat: vi.fn(),
  // Return unique names per call so filter parameter variables don't collide
  clickhouseCompliantRandomCharacters: vi.fn(() => `x${++charCounter}`),
}));

vi.mock("../queries/clickhouse-sql/query-options", () => ({
  shouldSkipObservationsFinal: mockShouldSkipObservationsFinal,
}));

vi.mock("./telemetry/doris/runtime", () => ({
  isDorisAnalyticsBackend: () => false,
}));

import { getTracesByIds, getTracesCountForPublicApi } from "./traces";
import {
  FilterList,
  StringFilter,
} from "../queries/clickhouse-sql/clickhouse-filter";

describe("getTracesCountForPublicApi — FINAL modifier", () => {
  beforeEach(() => {
    charCounter = 0;
    vi.clearAllMocks();
    mockQueryClickhouse.mockResolvedValue([{ count: "0" }]);
    mockShouldSkipObservationsFinal.mockResolvedValue(false);
  });

  it("uses FINAL for a non-skip-index trace filter", async () => {
    const filter = new FilterList([
      new StringFilter({
        clickhouseTable: "traces",
        field: "name",
        operator: "=",
        value: "my-trace",
      }),
    ]);

    await getTracesCountForPublicApi({ projectId: "proj-1", filter });

    expect(mockQueryClickhouse).toHaveBeenCalledOnce();
    const { query } = mockQueryClickhouse.mock.calls[0][0];
    expect(query).toMatch(/FROM\s+traces\s+t\s+FINAL/);
  });

  it("does not use FINAL for skip-index trace filters (user_id / session_id / metadata)", async () => {
    const filter = new FilterList([
      new StringFilter({
        clickhouseTable: "traces",
        field: "user_id",
        operator: "=",
        value: "user-abc",
      }),
    ]);

    await getTracesCountForPublicApi({ projectId: "proj-1", filter });

    expect(mockQueryClickhouse).toHaveBeenCalledOnce();
    const { query } = mockQueryClickhouse.mock.calls[0][0];
    expect(query).not.toContain("FINAL");
  });

  it("uses FINAL when an observations-table filter is present", async () => {
    const filter = new FilterList([
      new StringFilter({
        clickhouseTable: "observations",
        field: "name",
        operator: "=",
        value: "obs-span",
      }),
    ]);

    await getTracesCountForPublicApi({ projectId: "proj-1", filter });

    expect(mockQueryClickhouse).toHaveBeenCalledOnce();
    const { query } = mockQueryClickhouse.mock.calls[0][0];
    expect(query).toMatch(/FROM\s+traces\s+t\s+FINAL/);
  });
});

describe("getTracesByIds — ClickHouse routing", () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  it("keeps the existing ClickHouse exact-ID query when ClickHouse is selected", async () => {
    mockQueryClickhouse.mockResolvedValueOnce([]);

    await expect(
      getTracesByIds(
        ["trace-1"],
        "project-1",
        new Date("2026-07-17T00:00:00.000Z"),
      ),
    ).resolves.toEqual([]);
    expect(mockQueryClickhouse).toHaveBeenCalledOnce();
    expect(mockQueryClickhouse).toHaveBeenCalledWith(
      expect.objectContaining({
        query: expect.stringContaining(
          "FROM traces\n        WHERE id IN ({traceIds: Array(String)})",
        ),
        params: expect.objectContaining({
          traceIds: ["trace-1"],
          projectId: "project-1",
        }),
      }),
    );
  });
});
