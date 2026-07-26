import { BatchExportFileFormat } from "@langfuse/shared";
import { Readable } from "stream";
import { describe, expect, it, vi } from "vitest";

import { ClickHouseAnalyticsExportSource } from "./ClickHouseAnalyticsExportSource";
import { DorisAnalyticsExportSource } from "./DorisAnalyticsExportSource";

const request = (
  tableName:
    | "observations"
    | "traces"
    | "events"
    | "scores"
    | "dataset_run_items",
) => ({
  projectId: "project-1",
  cutoffCreatedAt: new Date("2026-07-20T00:00:00.000Z"),
  tableName,
  filter: [],
  orderBy: { column: "startTime", order: "DESC" as const },
  fileFormat: BatchExportFileFormat.JSON,
});

describe("analytics export adapters", () => {
  it.each([
    ["observations", "observations"],
    ["traces", "traces"],
    ["events", "events"],
    ["scores", "paginated"],
  ] as const)(
    "keeps ClickHouse %s on its established reader",
    async (tableName, selected) => {
      const stream = Readable.from([]);
      const dependencies = {
        observations: vi.fn().mockResolvedValue(stream),
        traces: vi.fn().mockResolvedValue(stream),
        events: vi.fn().mockResolvedValue(stream),
        paginated: vi.fn().mockResolvedValue(stream),
      };

      await expect(
        new ClickHouseAnalyticsExportSource(dependencies).open(
          request(tableName),
        ),
      ).resolves.toBe(stream);
      expect(dependencies[selected]).toHaveBeenCalledOnce();
    },
  );

  it("routes supported Doris tables through the backend-neutral paginated reader", async () => {
    const paginated = vi.fn().mockResolvedValue(
      Readable.from([
        { id: "event-b", value: 2 },
        { id: "event-a", value: 1 },
      ]),
    );
    const identities = { scan: vi.fn() };
    const source = new DorisAnalyticsExportSource({
      paginated,
      identities,
      pageSize: 100,
    });

    const stream = await source.open(request("events"), {
      identities: Readable.from([
        { id: "event-a", traceId: "trace-1" },
        { id: "event-b", traceId: "trace-1" },
      ]),
    });
    const rows = [];
    for await (const row of stream) rows.push(row);
    expect(rows).toEqual([
      { id: "event-a", value: 1 },
      { id: "event-b", value: 2 },
    ]);
    expect(paginated).toHaveBeenCalledWith(
      expect.objectContaining({
        tableName: "events",
        projectId: "project-1",
        filter: [],
        exactIdentityIds: ["event-a", "event-b"],
        exactTraceId: "trace-1",
        rowLimit: 2,
      }),
    );
  });

  it("requires a sealed manifest before opening Doris analytics IO", async () => {
    const paginated = vi.fn();
    const source = new DorisAnalyticsExportSource({
      paginated,
      identities: { scan: vi.fn() },
      pageSize: 100,
    });

    await expect(source.open(request("traces"))).rejects.toThrow(
      "sealed identity manifest",
    );
    expect(paginated).not.toHaveBeenCalled();
  });

  it("omits deleted identities without shifting or duplicating later rows", async () => {
    const paginated = vi
      .fn()
      .mockResolvedValueOnce(Readable.from([{ id: "trace-a" }]))
      .mockResolvedValueOnce(Readable.from([{ id: "trace-c" }]));
    const source = new DorisAnalyticsExportSource({
      paginated,
      identities: { scan: vi.fn() },
      pageSize: 2,
    });
    const stream = await source.open(request("traces"), {
      identities: Readable.from([
        { id: "trace-a" },
        { id: "trace-b" },
        { id: "trace-c" },
      ]),
    });
    const rows = [];
    for await (const row of stream) rows.push(row);

    expect(rows).toEqual([{ id: "trace-a" }, { id: "trace-c" }]);
    expect(paginated).toHaveBeenCalledTimes(2);
  });

  it("reads the current payload without reapplying sealed membership search", async () => {
    const paginated = vi
      .fn()
      .mockResolvedValue(Readable.from([{ id: "trace-a", name: "updated" }]));
    const source = new DorisAnalyticsExportSource({
      paginated,
      identities: { scan: vi.fn() },
      pageSize: 100,
    });
    const stream = await source.open(
      {
        ...request("traces"),
        searchQuery: "original-name",
        searchType: ["id", "name"],
      },
      { identities: Readable.from([{ id: "trace-a" }]) },
    );
    const rows = [];
    for await (const row of stream) rows.push(row);

    expect(rows).toEqual([{ id: "trace-a", name: "updated" }]);
    expect(paginated).toHaveBeenCalledWith(
      expect.not.objectContaining({
        searchQuery: expect.anything(),
        searchType: expect.anything(),
      }),
    );
  });

  it("revalidates the durable execution claim before every exact-ID batch", async () => {
    const paginated = vi
      .fn()
      .mockResolvedValueOnce(Readable.from([{ id: "trace-a" }]))
      .mockResolvedValueOnce(Readable.from([{ id: "trace-b" }]));
    const revalidate = vi
      .fn()
      .mockResolvedValueOnce(undefined)
      .mockRejectedValueOnce(new Error("execution claim was fenced"));
    const source = new DorisAnalyticsExportSource({
      paginated,
      identities: { scan: vi.fn() },
      pageSize: 1,
    });
    const stream = await source.open(request("traces"), {
      identities: Readable.from([{ id: "trace-a" }, { id: "trace-b" }]),
      revalidate,
    });
    const rows = [];

    await expect(async () => {
      for await (const row of stream) rows.push(row);
    }).rejects.toThrow("execution claim was fenced");
    expect(rows).toEqual([{ id: "trace-a" }]);
    expect(revalidate).toHaveBeenCalledTimes(2);
    expect(paginated).toHaveBeenCalledTimes(1);
  });

  it("reads sealed Doris experiment identities through the neutral reader", async () => {
    const paginated = vi
      .fn()
      .mockResolvedValue(Readable.from([{ id: "run-item-1" }]));
    const stream = await new DorisAnalyticsExportSource({
      paginated,
      identities: { scan: vi.fn() },
      pageSize: 100,
    }).open(request("dataset_run_items"), {
      identities: Readable.from([{ id: "run-item-1" }]),
    });
    const rows = [];
    for await (const row of stream) rows.push(row);

    expect(rows).toEqual([{ id: "run-item-1" }]);
    expect(paginated).toHaveBeenCalledWith(
      expect.objectContaining({
        tableName: "dataset_run_items",
        exactIdentityIds: ["run-item-1"],
        rowLimit: 1,
      }),
    );
  });
});
