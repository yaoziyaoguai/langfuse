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
    const stream = Readable.from([]);
    const paginated = vi.fn().mockResolvedValue(stream);

    await expect(
      new DorisAnalyticsExportSource({ paginated }).open(request("events")),
    ).resolves.toBe(stream);
    expect(paginated).toHaveBeenCalledWith(
      expect.objectContaining({ tableName: "events", projectId: "project-1" }),
    );
  });

  it("rejects Doris experiment exports before opening a stream", async () => {
    const paginated = vi.fn();

    await expect(
      new DorisAnalyticsExportSource({ paginated }).open(
        request("dataset_run_items"),
      ),
    ).rejects.toMatchObject({
      code: "ANALYTICS_EXPORT_UNSUPPORTED",
      tableName: "dataset_run_items",
    });
    expect(paginated).not.toHaveBeenCalled();
  });
});
