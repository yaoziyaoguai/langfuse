import { describe, expect, it, vi } from "vitest";

const mocks = vi.hoisted(() => ({
  ingestionService: vi.fn(),
  clickhouseWriter: vi.fn(),
}));

vi.mock("@langfuse/shared/src/server", () => ({
  clickhouseClient: vi.fn(),
  redis: {},
}));
vi.mock("@langfuse/shared/src/db", () => ({ prisma: {} }));
vi.mock("../../services/ClickhouseWriter", () => ({
  ClickhouseWriter: {
    getInstance: mocks.clickhouseWriter,
  },
}));
vi.mock("../../services/IngestionService", () => ({
  IngestionService: mocks.ingestionService,
}));
vi.mock("../../env", () => ({
  env: { LANGFUSE_ANALYTICS_BACKEND: "doris" },
  v4WritesToEventsTable: vi.fn(() => true),
}));

import { createInternalEventsWriter } from "./createInternalEventsWriter";

describe("createInternalEventsWriter", () => {
  it("keeps Doris admission metadata without constructing ClickHouse services", async () => {
    const analyticsAdmissionContext = {
      runtimeLeaseId: "runtime-lease-1",
      backend: "doris" as const,
      deploymentGeneration: 4n,
    };
    const writer = createInternalEventsWriter({
      analyticsAdmissionContext,
      experimentContext: {
        id: "run-1",
        name: "experiment-1",
        datasetId: "dataset-1",
        itemId: "item-1",
        itemVersion: "2026-07-25 09:00:00.000",
      },
    });

    await expect(
      writer.write({
        rootSpanId: "root-span",
        eventInputs: [],
      }),
    ).resolves.toBeUndefined();

    expect(writer.analyticsAdmissionContext).toBe(analyticsAdmissionContext);
    expect(writer.experimentContext?.id).toBe("run-1");
    expect(mocks.ingestionService).not.toHaveBeenCalled();
    expect(mocks.clickhouseWriter).not.toHaveBeenCalled();
  });
});
