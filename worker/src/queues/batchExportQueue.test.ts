import { beforeEach, describe, expect, it, vi } from "vitest";

const testTypes = vi.hoisted(() => ({
  BusyError: class BusyError extends Error {},
  NotFoundError: class NotFoundError extends Error {},
}));

const mocks = vi.hoisted(() => ({
  handle: vi.fn(),
  findFirst: vi.fn(),
  update: vi.fn(),
  traceException: vi.fn(),
}));

vi.mock("@langfuse/shared", () => ({
  BaseError: class BaseError extends Error {},
  BatchExportStatus: {
    QUEUED: "QUEUED",
    PROCESSING: "PROCESSING",
    COMPLETED: "COMPLETED",
    FAILED: "FAILED",
    CANCELLED: "CANCELLED",
  },
  LangfuseNotFoundError: testTypes.NotFoundError,
}));
vi.mock("@langfuse/shared/src/db", () => ({
  prisma: {
    batchExport: {
      findFirst: mocks.findFirst,
      update: mocks.update,
    },
  },
}));
vi.mock("@langfuse/shared/src/server", () => ({
  BatchExportManifestBusyError: testTypes.BusyError,
  isManagedBatchExportJob: (job: object) => "analyticsBackend" in job,
  traceException: mocks.traceException,
  logger: { info: vi.fn(), warn: vi.fn(), error: vi.fn() },
  QueueName: { BatchExport: "BatchExport" },
}));
vi.mock("../features/batchExport/handleBatchExportJob", () => ({
  handleBatchExportJob: mocks.handle,
}));

import { batchExportQueueProcessor } from "./batchExportQueue";

const managedPayload = {
  projectId: "project-1",
  batchExportId: "export-1",
  dispatchGeneration: 1,
  analyticsBackend: "DORIS" as const,
  deploymentGeneration: "1",
  workloadEpochFingerprint: "e".repeat(64),
  runtimeContractVersion: 1,
  capabilityActivationGeneration: "1",
  capabilityContractVersion: 1,
};

function job(payload: object = managedPayload) {
  return { data: { payload } } as never;
}

describe("batch export queue terminal handling", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mocks.findFirst.mockResolvedValue({
      status: "PROCESSING",
      executionState: "EXPORTING",
    });
  });

  it.each([
    { status: "COMPLETED", executionState: "COMPLETED" },
    { status: "CANCELLED", executionState: "CANCELLED" },
    { status: "FAILED", executionState: "QUARANTINED" },
  ])(
    "does not overwrite a managed terminal state after redelivery",
    async (current) => {
      mocks.handle.mockRejectedValue(new Error("late failure"));
      mocks.findFirst.mockResolvedValue(current);

      await expect(batchExportQueueProcessor(job())).resolves.toBe(true);
    },
  );

  it("rethrows an active-claim conflict without marking the export failed", async () => {
    const error = new testTypes.BusyError("active claim");
    mocks.handle.mockRejectedValue(error);

    await expect(batchExportQueueProcessor(job())).rejects.toBe(error);
  });

  it("does not mutate managed state outside the handler claim boundary", async () => {
    const error = new Error("reader failed");
    mocks.handle.mockRejectedValue(error);

    await expect(batchExportQueueProcessor(job())).rejects.toBe(error);
    expect(mocks.findFirst).toHaveBeenCalledOnce();
    expect(mocks.update).not.toHaveBeenCalled();
  });

  it("preserves the legacy ClickHouse failure lifecycle", async () => {
    const error = new Error("clickhouse reader failed");
    mocks.handle.mockRejectedValue(error);

    await expect(
      batchExportQueueProcessor(
        job({ projectId: "project-1", batchExportId: "legacy-export" }),
      ),
    ).rejects.toBe(error);
    expect(mocks.update).toHaveBeenCalledWith(
      expect.objectContaining({
        where: { id: "legacy-export", projectId: "project-1" },
        data: expect.objectContaining({ status: "FAILED" }),
      }),
    );
  });
});
