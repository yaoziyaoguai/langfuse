import { beforeEach, describe, expect, it, vi } from "vitest";

const { createIntent, publishDispatch, getQueue, loggerWarn } = vi.hoisted(
  () => ({
    createIntent: vi.fn(),
    publishDispatch: vi.fn(),
    getQueue: vi.fn(),
    loggerWarn: vi.fn(),
  }),
);

vi.mock("@langfuse/shared/src/server", () => ({
  BatchExportQueue: { getInstance: getQueue },
  ClickHouseClientManager: {
    getInstance: () => ({ closeAllConnections: vi.fn() }),
  },
  createDorisBatchExportIntent: createIntent,
  logger: { warn: loggerWarn, debug: vi.fn() },
  publishBatchExportDispatch: publishDispatch,
  QueueJobs: { BatchExportJob: "batch-export-job" },
  redis: null,
}));

import {
  createAdmittedDorisBatchExport,
  dispatchDorisBatchExport,
} from "@/src/features/batch-exports/server/dorisBatchExport";

const admissionContext = {
  runtimeLeaseId: "web-runtime",
  backend: "doris" as const,
  deploymentGeneration: 7n,
};

const managedExport = {
  id: "export-1",
  dispatchOutbox: { generation: 3 },
};

function clientWithActivation(activation: unknown) {
  return {
    analyticsCapabilityActivation: {
      findUnique: vi.fn().mockResolvedValue(activation),
    },
  };
}

describe("Doris batch export admission", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    createIntent.mockResolvedValue(managedExport);
  });

  it.each([
    null,
    {
      status: "DISABLED",
      backend: "DORIS",
      deploymentGeneration: 7n,
    },
    {
      status: "ACTIVE",
      backend: "DORIS",
      deploymentGeneration: 8n,
    },
  ])(
    "rejects before mutation when activation is not admitted",
    async (activation) => {
      await expect(
        createAdmittedDorisBatchExport({
          client: clientWithActivation(activation) as never,
          admissionContext,
          projectId: "project-1",
          userId: "user-1",
          name: "export",
          format: "JSONL",
          query: { tableName: "traces" },
        }),
      ).rejects.toMatchObject({
        body: { code: "R2_BATCH_EXPORTS_UNAVAILABLE" },
      });
      expect(createIntent).not.toHaveBeenCalled();
    },
  );

  it("creates the stamped intent only after the active generation matches", async () => {
    const client = clientWithActivation({
      status: "ACTIVE",
      backend: "DORIS",
      deploymentGeneration: 7n,
    });

    await expect(
      createAdmittedDorisBatchExport({
        client: client as never,
        admissionContext,
        projectId: "project-1",
        userId: "user-1",
        name: "export",
        format: "JSONL",
        query: { tableName: "traces" },
      }),
    ).resolves.toBe(managedExport);
    expect(createIntent).toHaveBeenCalledWith(
      expect.objectContaining({
        client,
        admissionContext,
        projectId: "project-1",
      }),
    );
  });

  it("uses a stable queue identity and leaves transient publication to recovery", async () => {
    const delivery = {
      getState: vi.fn().mockResolvedValue("failed"),
      retry: vi.fn().mockResolvedValue(undefined),
    };
    const queue = { add: vi.fn().mockResolvedValue(delivery) };
    getQueue.mockReturnValue(queue);
    publishDispatch.mockImplementation(async ({ publish }) => {
      await publish({ projectId: "project-1", batchExportId: "export-1" });
      return true;
    });

    await expect(
      dispatchDorisBatchExport({
        client: {} as never,
        admissionContext,
        batchExport: managedExport as never,
      }),
    ).resolves.toBeUndefined();
    expect(queue.add).toHaveBeenCalledWith(
      "batch-export-job",
      expect.objectContaining({ id: "export-1-g3" }),
      { jobId: "export-1-g3" },
    );
    expect(delivery.retry).toHaveBeenCalledWith("failed");

    publishDispatch.mockRejectedValueOnce(new Error("redis unavailable"));
    await expect(
      dispatchDorisBatchExport({
        client: {} as never,
        admissionContext,
        batchExport: managedExport as never,
      }),
    ).resolves.toBeUndefined();
    expect(loggerWarn).toHaveBeenCalledWith(
      expect.stringContaining("durable recovery"),
      expect.objectContaining({ batchExportId: "export-1" }),
    );
  });
});
