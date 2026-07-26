import { beforeEach, describe, expect, it, vi } from "vitest";

const mocks = vi.hoisted(() => ({
  findMany: vi.fn(),
  schedule: vi.fn(),
  clean: vi.fn(),
}));

vi.mock("@langfuse/shared/src/db", () => ({
  prisma: {
    blobStorageIntegration: {
      findMany: mocks.findMany,
    },
  },
}));
vi.mock("@langfuse/shared/src/server", () => ({
  BlobStorageIntegrationProcessingQueue: {
    getInstance: () => ({
      clean: mocks.clean,
    }),
  },
  QueueJobs: {
    BlobStorageIntegrationProcessingJob:
      "blobstorage-integration-processing-job",
  },
  logger: {
    info: vi.fn(),
  },
}));
vi.mock("../../env", () => ({
  env: {
    LANGFUSE_ANALYTICS_BACKEND: "doris",
  },
}));
vi.mock("../analytics-integrations/scheduleDorisAnalyticsIntegrations", () => ({
  scheduleDorisAnalyticsIntegrations: mocks.schedule,
}));

import { handleBlobStorageIntegrationSchedule } from "./handleBlobStorageIntegrationSchedule";

describe("handleBlobStorageIntegrationSchedule", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mocks.findMany.mockResolvedValue([
      {
        projectId: "project-1",
        lastSyncAt: new Date("2026-01-01T00:00:00.000Z"),
      },
    ]);
    mocks.schedule.mockResolvedValue(1);
  });

  it("forces exactly the requested Doris project without applying the due-time window", async () => {
    await handleBlobStorageIntegrationSchedule({ projectId: "project-1" });

    expect(mocks.findMany).toHaveBeenCalledWith({
      select: {
        lastSyncAt: true,
        projectId: true,
      },
      where: {
        enabled: true,
        projectId: "project-1",
      },
    });
    expect(mocks.schedule).toHaveBeenCalledWith({
      integrationType: "BLOB_STORAGE",
      projectIds: ["project-1"],
      queue: expect.any(Object),
      jobName: "blobstorage-integration-processing-job",
    });
  });
});
