import type { Job } from "bullmq";
import { beforeEach, describe, expect, it, vi } from "vitest";

const mocks = vi.hoisted(() => ({
  communityEnabled: true,
  communitySchedule: vi.fn(),
  communityProcess: vi.fn(),
  enterpriseSchedule: vi.fn(),
  enterpriseProcess: vi.fn(),
}));

vi.mock("@langfuse/shared/src/server", () => ({
  isCommunityExtensionEnabled: () => mocks.communityEnabled,
  instrumentAsync: async (
    _options: unknown,
    operation: () => Promise<unknown>,
  ) => operation(),
  logger: {
    info: vi.fn(),
    error: vi.fn(),
  },
  QueueJobs: {
    DataRetentionJob: "data-retention-job",
    DataRetentionProcessingJob: "data-retention-processing-job",
  },
}));

vi.mock("../features/community-extensions/data-retention/index.js", () => ({
  scheduleCommunityDataRetention: mocks.communitySchedule,
  processCommunityDataRetentionJob: mocks.communityProcess,
}));

vi.mock("../ee/dataRetention/handleDataRetentionSchedule.js", () => ({
  handleDataRetentionSchedule: mocks.enterpriseSchedule,
}));

vi.mock("../ee/dataRetention/handleDataRetentionProcessingJob.js", () => ({
  handleDataRetentionProcessingJob: mocks.enterpriseProcess,
}));

import {
  dataRetentionProcessingProcessor,
  dataRetentionProcessor,
} from "./dataRetentionQueue";

describe("data retention queue composition", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mocks.communityEnabled = true;
  });

  it("loads only Community Extensions handlers when the community build is enabled", async () => {
    const scheduleJob = {
      name: "data-retention-job",
    } as Job;
    const processingJob = {
      name: "data-retention-processing-job",
    } as Job;

    await dataRetentionProcessor(scheduleJob);
    await dataRetentionProcessingProcessor(processingJob);

    expect(mocks.communitySchedule).toHaveBeenCalledOnce();
    expect(mocks.communityProcess).toHaveBeenCalledWith(processingJob);
    expect(mocks.enterpriseSchedule).not.toHaveBeenCalled();
    expect(mocks.enterpriseProcess).not.toHaveBeenCalled();
  });

  it("preserves the upstream handlers when Community Extensions is disabled", async () => {
    mocks.communityEnabled = false;
    const scheduleJob = {
      name: "data-retention-job",
    } as Job;
    const processingJob = {
      name: "data-retention-processing-job",
    } as Job;

    await dataRetentionProcessor(scheduleJob);
    await dataRetentionProcessingProcessor(processingJob);

    expect(mocks.enterpriseSchedule).toHaveBeenCalledOnce();
    expect(mocks.enterpriseProcess).toHaveBeenCalledWith(processingJob);
    expect(mocks.communitySchedule).not.toHaveBeenCalled();
    expect(mocks.communityProcess).not.toHaveBeenCalled();
  });
});
