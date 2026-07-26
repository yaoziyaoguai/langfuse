import { PrismaClient } from "@prisma/client";
import { afterAll, beforeAll, describe, expect, it } from "vitest";

const controlDatabaseUrl = process.env.DORIS_CONTROL_TEST_DATABASE_URL;

describe.skipIf(!controlDatabaseUrl)(
  "analytics dataset deletion operations",
  () => {
    const prisma = new PrismaClient({ datasourceUrl: controlDatabaseUrl });
    const suffix = `${Date.now()}-${Math.random().toString(16).slice(2)}`;
    const projectId = `dataset-deletion-project-${suffix}`;
    const datasetId = `dataset-${suffix}`;
    const runIds = [`run-a-${suffix}`, `run-b-${suffix}`];
    let repository: typeof import("./analyticsDatasetDeletionOperations.js");

    beforeAll(async () => {
      repository = await import("./analyticsDatasetDeletionOperations.js");
    }, 30_000);

    afterAll(async () => {
      await prisma.analyticsDatasetDeletionOutbox.deleteMany({
        where: { operation: { projectId } },
      });
      await prisma.analyticsDatasetDeletionOperation.deleteMany({
        where: { projectId },
      });
      await prisma.analyticsDatasetRunDeletionGeneration.deleteMany({
        where: { projectId },
      });
      await prisma.analyticsDatasetDeletionGeneration.deleteMany({
        where: { projectId },
      });
      await prisma.$disconnect();
    }, 30_000);

    it("publishes dataset and run fences atomically with one durable intent", async () => {
      const scheduled = await prisma.$transaction((transaction) =>
        repository.createAnalyticsDatasetDeletionIntent({
          transaction,
          scope: "DATASET",
          projectId,
          datasetId,
          datasetRunIds: runIds,
        }),
      );

      expect(scheduled).toMatchObject({
        datasetGeneration: 1n,
        runGenerations: {
          [runIds[0]!]: 1n,
          [runIds[1]!]: 1n,
        },
      });
      await expect(
        prisma.analyticsDatasetDeletionOutbox.findUniqueOrThrow({
          where: { operationId: scheduled.operation.id },
        }),
      ).resolves.toMatchObject({ status: "PENDING" });
      await expect(
        repository.getAnalyticsDatasetDeletionGenerations({
          client: prisma,
          projectId,
          datasetId,
          datasetRunIds: runIds,
        }),
      ).resolves.toEqual({
        datasetGeneration: 1n,
        runGenerations: {
          [runIds[0]!]: 1n,
          [runIds[1]!]: 1n,
        },
      });
    });

    it("claims with a fencing lease and advances only the current worker", async () => {
      const scheduled = await prisma.$transaction((transaction) =>
        repository.createAnalyticsDatasetDeletionIntent({
          transaction,
          scope: "DATASET_RUNS",
          projectId,
          datasetId,
          datasetRunIds: [runIds[0]!],
        }),
      );
      const claimed = await repository.claimAnalyticsDatasetDeletionOperation({
        client: prisma,
        operationId: scheduled.operation.id,
        projectId,
        owner: "dataset-worker",
        now: new Date("2026-07-23T09:00:00.000Z"),
      });

      expect(claimed).toMatchObject({
        leaseOwner: "dataset-worker",
        workerFence: 1n,
      });
      await expect(
        repository.markAnalyticsDatasetDeletionBarrierVisible({
          client: prisma,
          operationId: scheduled.operation.id,
          projectId,
          lease: { owner: "dataset-worker", fence: 1n },
          now: new Date("2026-07-23T09:00:01.000Z"),
        }),
      ).resolves.toBe(true);
      await expect(
        repository.completeAnalyticsDatasetDeletionOperation({
          client: prisma,
          operationId: scheduled.operation.id,
          projectId,
          lease: { owner: "stale-worker", fence: 1n },
          now: new Date("2026-07-23T09:00:02.000Z"),
        }),
      ).resolves.toBe(false);
      await expect(
        repository.completeAnalyticsDatasetDeletionOperation({
          client: prisma,
          operationId: scheduled.operation.id,
          projectId,
          lease: { owner: "dataset-worker", fence: 1n },
          now: new Date("2026-07-23T09:00:02.000Z"),
        }),
      ).resolves.toBe(true);
    });

    it("does not allow one run generation fence to move between datasets", async () => {
      const datasetRunId = `stable-run-${suffix}`;
      await prisma.$transaction((transaction) =>
        repository.createAnalyticsDatasetDeletionIntent({
          transaction,
          scope: "DATASET_RUNS",
          projectId,
          datasetId,
          datasetRunIds: [datasetRunId],
        }),
      );

      await expect(
        prisma.$transaction((transaction) =>
          repository.createAnalyticsDatasetDeletionIntent({
            transaction,
            scope: "DATASET_RUNS",
            projectId,
            datasetId: `other-${datasetId}`,
            datasetRunIds: [datasetRunId],
          }),
        ),
      ).rejects.toThrow("dataset ownership");
    });
  },
);
