import { PrismaClient } from "@prisma/client";
import { afterAll, beforeAll, describe, expect, it } from "vitest";

const controlDatabaseUrl = process.env.DORIS_CONTROL_TEST_DATABASE_URL;

describe.skipIf(!controlDatabaseUrl)(
  "analytics dataset deletion fencing",
  () => {
    const prisma = new PrismaClient({ datasourceUrl: controlDatabaseUrl });
    const suffix = `${Date.now()}-${Math.random().toString(16).slice(2)}`;
    const organizationId = `dataset-fence-org-${suffix}`;
    const projectId = `dataset-fence-project-${suffix}`;
    const operationId = `dataset-fence-operation-${suffix}`;
    const loadBatchId = `dataset-fence-load-${suffix}`;
    const datasetId = `dataset-${suffix}`;
    const datasetRunId = `run-${suffix}`;
    let loadRepository: typeof import("./analyticsLoadBatches.js");
    let controlRepository: typeof import("./analytics-control-state.js");

    beforeAll(async () => {
      [loadRepository, controlRepository] = await Promise.all([
        import("./analyticsLoadBatches.js"),
        import("./analytics-control-state.js"),
      ]);
      await prisma.organization.create({
        data: { id: organizationId, name: "Dataset fence test" },
      });
      await prisma.project.create({
        data: {
          id: projectId,
          orgId: organizationId,
          name: "Dataset fence test",
        },
      });
      await prisma.analyticsIngestionOperation.create({
        data: {
          id: operationId,
          projectId,
          sourceOperationId: `source-${suffix}`,
          sourceChecksum: "a".repeat(64),
          rawObjectKey: `raw/${suffix}`,
          acceptedAt: new Date("2026-07-23T09:00:00.000Z"),
          acceptedAtNanos: 1_785_316_400_000_000_000n,
          canonicalizerVersion: "1",
          schemaVersion: 2,
          canonicalizationFence: 1n,
          canonicalObjectKey: `canonical/${suffix}`,
          manifestState: "FROZEN",
          status: "PERSISTED",
          recoverableUntil: new Date("2026-07-30T09:00:00.000Z"),
          statusExpiresAt: new Date("2026-08-30T09:00:00.000Z"),
        },
      });
    }, 60_000);

    afterAll(async () => {
      await prisma.organization.deleteMany({ where: { id: organizationId } });
      await prisma.analyticsDatasetRunDeletionGeneration.deleteMany({
        where: { projectId },
      });
      await prisma.analyticsDatasetDeletionGeneration.deleteMany({
        where: { projectId },
      });
      await prisma.$disconnect();
    }, 60_000);

    it("cancels a pending load when its dataset generation becomes nonzero", async () => {
      await prisma.analyticsLoadBatch.create({
        data: {
          id: loadBatchId,
          operationId,
          projectId,
          databaseName: "langfuse",
          targetTable: "dataset_run_items_current",
          logicalBatchId: `logical-${suffix}`,
          fenceGeneration: 1n,
          label: `dataset_fence_${suffix}`,
          payloadHash: "b".repeat(64),
          canonicalObjectKey: `canonical/${suffix}`,
          partitionDate: new Date("2026-07-23T00:00:00.000Z"),
        },
      });
      await prisma.analyticsIngestionCandidate.create({
        data: {
          operationId,
          projectId,
          candidateKey: `candidate-${suffix}`,
          entityType: "DATASET_RUN_ITEM",
          entityKey: `entity-${suffix}`,
          owningDatasetId: datasetId,
          owningDatasetRunId: datasetRunId,
          partitionDate: new Date("2026-07-23T00:00:00.000Z"),
          sourceVersion: 1n,
          canonicalPayloadHash: "c".repeat(64),
          disposition: "LOAD_REQUIRED",
          loadBatchId,
        },
      });
      await prisma.analyticsDatasetDeletionGeneration.create({
        data: { projectId, datasetId, generation: 1n },
      });

      await expect(
        loadRepository.cancelAnalyticsLoadBatchIfDeleted({
          client: prisma,
          loadBatchId,
          projectId,
        }),
      ).resolves.toEqual({ outcome: "cancelled" });
    });

    it("classifies an identical replay behind a dataset fence as stale", async () => {
      const base = {
        client: prisma,
        projectId,
        operationId,
        entityType: "DATASET_RUN_ITEM" as const,
        entityKey: `head-${suffix}`,
        lookupId: `run-item-${suffix}`,
        owningTraceId: null,
        owningDatasetId: datasetId,
        owningDatasetRunId: datasetRunId,
        expectedSourceVersion: null,
        sourceVersion: 2n,
        canonicalPayloadHash: "d".repeat(64),
        partitionDate: new Date("2026-07-23T00:00:00.000Z"),
        canonicalizerVersion: "1",
        fenceGeneration: 1n,
        traceDeletionGeneration: 0n,
        projectDeletionGeneration: 0n,
      };
      await expect(
        controlRepository.claimAnalyticsEntityHead({
          ...base,
          datasetDeletionGeneration: 1n,
          runDeletionGeneration: 0n,
        }),
      ).resolves.toMatchObject({ outcome: "won" });
      await expect(
        controlRepository.claimAnalyticsEntityHead({
          ...base,
          datasetDeletionGeneration: 0n,
          runDeletionGeneration: 0n,
        }),
      ).resolves.toMatchObject({ outcome: "stale_fence" });
    });
  },
);
