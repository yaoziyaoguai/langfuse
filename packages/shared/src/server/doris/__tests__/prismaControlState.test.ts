import { Prisma } from "@prisma/client";
import { describe, expect, it } from "vitest";

describe("Doris analytics Postgres control state", () => {
  it("exposes analytics control, retention, deployment, lease, and activation models", () => {
    const models = Prisma.dmmf.datamodel.models.map(({ name }) => name);

    expect(models).toEqual(
      expect.arrayContaining([
        "AnalyticsIngestionOperation",
        "AnalyticsIngestionCandidate",
        "AnalyticsIngestionOutbox",
        "AnalyticsLoadBatch",
        "AnalyticsEntityHead",
        "AnalyticsDeletionTombstone",
        "AnalyticsProjectDeletionGeneration",
        "AnalyticsDeletionOperation",
        "AnalyticsCheckpointGeneration",
        "AnalyticsBackgroundMigrationRetirement",
        "AnalyticsRetentionRun",
        "AnalyticsRetentionState",
        "AnalyticsBackendDeploymentState",
        "AnalyticsRuntimeLease",
        "AnalyticsRuntimeCapabilityContract",
        "AnalyticsCapabilityActivation",
        "AnalyticsBackendClaimLease",
        "AnalyticsBackendDeploymentTransition",
        "TraceControlState",
      ]),
    );
  });

  it("keeps project-deletion generation and status outside Project cascade", () => {
    for (const modelName of [
      "AnalyticsProjectDeletionGeneration",
      "AnalyticsDeletionOperation",
    ]) {
      const model = Prisma.dmmf.datamodel.models.find(
        ({ name }) => name === modelName,
      );
      expect(model?.fields.some(({ relationName }) => relationName)).toBe(
        false,
      );
    }
  });
});
