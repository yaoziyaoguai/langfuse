import { Prisma } from "@prisma/client";
import { describe, expect, it } from "vitest";

describe("Doris analytics Postgres control state", () => {
  it("exposes every R1A control model and no R1B retention model", () => {
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
        "TraceControlState",
      ]),
    );
    expect(models).not.toContain("AnalyticsRetentionRun");
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
