import { describe, expect, it, vi } from "vitest";

import { AnalyticsGenerationUsageResolver } from "./AnalyticsGenerationUsageResolver";

const input = {
  projectId: "project-1",
  spanId: "span-1",
  traceId: "trace-1",
  providedModelName: "unknown-model",
  providedUsageDetails: { input: 2, output: 3 },
  providedCostDetails: { input: 0.2, output: 0.5 },
  level: "DEFAULT",
};

describe("AnalyticsGenerationUsageResolver", () => {
  it("preserves provided usage and cost without requiring a matched model", async () => {
    const resolver = new AnalyticsGenerationUsageResolver({
      findModel: vi.fn().mockResolvedValue({ model: null, pricingTiers: [] }),
    });

    await expect(resolver.resolve(input)).resolves.toMatchObject({
      internalModelId: null,
      usageDetails: { input: 2, output: 3, total: 5 },
      costDetails: { input: 0.2, output: 0.5, total: 0.7 },
      totalCost: 0.7,
      usagePricingTierId: null,
    });
  });
});
