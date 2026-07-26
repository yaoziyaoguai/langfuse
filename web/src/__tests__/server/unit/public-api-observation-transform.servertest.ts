vi.mock("@langfuse/shared/src/env", () => ({
  env: new Proxy({} as Record<string, unknown>, { get: () => undefined }),
  removeEmptyEnvVariables: (env: Record<string, string | undefined>) => env,
}));

vi.mock("@langfuse/shared/src/db", () => ({ prisma: {} }));

import type { FullEventsObservation } from "@langfuse/shared/src/server";
import Decimal from "decimal.js";

import {
  APIObservation,
  transformDbToApiObservation,
} from "@/src/features/public-api/types/observations";

function fullObservation(): FullEventsObservation {
  const timestamp = new Date("2026-07-21T00:00:00.000Z");
  return {
    id: "observation-1",
    traceId: "trace-1",
    projectId: "project-1",
    environment: "default",
    type: "GENERATION",
    startTime: timestamp,
    endTime: timestamp,
    name: "generation",
    metadata: {},
    parentObservationId: null,
    level: "DEFAULT",
    statusMessage: null,
    version: null,
    createdAt: timestamp,
    updatedAt: timestamp,
    model: null,
    internalModelId: null,
    modelParameters: null,
    input: null,
    output: null,
    completionStartTime: null,
    promptId: null,
    promptName: null,
    promptVersion: null,
    latency: null,
    timeToFirstToken: null,
    providedUsageDetails: {},
    usageDetails: {},
    costDetails: {},
    providedCostDetails: {},
    inputCost: null,
    outputCost: null,
    totalCost: null,
    inputUsage: 0,
    outputUsage: 0,
    totalUsage: 0,
    usagePricingTierId: null,
    usagePricingTierName: null,
    toolDefinitions: null,
    toolCalls: null,
    toolCallNames: null,
    userId: null,
    sessionId: null,
    traceName: null,
    release: null,
    tags: [],
    bookmarked: false,
    public: false,
    inputPrice: new Decimal(0),
    outputPrice: new Decimal(0),
    totalPrice: new Decimal(0),
    traceTags: [],
    traceTimestamp: timestamp,
    toolDefinitionsCount: 1,
    toolCallsCount: 2,
  };
}

describe("transformDbToApiObservation", () => {
  it("removes internal read-model fields from the strict public response", () => {
    const transformed = transformDbToApiObservation(fullObservation());

    expect(() => APIObservation.parse(transformed)).not.toThrow();
    expect(transformed).not.toHaveProperty("traceTimestamp");
    expect(transformed).not.toHaveProperty("toolDefinitionsCount");
    expect(transformed).not.toHaveProperty("toolCallsCount");
  });
});
