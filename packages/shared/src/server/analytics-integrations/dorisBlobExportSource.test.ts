import { describe, expect, it, vi } from "vitest";

import { DorisBlobAnalyticsExportSource } from "./dorisBlobExportSource";

const observation = {
  id: "observation-1",
  traceId: "trace-1",
  projectId: "project-1",
  type: "SPAN",
  parentObservationId: null,
  startTime: new Date("2026-07-25T00:00:00.000Z"),
  endTime: new Date("2026-07-25T00:00:01.000Z"),
  environment: "production",
  name: "span",
  level: "DEFAULT",
  statusMessage: null,
  version: "v1",
  bookmarked: false,
  public: false,
  userId: "user-1",
  sessionId: "session-1",
  input: { privateInput: true },
  output: { privateOutput: true },
  metadata: { tenant: "allowed-only-when-selected" },
  providedModelName: null,
  internalModelId: null,
  modelParameters: {},
  providedUsageDetails: {},
  usageDetails: {},
  providedCostDetails: {},
  costDetails: {},
  completionStartTime: null,
  promptId: null,
  promptName: null,
  promptVersion: null,
  totalCost: null,
  latency: 1,
  timeToFirstToken: null,
  createdAt: new Date("2026-07-25T00:00:00.000Z"),
  updatedAt: new Date("2026-07-25T00:00:01.000Z"),
  toolCalls: [],
  toolCallNames: [],
  toolDefinitions: {},
  traceName: "trace",
  release: null,
  tags: [],
};

function source() {
  return new DorisBlobAnalyticsExportSource({
    repositories: {
      traces: { getMany: vi.fn(async () => []) },
      observations: {
        get: vi.fn(async ({ observationId }) =>
          observationId === observation.id ? observation : null,
        ),
      },
      scores: { get: vi.fn(async () => null) },
    } as never,
    findTraceControls: vi.fn(async () => []),
  });
}

describe("DorisBlobAnalyticsExportSource", () => {
  it("exports only requested observation field groups plus canonical identity", async () => {
    const result = await source().readExact({
      projectId: "project-1",
      observationTable: "observations_v2",
      observationFieldGroups: ["metadata"],
      items: [
        {
          deliveryKind: "OBSERVATION",
          entityKey: "observation-1",
          deliveryIds: ["delivery-1"],
        },
      ],
    });

    expect(result.missing).toEqual([]);
    expect(result.records).toHaveLength(1);
    expect(result.records[0]).toMatchObject({
      table: "observations_v2",
      row: {
        id: "observation-1",
        trace_id: "trace-1",
        project_id: "project-1",
        type: "SPAN",
        metadata: { tenant: "allowed-only-when-selected" },
      },
    });
    expect(result.records[0]?.row).not.toHaveProperty("input");
    expect(result.records[0]?.row).not.toHaveProperty("output");
    expect(result.records[0]?.row).not.toHaveProperty("model_parameters");
  });

  it("marks missing canonical identities and ignores the paired generation marker", async () => {
    const result = await source().readExact({
      projectId: "project-1",
      observationTable: "observations",
      observationFieldGroups: ["core"],
      items: [
        {
          deliveryKind: "GENERATION",
          entityKey: "observation-1",
          deliveryIds: ["generation-delivery"],
        },
        {
          deliveryKind: "SCORE",
          entityKey: "deleted-score",
          deliveryIds: ["score-delivery"],
        },
      ],
    });

    expect(result.records).toEqual([]);
    expect(result.missing).toEqual([
      {
        deliveryKind: "SCORE",
        entityKey: "deleted-score",
        deliveryIds: ["score-delivery"],
      },
    ]);
  });
});
