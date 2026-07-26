import { describe, expect, it, vi } from "vitest";

import { DorisAnalyticsIntegrationExportSource } from "./dorisExportSource";

async function* identities(values: readonly Record<string, string>[]) {
  yield* values;
}

const trace = {
  id: "trace-1",
  projectId: "project-1",
  timestamp: new Date("2026-07-25T00:00:00.000Z"),
  endTime: new Date("2026-07-25T00:00:02.000Z"),
  name: "checkout",
  userId: "user-1",
  sessionId: "session-1",
  totalCost: 1.25,
  observationCount: 2,
  latency: 2,
  release: "release-1",
  version: "v1",
  tags: ["prod"],
  environment: "production",
  metadata: {
    $posthog_session_id: "ph-session",
    $mixpanel_session_id: "mp-session",
    private: "must-not-be-exported",
  },
};

const generation = {
  id: "generation-1",
  traceId: "trace-1",
  projectId: "project-1",
  type: "GENERATION",
  startTime: new Date("2026-07-25T00:00:01.000Z"),
  endTime: new Date("2026-07-25T00:00:02.000Z"),
  name: "completion",
  traceName: "checkout",
  userId: "user-1",
  sessionId: "session-1",
  totalCost: 1,
  totalInputTokens: 10,
  totalOutputTokens: 20,
  totalUsage: 30,
  latency: 1,
  timeToFirstToken: 0.2,
  release: "release-1",
  version: "v1",
  providedModelName: "model",
  level: "DEFAULT",
  tags: ["prod"],
  environment: "production",
  metadata: { secret: "must-not-be-exported" },
};

function repositories() {
  return {
    traces: {
      getMany: vi.fn(async ({ traceIds }) =>
        traceIds.includes(trace.id) ? [trace] : [],
      ),
      scanIdentities: vi.fn(() => identities([{ id: trace.id }])),
    },
    observations: {
      get: vi.fn(async ({ observationId }) =>
        observationId === generation.id ? generation : null,
      ),
      scanIdentities: vi.fn(() =>
        identities([{ id: generation.id, traceId: trace.id }]),
      ),
    },
    scores: {
      get: vi.fn(async () => null),
      scanIdentities: vi.fn(() => identities([{ id: "score-1" }])),
    },
  };
}

describe("DorisAnalyticsIntegrationExportSource", () => {
  it("seals a bounded full-history identity inventory", async () => {
    const dependencies = repositories();
    const source = new DorisAnalyticsIntegrationExportSource(
      dependencies as never,
    );

    await expect(
      source.scanBootstrapIdentities({
        projectId: "project-1",
        limit: 10,
        now: new Date("2026-07-25T00:00:00.000Z"),
      }),
    ).resolves.toEqual([
      { deliveryKind: "TRACE", entityKey: "trace-1" },
      { deliveryKind: "OBSERVATION", entityKey: "generation-1" },
      { deliveryKind: "SCORE", entityKey: "score-1" },
    ]);
    expect(dependencies.traces.scanIdentities).toHaveBeenCalledWith(
      expect.objectContaining({
        range: {
          from: new Date("1970-01-01T00:00:00.000Z"),
          to: new Date("2026-07-25T00:00:00.001Z"),
        },
      }),
    );
    expect(dependencies.observations.scanIdentities).toHaveBeenCalledWith(
      expect.objectContaining({
        range: {
          from: new Date("1970-01-01T00:00:00.000Z"),
          to: new Date("2026-07-25T00:00:00.001Z"),
        },
      }),
    );
  });

  it("fails instead of truncating an over-budget bootstrap", async () => {
    const dependencies = repositories();
    dependencies.traces.scanIdentities.mockReturnValue(
      identities([{ id: "trace-1" }, { id: "trace-2" }]),
    );
    const source = new DorisAnalyticsIntegrationExportSource(
      dependencies as never,
    );

    await expect(
      source.scanBootstrapIdentities({
        projectId: "project-1",
        limit: 1,
      }),
    ).rejects.toThrow(/exceeds its durable manifest budget/i);
  });

  it("projects allowlisted semantic events and expands bootstrap generations once", async () => {
    const source = new DorisAnalyticsIntegrationExportSource(
      repositories() as never,
    );
    const result = await source.readExact({
      projectId: "project-1",
      projectName: "Project One",
      items: [
        {
          deliveryKind: "TRACE",
          entityKey: "trace-1",
          deliveryIds: [],
        },
        {
          deliveryKind: "OBSERVATION",
          entityKey: "generation-1",
          deliveryIds: [],
        },
        {
          deliveryKind: "SCORE",
          entityKey: "deleted-score",
          deliveryIds: [],
        },
      ],
    });

    expect(result.records.map(({ deliveryKind }) => deliveryKind)).toEqual([
      "TRACE",
      "OBSERVATION",
      "GENERATION",
    ]);
    expect(result.missing).toEqual([
      {
        deliveryKind: "SCORE",
        entityKey: "deleted-score",
        deliveryIds: [],
      },
    ]);
    expect(result.records[0]?.event).toMatchObject({
      langfuse_id: "trace-1",
      langfuse_project_id: "project-1",
      posthog_session_id: "ph-session",
      mixpanel_session_id: "mp-session",
    });
    expect(result.records[0]?.event).not.toHaveProperty("metadata");
    expect(result.records[1]?.event).not.toHaveProperty("input");
    expect(result.records[1]?.event).not.toHaveProperty("output");
  });

  it("does not duplicate a generation when both incremental kinds are sealed", async () => {
    const source = new DorisAnalyticsIntegrationExportSource(
      repositories() as never,
    );
    const result = await source.readExact({
      projectId: "project-1",
      projectName: "Project One",
      items: [
        {
          deliveryKind: "OBSERVATION",
          entityKey: "generation-1",
          deliveryIds: ["observation-delivery"],
        },
        {
          deliveryKind: "GENERATION",
          entityKey: "generation-1",
          deliveryIds: ["generation-delivery"],
        },
      ],
    });

    expect(result.records.map(({ deliveryKind }) => deliveryKind)).toEqual([
      "OBSERVATION",
      "GENERATION",
    ]);
  });
});
