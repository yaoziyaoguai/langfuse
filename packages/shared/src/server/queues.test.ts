import { describe, it, expect } from "vitest";

import {
  AnalyticsDurableProvenanceSchema,
  AnalyticsEvaluationDispatchEventSchema,
  BatchActionProcessingEventSchema,
  BatchExportJobSchema,
  ProjectQueueEventSchema,
  ScoresQueueEventSchema,
  TraceQueueEventSchema,
  IngestionEvent,
  OtelIngestionEvent,
  WebhookOutboundEnvelopeSchema,
} from "./queues";

const analyticsProvenance = {
  analyticsBackend: "DORIS" as const,
  deploymentGeneration: "7",
  workloadEpochFingerprint: "a".repeat(64),
  runtimeContractVersion: 3,
  producerRuntimeLeaseId: "runtime-producer",
};

describe("batch evaluation source provenance", () => {
  const basePayload = {
    actionId: "observation-run-batched-evaluation" as const,
    projectId: "project-01",
    query: {
      filter: [],
      orderBy: { column: "startTime", order: "DESC" as const },
    },
    cutoffCreatedAt: new Date("2026-07-22T00:00:00.000Z"),
    batchActionId: "batch-action-01",
    evaluatorIds: ["evaluator-01"],
  };

  it("defaults old queue jobs to the events source", () => {
    const parsed = BatchActionProcessingEventSchema.parse(basePayload);

    expect(parsed).toMatchObject({ sourceTable: "events" });
  });

  it("preserves an experiment source in the durable queue payload", () => {
    const parsed = BatchActionProcessingEventSchema.parse({
      ...basePayload,
      sourceTable: "experiments",
    });

    expect(parsed).toMatchObject({ sourceTable: "experiments" });
  });
});

describe("analytics durable provenance queue schema", () => {
  it("accepts a complete capability stamp and rejects partial capability identity", () => {
    const capabilityProvenance = {
      ...analyticsProvenance,
      capability: "datasetRunIngestion" as const,
      capabilityActivationGeneration: "3",
      capabilityContractVersion: 1,
    };

    expect(
      AnalyticsDurableProvenanceSchema.safeParse(capabilityProvenance).success,
    ).toBe(true);
    expect(
      AnalyticsDurableProvenanceSchema.safeParse({
        ...capabilityProvenance,
        capabilityContractVersion: undefined,
      }).success,
    ).toBe(false);
  });
});

const validMonitorEnvelope = {
  id: "exe_01",
  timestamp: new Date("2026-05-18T12:01:00.000Z"),
  type: "monitor-alert" as const,
  apiVersion: "v1" as const,
  payload: {
    monitorId: "mon_01",
    projectId: "proj_01",
    permalink: "https://cloud.langfuse.com/project/proj_01/monitors/mon_01",
    message: { title: "[ALERT] err", body: "errors > 100" },
    severity: "ALERT" as const,
    timestamp: new Date("2026-05-18T12:01:00.000Z"),
    fromTimestamp: new Date("2026-05-18T11:55:30.000Z"),
    toTimestamp: new Date("2026-05-18T12:00:30.000Z"),
    view: "observations" as const,
    filters: [],
    window: "5m" as const,
  },
};

describe("WebhookOutboundEnvelopeSchema (discriminated union)", () => {
  it("parses a monitor-alert envelope", () => {
    const parsed =
      WebhookOutboundEnvelopeSchema.safeParse(validMonitorEnvelope);
    expect(parsed.success).toBe(true);
    if (parsed.success && parsed.data.type === "monitor-alert") {
      expect(parsed.data.payload.severity).toBe("ALERT");
    }
  });

  it("rejects an unknown discriminator", () => {
    expect(
      WebhookOutboundEnvelopeSchema.safeParse({ type: "bogus" }).success,
    ).toBe(false);
  });

  it("rejects a monitor-alert envelope with missing payload", () => {
    const { payload: _unused, ...withoutPayload } = validMonitorEnvelope;
    expect(
      WebhookOutboundEnvelopeSchema.safeParse(withoutPayload).success,
    ).toBe(false);
  });
});

describe("batch export queue provenance", () => {
  const managed = {
    projectId: "project-01",
    batchExportId: "export-01",
    dispatchGeneration: 1,
    analyticsBackend: "DORIS" as const,
    deploymentGeneration: "7",
    workloadEpochFingerprint: "a".repeat(64),
    runtimeContractVersion: 1,
    capabilityActivationGeneration: "3",
    capabilityContractVersion: 1,
  };

  it("accepts only complete managed Doris payloads or strict ClickHouse legacy payloads", () => {
    expect(BatchExportJobSchema.safeParse(managed).success).toBe(true);
    expect(
      BatchExportJobSchema.safeParse({
        projectId: "legacy-project",
        batchExportId: "legacy-export",
      }).success,
    ).toBe(true);
    expect(
      BatchExportJobSchema.safeParse({
        ...managed,
        capabilityActivationGeneration: undefined,
      }).success,
    ).toBe(false);
  });

  it("rejects backend and generation tampering", () => {
    expect(
      BatchExportJobSchema.safeParse({
        ...managed,
        analyticsBackend: "CLICKHOUSE",
      }).success,
    ).toBe(false);
    expect(
      BatchExportJobSchema.safeParse({
        ...managed,
        deploymentGeneration: "0",
      }).success,
    ).toBe(false);
  });
});

describe("evaluation dispatch queue provenance", () => {
  const envelope = {
    dispatchId: "dispatch-01",
    dispatchGeneration: 1,
    projectId: "project-01",
    operationId: "operation-01",
    targetType: "TRACE_UPSERT" as const,
    targetId: "trace-01",
    analyticsBackend: "DORIS" as const,
    deploymentGeneration: "7",
    workloadEpochFingerprint: "a".repeat(64),
    runtimeContractVersion: 1,
    capabilityActivationGeneration: "3",
    capabilityContractVersion: 1,
  };

  it("accepts a complete strict Doris envelope", () => {
    expect(
      AnalyticsEvaluationDispatchEventSchema.safeParse(envelope).success,
    ).toBe(true);
  });

  it("rejects missing provenance and payload extensions", () => {
    expect(
      AnalyticsEvaluationDispatchEventSchema.safeParse({
        ...envelope,
        capabilityActivationGeneration: undefined,
      }).success,
    ).toBe(false);
    expect(
      AnalyticsEvaluationDispatchEventSchema.safeParse({
        ...envelope,
        untrustedTarget: "other",
      }).success,
    ).toBe(false);
  });
});

describe("ingestion queue payload compatibility", () => {
  it("accepts ingestion jobs created before attribution fields existed", () => {
    const parsed = IngestionEvent.safeParse({
      data: {
        type: "trace-create",
        eventBodyId: "trace-01",
        fileKey: "event-01",
      },
      authCheck: {
        validKey: true,
        scope: {
          projectId: "project-01",
        },
      },
    });

    expect(parsed.success).toBe(true);
  });

  it("accepts otel jobs with omitted attribution fields", () => {
    const parsed = OtelIngestionEvent.safeParse({
      data: {
        fileKey: "otel-01",
      },
      authCheck: {
        validKey: true,
        scope: {
          projectId: "project-01",
          accessLevel: "project",
        },
      },
    });

    expect(parsed.success).toBe(true);
  });
});

describe("analytics deletion queue provenance", () => {
  it("preserves score deletion provenance for direct and batch jobs", () => {
    expect(
      ScoresQueueEventSchema.parse({
        projectId: "project-01",
        scoreIds: ["score-01"],
        deletionOperationId: "operation-direct-01",
        deletionGeneration: "1",
        analyticsProvenance,
      }).analyticsProvenance,
    ).toEqual(analyticsProvenance);
    const batchPayload = BatchActionProcessingEventSchema.parse({
      actionId: "score-delete",
      projectId: "project-01",
      query: {
        filter: [],
        orderBy: { column: "timestamp", order: "DESC" },
      },
      tableName: "scores",
      cutoffCreatedAt: new Date("2026-07-22T00:00:00.000Z"),
      type: "delete",
      deletionOperationId: "operation-batch-01",
      deletionGeneration: "1",
      analyticsProvenance,
    });
    expect(batchPayload.actionId).toBe("score-delete");
    if (batchPayload.actionId !== "score-delete") {
      throw new Error("Expected score-delete payload");
    }
    expect(batchPayload.analyticsProvenance).toEqual(analyticsProvenance);
  });

  it("accepts only complete managed score references or legacy unstamped payloads", () => {
    expect(
      ScoresQueueEventSchema.safeParse({
        projectId: "project-legacy",
        scoreIds: ["score-legacy"],
      }).success,
    ).toBe(true);
    expect(
      ScoresQueueEventSchema.safeParse({
        projectId: "project-01",
        scoreIds: ["score-01"],
        analyticsProvenance,
      }).success,
    ).toBe(false);
    expect(
      ScoresQueueEventSchema.safeParse({
        projectId: "project-01",
        scoreIds: ["score-01"],
        deletionOperationId: "operation-01",
        analyticsProvenance,
      }).success,
    ).toBe(false);
  });

  it("rejects partial managed batch score references", () => {
    expect(
      BatchActionProcessingEventSchema.safeParse({
        actionId: "score-delete",
        projectId: "project-01",
        query: { filter: [], orderBy: null },
        tableName: "scores",
        cutoffCreatedAt: new Date("2026-07-22T00:00:00.000Z"),
        type: "delete",
        analyticsProvenance,
      }).success,
    ).toBe(false);
  });

  it("preserves the authoritative provenance copied into trace references", () => {
    expect(
      TraceQueueEventSchema.parse({
        projectId: "project-01",
        traceId: "trace-01",
        deletionOperations: [
          {
            operationId: "operation-01",
            traceId: "trace-01",
            generation: "1",
            analyticsProvenance,
          },
        ],
      }).deletionOperations?.[0]?.analyticsProvenance,
    ).toEqual(analyticsProvenance);
  });

  it("preserves project provenance while accepting legacy unstamped jobs", () => {
    expect(
      ProjectQueueEventSchema.parse({
        projectId: "project-01",
        orgId: "org-01",
        deletionOperationId: "operation-01",
        deletionGeneration: "1",
        analyticsProvenance,
      }).analyticsProvenance,
    ).toEqual(analyticsProvenance);
    expect(
      ProjectQueueEventSchema.safeParse({
        projectId: "project-legacy",
        orgId: "org-legacy",
      }).success,
    ).toBe(true);
  });

  it("rejects a tampered deletion provenance envelope", () => {
    expect(
      ProjectQueueEventSchema.safeParse({
        projectId: "project-01",
        orgId: "org-01",
        deletionOperationId: "operation-01",
        deletionGeneration: "1",
        analyticsProvenance: {
          ...analyticsProvenance,
          deploymentGeneration: "0",
        },
      }).success,
    ).toBe(false);
  });

  it("rejects an empty durable trace deletion reference list", () => {
    expect(
      TraceQueueEventSchema.safeParse({
        projectId: "project-01",
        traceId: "trace-01",
        deletionOperations: [],
      }).success,
    ).toBe(false);
  });
});
