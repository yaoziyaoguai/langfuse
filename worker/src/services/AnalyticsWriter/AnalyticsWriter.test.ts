import { PrismaClient } from "@prisma/client";
import {
  canonicalPayloadHash,
  normalizeVersionToken,
  type CanonicalAnalyticsBatch,
  type CanonicalAnalyticsEvent,
} from "@langfuse/shared/analytics-persistence";
import {
  createAnalyticsIngestionReceipt,
  DorisError,
  reserveCanonicalizationFence,
} from "@langfuse/shared/src/server";
import { afterAll, beforeAll, describe, expect, it, vi } from "vitest";

import {
  CanonicalIngestionArtifactStore,
  canonicalArtifactObjectKey,
  type ConditionalCanonicalObjectStore,
} from "../CanonicalIngestionArtifactStore";
import { AnalyticsWriter } from ".";
import { DorisBatchSink } from "./DorisBatchSink";

const controlDatabaseUrl = process.env.DORIS_CONTROL_TEST_DATABASE_URL;
const acceptedAt = normalizeVersionToken("2026-07-18T12:00:00.000000000Z");
const startTime = normalizeVersionToken("2026-07-17T10:00:00.000000000Z");

class MemoryObjectStore implements ConditionalCanonicalObjectStore {
  readonly objects = new Map<string, string>();

  async putIfAbsent(input: {
    readonly key: string;
    readonly body: string;
    readonly contentType: "application/json";
  }): Promise<"created" | "already_exists"> {
    if (this.objects.has(input.key)) return "already_exists";
    this.objects.set(input.key, input.body);
    return "created";
  }

  async get(key: string): Promise<string | null> {
    return this.objects.get(key) ?? null;
  }
}

function event(input: {
  operationId: string;
  rawObjectKey: string;
  name?: string;
  traceId?: string;
  spanId?: string;
}): CanonicalAnalyticsEvent {
  const content = {
    traceId: input.traceId ?? "writer-trace-1",
    spanId: input.spanId ?? "writer-span-1",
    name: input.name ?? "generation",
  };
  return {
    kind: "event",
    projectId: "",
    partitionDate: "2026-07-17",
    sourceContract: "v4",
    sourceVersion: normalizeVersionToken("2026-07-17T10:02:00Z"),
    canonicalizerVersion: "1",
    schemaVersion: 3,
    canonicalPayloadHash: canonicalPayloadHash(content),
    systemTimestamp: acceptedAt,
    rawObjectKey: input.rawObjectKey,
    resolvedEnrichmentIds: {},
    traceId: content.traceId,
    spanId: content.spanId,
    parentSpanId: null,
    type: "GENERATION",
    name: content.name,
    environment: "default",
    version: null,
    release: null,
    traceName: null,
    startTime,
    endTime: null,
    completionStartTime: null,
    userId: null,
    sessionId: null,
    level: "DEFAULT",
    statusMessage: null,
    isAppRoot: false,
    bookmarked: false,
    public: false,
    tags: [],
    input: null,
    output: null,
    metadata: {},
    providedModelName: null,
    internalModelId: null,
    promptId: null,
    promptName: null,
    promptVersion: null,
    modelParameters: {},
    providedUsageDetails: {},
    usageDetails: {},
    providedCostDetails: {},
    costDetails: {},
    totalCost: null,
    toolDefinitions: {},
    toolCalls: [],
    toolCallNames: [],
    source: "sdk",
    ingestionSdkName: "langfuse-js",
    ingestionSdkVersion: "4.0.0",
    serviceName: null,
    telemetrySdkLanguage: null,
    eventBytes: 1,
  };
}

function batch(input: {
  projectId: string;
  operationId: string;
  rawObjectKey: string;
  name?: string;
  traceId?: string;
  spanId?: string;
}): CanonicalAnalyticsBatch {
  return {
    projectId: input.projectId,
    operationId: input.operationId,
    canonicalizerVersion: "1",
    schemaVersion: 3,
    acceptedAt,
    rawObjectKey: input.rawObjectKey,
    children: [
      {
        entity: { ...event(input), projectId: input.projectId },
        expectedSourceVersion: null,
        fenceGeneration: 1n,
        traceDeletionGeneration: 0n,
        projectDeletionGeneration: 0n,
      },
    ],
  };
}

describe.skipIf(!controlDatabaseUrl)("AnalyticsWriter", () => {
  const prisma = new PrismaClient({ datasourceUrl: controlDatabaseUrl });
  const suffix = `${Date.now()}-${Math.random().toString(16).slice(2)}`;
  const organizationId = `writer-org-${suffix}`;
  const projectId = `writer-project-${suffix}`;

  beforeAll(async () => {
    await prisma.organization.create({
      data: { id: organizationId, name: "Doris AnalyticsWriter test" },
    });
    await prisma.project.create({
      data: {
        id: projectId,
        name: "Doris AnalyticsWriter test",
        orgId: organizationId,
      },
    });
  }, 30_000);

  afterAll(async () => {
    await prisma.organization.deleteMany({ where: { id: organizationId } });
    await prisma.$disconnect();
  }, 30_000);

  async function createReceipt(input: {
    operationId: string;
    rawObjectKey: string;
    checksumCharacter?: string;
  }) {
    await createAnalyticsIngestionReceipt({
      client: prisma,
      operationId: input.operationId,
      projectId,
      sourceOperationId: `source-${input.operationId}`,
      sourceChecksum: (input.checksumCharacter ?? "a").repeat(64),
      rawObjectKey: input.rawObjectKey,
      acceptedAt: new Date("2026-07-18T12:00:00.000Z"),
      acceptedAtNanos: acceptedAt,
      canonicalizerVersion: "1",
      schemaVersion: 3,
      recoverableUntil: new Date("2026-07-25T12:00:00.000Z"),
      statusExpiresAt: new Date("2026-08-24T12:00:00.000Z"),
    });
  }

  it("freezes the complete manifest before load and replays without a second load", async () => {
    const operationId = `writer-operation-${suffix}`;
    const rawObjectKey = `events/${projectId}/raw/${operationId}.json`;
    await createReceipt({ operationId, rawObjectKey });

    const objectStore = new MemoryObjectStore();
    const load = vi.fn(async () => {
      const operation =
        await prisma.analyticsIngestionOperation.findUniqueOrThrow({
          where: { id: operationId },
          include: { candidates: true, loadBatches: true },
        });
      expect(operation.manifestState).toBe("FROZEN");
      expect(operation.candidates).toMatchObject([
        { disposition: "LOAD_REQUIRED" },
      ]);
      expect(operation.loadBatches).toMatchObject([{ status: "LOADING" }]);
      return {
        status: "Success",
        label: operation.loadBatches[0]!.label,
        numberTotalRows: 1,
        numberFilteredRows: 0,
        committed: true,
        requiresReconciliation: false,
      };
    });
    const writer = new AnalyticsWriter({
      client: prisma,
      artifactStore: new CanonicalIngestionArtifactStore(objectStore),
      doris: new DorisBatchSink({ load, reconcile: vi.fn() }),
      databaseName: "langfuse_poc",
      canonicalPrefix: "events/",
      workerId: "writer-worker-a",
      now: () => new Date("2026-07-18T12:00:10.000Z"),
    });
    const canonicalBatch = batch({ projectId, operationId, rawObjectKey });

    await expect(writer.persist(canonicalBatch)).resolves.toEqual({
      operationId,
      status: "VISIBLE",
    });
    expect(load).toHaveBeenCalledOnce();
    expect(objectStore.objects.size).toBe(1);

    await expect(
      writer.persist(
        batch({
          projectId,
          operationId,
          rawObjectKey,
          name: "mutable-current-enrichment-must-not-win",
        }),
      ),
    ).resolves.toEqual({ operationId, status: "VISIBLE" });
    expect(load).toHaveBeenCalledOnce();
  });

  it("recovers an immutable artifact written before its pointer was published", async () => {
    const operationId = `writer-put-recovery-${suffix}`;
    const rawObjectKey = `events/${projectId}/raw/${operationId}.json`;
    await createReceipt({ operationId, rawObjectKey, checksumCharacter: "b" });

    const objectStore = new MemoryObjectStore();
    const artifactStore = new CanonicalIngestionArtifactStore(objectStore);
    const canonicalBatch = batch({
      projectId,
      operationId,
      rawObjectKey,
      traceId: `writer-put-trace-${suffix}`,
      spanId: "writer-put-span",
    });
    const reservedObjectKey = canonicalArtifactObjectKey({
      prefix: "events/",
      projectId,
      operationId,
      fenceGeneration: 1n,
    });
    await expect(
      reserveCanonicalizationFence({
        client: prisma,
        operationId,
        projectId,
        expectedFence: 0n,
        nextFence: 1n,
        leaseOwner: "writer-worker-put",
        leaseUntil: new Date("2026-07-18T12:01:00.000Z"),
        now: new Date("2026-07-18T12:00:10.000Z"),
        reservedObjectKey,
        confirmedAbsentObjectKey: null,
      }),
    ).resolves.toMatchObject({ outcome: "reserved" });
    await expect(
      artifactStore.putIfAbsent(reservedObjectKey, canonicalBatch),
    ).resolves.toMatchObject({ outcome: "created" });

    const load = vi.fn(async (input: { label: string }) => ({
      status: "Success",
      label: input.label,
      numberTotalRows: 1,
      numberFilteredRows: 0,
      committed: true,
      requiresReconciliation: false,
    }));
    const writer = new AnalyticsWriter({
      client: prisma,
      artifactStore,
      doris: new DorisBatchSink({ load, reconcile: vi.fn() }),
      databaseName: "langfuse_poc",
      canonicalPrefix: "events/",
      workerId: "writer-worker-put",
      now: () => new Date("2026-07-18T12:00:11.000Z"),
    });

    await expect(writer.persist(canonicalBatch)).resolves.toEqual({
      operationId,
      status: "VISIBLE",
    });
    expect(load).toHaveBeenCalledOnce();
    expect(objectStore.objects.size).toBe(1);
    await expect(
      prisma.analyticsIngestionOperation.findUniqueOrThrow({
        where: { id: operationId },
      }),
    ).resolves.toMatchObject({
      canonicalObjectKey: reservedObjectKey,
      canonicalizationFence: 1n,
      status: "VISIBLE",
    });
  });

  it("reconciles an UNKNOWN load by label without sending the body twice", async () => {
    const operationId = `writer-unknown-${suffix}`;
    const rawObjectKey = `events/${projectId}/raw/${operationId}.json`;
    await createReceipt({ operationId, rawObjectKey, checksumCharacter: "c" });
    const load = vi.fn(async () => {
      throw new DorisError("ANALYTICS_TIMEOUT", true);
    });
    const reconcile = vi.fn(async () => ({ status: "VISIBLE", visible: true }));
    const writer = new AnalyticsWriter({
      client: prisma,
      artifactStore: new CanonicalIngestionArtifactStore(
        new MemoryObjectStore(),
      ),
      doris: new DorisBatchSink({ load, reconcile }),
      databaseName: "langfuse_poc",
      canonicalPrefix: "events/",
      workerId: "writer-worker-unknown",
      now: () => new Date("2026-07-18T12:00:20.000Z"),
    });
    const canonicalBatch = batch({
      projectId,
      operationId,
      rawObjectKey,
      traceId: `writer-unknown-trace-${suffix}`,
      spanId: "writer-unknown-span",
    });

    await expect(writer.persist(canonicalBatch)).rejects.toMatchObject({
      code: "ANALYTICS_UNAVAILABLE",
      retryable: true,
    });
    await expect(
      prisma.analyticsLoadBatch.findFirstOrThrow({ where: { operationId } }),
    ).resolves.toMatchObject({ status: "UNKNOWN" });

    await expect(writer.persist(canonicalBatch)).resolves.toEqual({
      operationId,
      status: "VISIBLE",
    });
    expect(load).toHaveBeenCalledOnce();
    expect(reconcile).toHaveBeenCalledOnce();
  });

  it("does not classify a rejected result after losing its load fence", async () => {
    const operationId = `writer-stale-load-${suffix}`;
    const rawObjectKey = `events/${projectId}/raw/${operationId}.json`;
    await createReceipt({ operationId, rawObjectKey, checksumCharacter: "f" });
    const load = vi.fn(async (input: { label: string }) => {
      const ledger = await prisma.analyticsLoadBatch.findFirstOrThrow({
        where: { operationId },
      });
      await prisma.analyticsLoadBatch.update({
        where: { id: ledger.id },
        data: {
          fenceGeneration: { increment: 1 },
          leaseOwner: "replacement-worker",
          leaseExpiresAt: new Date("2026-07-18T12:02:00.000Z"),
        },
      });
      return {
        status: "Fail",
        label: input.label,
        numberTotalRows: 0,
        numberFilteredRows: 0,
        committed: false,
        requiresReconciliation: false,
      };
    });
    const writer = new AnalyticsWriter({
      client: prisma,
      artifactStore: new CanonicalIngestionArtifactStore(
        new MemoryObjectStore(),
      ),
      doris: new DorisBatchSink({ load, reconcile: vi.fn() }),
      databaseName: "langfuse_poc",
      canonicalPrefix: "events/",
      workerId: "writer-worker-stale",
      now: () => new Date("2026-07-18T12:00:25.000Z"),
    });

    await expect(
      writer.persist(
        batch({
          projectId,
          operationId,
          rawObjectKey,
          traceId: `writer-stale-trace-${suffix}`,
          spanId: "writer-stale-span",
        }),
      ),
    ).rejects.toMatchObject({
      code: "ANALYTICS_UNAVAILABLE",
      retryable: true,
    });
    await expect(
      prisma.analyticsIngestionOperation.findUniqueOrThrow({
        where: { id: operationId },
      }),
    ).resolves.toMatchObject({ status: "PERSISTED", terminalAt: null });
  });

  it("quarantines one of two concurrent payloads with the same source token", async () => {
    const traceId = `writer-conflict-trace-${suffix}`;
    const spanId = "writer-conflict-span";
    const inputs = ["left", "right"].map((side, index) => {
      const operationId = `writer-conflict-${side}-${suffix}`;
      return {
        operationId,
        rawObjectKey: `events/${projectId}/raw/${operationId}.json`,
        checksumCharacter: index === 0 ? "d" : "e",
        name: side,
      };
    });
    await Promise.all(inputs.map((input) => createReceipt(input)));

    const load = vi.fn(async (input: { label: string }) => ({
      status: "Success",
      label: input.label,
      numberTotalRows: 1,
      numberFilteredRows: 0,
      committed: true,
      requiresReconciliation: false,
    }));
    const writer = new AnalyticsWriter({
      client: prisma,
      artifactStore: new CanonicalIngestionArtifactStore(
        new MemoryObjectStore(),
      ),
      doris: new DorisBatchSink({ load, reconcile: vi.fn() }),
      databaseName: "langfuse_poc",
      canonicalPrefix: "events/",
      workerId: "writer-worker-conflict",
      now: () => new Date("2026-07-18T12:00:30.000Z"),
    });
    const results = await Promise.allSettled(
      inputs.map((input) =>
        writer.persist(batch({ ...input, projectId, traceId, spanId })),
      ),
    );

    expect(results.map(({ status }) => status).sort()).toEqual([
      "fulfilled",
      "rejected",
    ]);
    expect(load).toHaveBeenCalledOnce();
    const persisted = await prisma.analyticsIngestionOperation.findMany({
      where: { id: { in: inputs.map(({ operationId }) => operationId) } },
      select: { status: true },
    });
    expect(persisted.map(({ status }) => status).sort()).toEqual([
      "QUARANTINED",
      "VISIBLE",
    ]);
  });
});
