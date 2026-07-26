import type { AnalyticsIngestionOperation, PrismaClient } from "@prisma/client";
import { describe, expect, it, vi } from "vitest";

import {
  calculateAnalyticsIngestionRetryDelayMs,
  createAnalyticsIngestionReceipt,
  expireUnreadyAnalyticsIngestionReceipts,
  handoffLegacyAnalyticsIngestionOutbox,
} from "./analyticsIngestionOperations";

const ACCEPTED_AT = new Date("2026-07-18T12:00:00.000Z");

function pendingReceipt(
  overrides: Partial<AnalyticsIngestionOperation> = {},
): AnalyticsIngestionOperation {
  return {
    id: "operation-1",
    projectId: "project-1",
    sourceOperationId: "source-1",
    sourceChecksum: "a".repeat(64),
    rawObjectKey: "analytics-ingestion/raw/project-1/operation-1.json",
    acceptedAt: ACCEPTED_AT,
    acceptedAtNanos: 1_784_376_000_000_000_000n,
    canonicalizerVersion: "1",
    schemaVersion: 3,
    analyticsBackend: null,
    deploymentGeneration: null,
    workloadEpochFingerprint: null,
    runtimeContractVersion: null,
    producerRuntimeLeaseId: null,
    checkpointGeneration: 0n,
    canonicalizationFence: 0n,
    canonicalizationLeaseOwner: null,
    canonicalizationLeaseUntil: null,
    canonicalizationAttempts: 0,
    reservedCanonicalObjectKey: null,
    canonicalObjectKey: null,
    canonicalArtifactChecksum: null,
    manifestState: "PENDING",
    candidateManifest: null,
    frozenManifest: null,
    status: "ACCEPTED",
    cancellationReasonCode: null,
    lastErrorCode: null,
    recoverableUntil: new Date("2026-07-25T12:00:00.000Z"),
    statusExpiresAt: new Date("2026-08-24T12:00:00.000Z"),
    visibleAt: null,
    terminalAt: null,
    createdAt: ACCEPTED_AT,
    updatedAt: ACCEPTED_AT,
    ...overrides,
  } as AnalyticsIngestionOperation;
}

describe("calculateAnalyticsIngestionRetryDelayMs", () => {
  it("applies deterministic jitter to capped exponential backoff", () => {
    const first = calculateAnalyticsIngestionRetryDelayMs({
      operationId: "operation-1",
      generation: 1,
    });
    const second = calculateAnalyticsIngestionRetryDelayMs({
      operationId: "operation-1",
      generation: 2,
    });
    const capped = calculateAnalyticsIngestionRetryDelayMs({
      operationId: "operation-1",
      generation: 50,
    });

    expect(first).toBeGreaterThanOrEqual(3_750);
    expect(first).toBeLessThanOrEqual(6_250);
    expect(second).toBeGreaterThan(first);
    expect(capped).toBeGreaterThanOrEqual(22.5 * 60_000);
    expect(capped).toBeLessThanOrEqual(30 * 60_000);
    expect(
      calculateAnalyticsIngestionRetryDelayMs({
        operationId: "operation-1",
        generation: 50,
      }),
    ).toBe(capped);
  });

  it("rejects invalid generations", () => {
    expect(() =>
      calculateAnalyticsIngestionRetryDelayMs({
        operationId: "operation-1",
        generation: 0,
      }),
    ).toThrow("Invalid analytics ingestion retry generation");
  });
});

describe("ledger-first raw readiness", () => {
  it("does not expire an old pending receipt while its raw artifact exists", async () => {
    const operation = pendingReceipt({
      createdAt: new Date("2026-07-18T11:29:00.000Z"),
    });
    const updateMany = vi.fn();
    const rawArtifactExists = vi.fn(async () => true);
    const transaction = {
      $queryRaw: vi.fn().mockResolvedValue([{ id: operation.id }]),
      analyticsIngestionOperation: {
        findFirst: vi.fn().mockResolvedValue(operation),
        updateMany,
      },
      analyticsIngestionOutboxV2: {
        count: vi.fn().mockResolvedValue(0),
      },
    };
    const client = {
      $queryRaw: vi
        .fn()
        .mockResolvedValue([{ now: new Date("2026-07-18T12:00:00.000Z") }]),
      analyticsIngestionOperation: {
        findMany: vi.fn().mockResolvedValue([
          {
            id: operation.id,
            projectId: operation.projectId,
            rawObjectKey: operation.rawObjectKey,
          },
        ]),
      },
      $transaction: vi.fn((callback) => callback(transaction)),
    } as unknown as PrismaClient;

    await expect(
      expireUnreadyAnalyticsIngestionReceipts({
        client,
        graceMs: 30 * 60_000,
        rawArtifactExists,
      }),
    ).resolves.toBe(0);

    expect(rawArtifactExists).toHaveBeenCalledWith(operation.rawObjectKey);
    expect(updateMany).not.toHaveBeenCalled();
  });

  it("rechecks terminal state under the operation lock before publishing outbox", async () => {
    const snapshot = pendingReceipt();
    const terminal = pendingReceipt({
      status: "UNRECOVERABLE",
      terminalAt: new Date("2026-07-18T12:31:00.000Z"),
    });
    const upsert = vi.fn();
    const transaction = {
      $queryRaw: vi
        .fn()
        .mockResolvedValueOnce([{ locked: "" }])
        .mockResolvedValueOnce([{ locked: "" }])
        .mockResolvedValueOnce([{ now: ACCEPTED_AT }])
        .mockResolvedValueOnce([{ id: snapshot.projectId }])
        .mockResolvedValueOnce([{ locked: "" }])
        .mockResolvedValueOnce([{ id: "global" }])
        .mockResolvedValueOnce([{ id: snapshot.id }]),
      analyticsCheckpointGeneration: {
        findFirst: vi.fn().mockResolvedValue(null),
      },
      analyticsBackendDeploymentState: {
        findUnique: vi.fn().mockResolvedValue(null),
      },
      analyticsIngestionOperation: {
        findFirst: vi.fn().mockResolvedValue(terminal),
      },
      analyticsIngestionOutboxV2: { upsert },
    };
    const client = {
      analyticsIngestionOperation: {
        findFirst: vi.fn().mockResolvedValue(snapshot),
      },
      $transaction: vi.fn((callback) => callback(transaction)),
    } as unknown as PrismaClient;

    await expect(
      createAnalyticsIngestionReceipt({
        client,
        operationId: snapshot.id,
        projectId: snapshot.projectId,
        sourceOperationId: snapshot.sourceOperationId,
        sourceChecksum: snapshot.sourceChecksum,
        rawObjectKey: snapshot.rawObjectKey,
        acceptedAt: snapshot.acceptedAt,
        acceptedAtNanos: snapshot.acceptedAtNanos,
        canonicalizerVersion: snapshot.canonicalizerVersion,
        schemaVersion: snapshot.schemaVersion,
        recoverableUntil: snapshot.recoverableUntil,
        statusExpiresAt: snapshot.statusExpiresAt,
        publishReady: true,
        rawArtifactVerified: true,
      }),
    ).rejects.toThrow("Ingestion source operation conflicts with its receipt");

    expect(
      transaction.analyticsIngestionOperation.findFirst,
    ).toHaveBeenCalled();
    expect(upsert).not.toHaveBeenCalled();
  });

  it("revives a matching raw receipt when expiry wins before upload promotion", async () => {
    const snapshot = pendingReceipt();
    const expired = pendingReceipt({
      status: "UNRECOVERABLE",
      lastErrorCode: "RAW_ARTIFACT_UNAVAILABLE",
      terminalAt: new Date("2026-07-18T12:31:00.000Z"),
    });
    const updateMany = vi.fn().mockResolvedValue({ count: 1 });
    const upsert = vi.fn();
    const transaction = {
      $queryRaw: vi
        .fn()
        .mockResolvedValueOnce([{ locked: "" }])
        .mockResolvedValueOnce([{ locked: "" }])
        .mockResolvedValueOnce([{ now: ACCEPTED_AT }])
        .mockResolvedValueOnce([{ id: snapshot.projectId }])
        .mockResolvedValueOnce([{ locked: "" }])
        .mockResolvedValueOnce([{ id: "global" }])
        .mockResolvedValueOnce([{ id: snapshot.id }])
        .mockResolvedValueOnce([{ now: new Date("2026-07-18T12:32:00.000Z") }]),
      analyticsCheckpointGeneration: {
        findFirst: vi.fn().mockResolvedValue(null),
      },
      analyticsBackendDeploymentState: {
        findUnique: vi.fn().mockResolvedValue(null),
      },
      analyticsIngestionOperation: {
        findFirst: vi.fn().mockResolvedValue(expired),
        updateMany,
      },
      analyticsIngestionOutboxV2: { upsert },
    };
    const client = {
      analyticsIngestionOperation: {
        findFirst: vi.fn().mockResolvedValue(snapshot),
      },
      $transaction: vi.fn((callback) => callback(transaction)),
    } as unknown as PrismaClient;

    await expect(
      createAnalyticsIngestionReceipt({
        client,
        operationId: snapshot.id,
        projectId: snapshot.projectId,
        sourceOperationId: snapshot.sourceOperationId,
        sourceChecksum: snapshot.sourceChecksum,
        rawObjectKey: snapshot.rawObjectKey,
        acceptedAt: snapshot.acceptedAt,
        acceptedAtNanos: snapshot.acceptedAtNanos,
        canonicalizerVersion: snapshot.canonicalizerVersion,
        schemaVersion: snapshot.schemaVersion,
        recoverableUntil: snapshot.recoverableUntil,
        statusExpiresAt: snapshot.statusExpiresAt,
        publishReady: true,
        rawArtifactVerified: true,
      } as never),
    ).resolves.toMatchObject({ created: false });

    expect(updateMany).toHaveBeenCalledWith({
      where: expect.objectContaining({
        id: snapshot.id,
        projectId: snapshot.projectId,
        status: "UNRECOVERABLE",
        lastErrorCode: "RAW_ARTIFACT_UNAVAILABLE",
        terminalAt: expired.terminalAt,
        recoverableUntil: {
          gte: new Date("2026-07-18T12:32:00.000Z"),
        },
        outbox: null,
        outboxV2: null,
      }),
      data: {
        status: "ACCEPTED",
        lastErrorCode: null,
        terminalAt: null,
      },
    });
    expect(upsert).toHaveBeenCalledOnce();
  });
});

describe("handoffLegacyAnalyticsIngestionOutbox", () => {
  it("moves a legacy row to V2 under the operation lock", async () => {
    const now = new Date("2026-07-18T12:05:00.000Z");
    const createV2 = vi.fn().mockResolvedValue({ id: "v2-1" });
    const deleteLegacy = vi.fn().mockResolvedValue({ count: 1 });
    const findMany = vi.fn().mockResolvedValue([
      {
        id: "legacy-1",
        operationId: "operation-1",
        operation: { projectId: "project-1" },
      },
    ]);
    let claimId = "";
    const claimRows = vi.fn(
      async ({ data }: { data: { lockedBy: string } }) => {
        claimId = data.lockedBy;
        return { count: 1 };
      },
    );
    const transaction = {
      $queryRaw: vi
        .fn()
        .mockResolvedValueOnce([{ acquired: true }])
        .mockResolvedValueOnce([{ id: "operation-1" }]),
      analyticsIngestionOperation: {
        findFirst: vi.fn().mockResolvedValue({
          id: "operation-1",
          projectId: "project-1",
          terminalAt: null,
        }),
      },
      analyticsIngestionOutbox: {
        findMany,
        updateMany: claimRows,
        findUnique: vi.fn(async () => ({
          id: "legacy-1",
          attempts: 3,
          lockedBy: claimId,
        })),
        deleteMany: deleteLegacy,
      },
      analyticsIngestionOutboxV2: {
        findUnique: vi.fn().mockResolvedValue(null),
        create: createV2,
      },
    };
    const client = {
      $transaction: vi.fn((callback) => callback(transaction)),
    } as unknown as PrismaClient;

    await expect(
      handoffLegacyAnalyticsIngestionOutbox({ client, now, limit: 10 }),
    ).resolves.toBe(1);
    expect(findMany).toHaveBeenCalledWith({
      where: {
        operation: { terminalAt: null },
        OR: [
          { lockedBy: null },
          { lockedBy: { not: { startsWith: "doris-handoff:" } } },
          { lockedUntil: null },
          { lockedUntil: { lte: now } },
        ],
      },
      orderBy: [{ updatedAt: "asc" }, { id: "asc" }],
      take: 10,
      select: {
        id: true,
        operationId: true,
        operation: { select: { projectId: true } },
      },
    });
    expect(claimRows).toHaveBeenCalledWith({
      where: { id: { in: ["legacy-1"] } },
      data: {
        lockedBy: expect.stringMatching(/^doris-handoff:/),
        lockedUntil: new Date("2026-07-18T12:10:00.000Z"),
      },
    });
    expect(createV2).toHaveBeenCalledWith({
      data: {
        operationId: "operation-1",
        status: "PENDING",
        generation: 1,
        attempts: 3,
        nextAttemptAt: now,
      },
    });
    expect(deleteLegacy).toHaveBeenCalledWith({
      where: { id: "legacy-1", operationId: "operation-1" },
    });
  });

  it("leaves a terminal legacy operation untouched", async () => {
    const transaction = {
      $queryRaw: vi
        .fn()
        .mockResolvedValueOnce([{ acquired: true }])
        .mockResolvedValueOnce([{ id: "operation-1" }]),
      analyticsIngestionOperation: {
        findFirst: vi.fn().mockResolvedValue({
          id: "operation-1",
          projectId: "project-1",
          terminalAt: new Date("2026-07-18T12:00:00.000Z"),
        }),
      },
      analyticsIngestionOutbox: {
        findMany: vi.fn().mockResolvedValue([
          {
            id: "legacy-1",
            operationId: "operation-1",
            operation: { projectId: "project-1" },
          },
        ]),
        updateMany: vi.fn().mockResolvedValue({ count: 1 }),
        findUnique: vi.fn(),
        deleteMany: vi.fn(),
      },
      analyticsIngestionOutboxV2: {
        findUnique: vi.fn(),
        create: vi.fn(),
      },
    };
    const client = {
      $transaction: vi.fn((callback) => callback(transaction)),
    } as unknown as PrismaClient;

    await expect(
      handoffLegacyAnalyticsIngestionOutbox({
        client,
        now: new Date("2026-07-18T12:05:00.000Z"),
        limit: 10,
      }),
    ).resolves.toBe(0);
    expect(
      transaction.analyticsIngestionOutbox.findUnique,
    ).not.toHaveBeenCalled();
    expect(
      transaction.analyticsIngestionOutboxV2.create,
    ).not.toHaveBeenCalled();
    expect(
      transaction.analyticsIngestionOutbox.deleteMany,
    ).not.toHaveBeenCalled();
  });

  it("does not scan when another replica owns the handoff lock", async () => {
    const findMany = vi.fn();
    const transaction = {
      $queryRaw: vi.fn().mockResolvedValue([{ acquired: false }]),
      analyticsIngestionOutbox: { findMany },
    };
    const client = {
      $transaction: vi.fn((callback) => callback(transaction)),
    } as unknown as PrismaClient;

    await expect(
      handoffLegacyAnalyticsIngestionOutbox({
        client,
        now: new Date("2026-07-18T12:05:00.000Z"),
        limit: 10,
      }),
    ).resolves.toBe(0);
    expect(findMany).not.toHaveBeenCalled();
  });
});
