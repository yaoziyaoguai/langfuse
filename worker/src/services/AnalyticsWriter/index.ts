import { createHash } from "node:crypto";

import type { PrismaClient } from "@prisma/client";
import {
  AnalyticsPersistenceError,
  assertAnalyticsBatchBoundary,
  type AnalyticsBatchReceipt,
  type AnalyticsBatchSink,
  type CanonicalAnalyticsBatch,
} from "@langfuse/shared/analytics-persistence";
import {
  claimAnalyticsEntityHead,
  claimAnalyticsLoadBatch,
  cancelAnalyticsLoadBatchIfDeleted,
  completeAnalyticsIngestionOperation,
  findAnalyticsIngestionOperationForProject,
  freezeAnalyticsIngestionManifest,
  getProjectDeletionGeneration,
  getTraceDeletionGeneration,
  publishCanonicalArtifact,
  recordAnalyticsLoadOutcome,
  recordAnalyticsLoadReconciliation,
  reserveCanonicalizationFence,
  DorisError,
} from "@langfuse/shared/src/server";

import {
  CanonicalIngestionArtifactStore,
  canonicalArtifactObjectKey,
} from "../CanonicalIngestionArtifactStore";
import {
  describeCanonicalCandidates,
  DorisBatchSink,
  prepareDorisLoadBatches,
  type CanonicalCandidateDescriptor,
  type PreparedDorisLoadBatch,
} from "./DorisBatchSink";

const LEASE_MS = 60_000;
const RECONCILIATION_STATES: ReadonlySet<string> = new Set([
  "UNKNOWN",
  "PREPARE",
  "COMMITTED",
  "VISIBLE",
  "ABORTED",
]);

type FrozenCandidateDisposition = {
  candidateKey: string;
  disposition:
    | "LOAD_REQUIRED"
    | "NOOP"
    | "QUARANTINED"
    | "CANCELLED_BY_DELETION";
  loadBatchId: string | null;
  reasonCode: string | null;
  quarantineExpiresAt: Date | null;
};

function sha256(value: string): string {
  return createHash("sha256").update(value, "utf8").digest("hex");
}

function partitionDate(value: string): Date {
  const date = new Date(`${value}T00:00:00.000Z`);
  if (
    Number.isNaN(date.getTime()) ||
    date.toISOString().slice(0, 10) !== value
  ) {
    throw new AnalyticsPersistenceError("ANALYTICS_VALIDATION_ERROR", false);
  }
  return date;
}

function operationMatchesBatch(
  operation: {
    readonly id: string;
    readonly projectId: string;
    readonly rawObjectKey: string;
    readonly acceptedAtNanos: bigint;
    readonly canonicalizerVersion: string;
    readonly schemaVersion: number;
  },
  batch: CanonicalAnalyticsBatch,
): boolean {
  return (
    operation.id === batch.operationId &&
    operation.projectId === batch.projectId &&
    operation.rawObjectKey === batch.rawObjectKey &&
    operation.canonicalizerVersion === batch.canonicalizerVersion &&
    operation.schemaVersion === batch.schemaVersion &&
    operation.acceptedAtNanos === batch.acceptedAt
  );
}

function assertArtifactForOperation(
  operation: {
    readonly id: string;
    readonly projectId: string;
    readonly rawObjectKey: string;
    readonly acceptedAtNanos: bigint;
    readonly canonicalizerVersion: string;
    readonly schemaVersion: number;
    readonly canonicalizationFence: bigint;
  },
  batch: CanonicalAnalyticsBatch,
): void {
  if (
    !operationMatchesBatch(operation, batch) ||
    batch.children.some(
      ({ fenceGeneration }) =>
        fenceGeneration !== operation.canonicalizationFence,
    )
  ) {
    throw new AnalyticsPersistenceError("ANALYTICS_CONFLICT", false);
  }
}

function candidatePublication(
  descriptors: readonly CanonicalCandidateDescriptor[],
) {
  return descriptors.map((descriptor) => ({
    candidateKey: descriptor.candidateKey,
    entityType: descriptor.entityType,
    entityKey: descriptor.entityKey,
    owningTraceId: descriptor.owningTraceId,
    partitionDate: partitionDate(descriptor.partitionDate),
    sourceVersion: descriptor.claim.entity.sourceVersion,
    canonicalPayloadHash: descriptor.claim.entity.canonicalPayloadHash,
    traceDeletionGeneration: descriptor.claim.traceDeletionGeneration,
    projectDeletionGeneration: descriptor.claim.projectDeletionGeneration,
  }));
}

function loadBatchIdentity(input: {
  readonly projectId: string;
  readonly operationId: string;
  readonly logicalBatchId: string;
  readonly attempt: number;
}) {
  const digest = sha256(
    [
      "langfuse-doris-load-v1",
      input.projectId,
      input.operationId,
      input.logicalBatchId,
      String(input.attempt),
    ].join("\0"),
  );
  return {
    id: `alb_${digest.slice(0, 28)}`,
    label: `lf_${digest}`,
  };
}

export class AnalyticsWriter implements AnalyticsBatchSink {
  private readonly now: () => Date;

  constructor(
    private readonly dependencies: {
      readonly client: PrismaClient;
      readonly artifactStore: CanonicalIngestionArtifactStore;
      readonly doris: DorisBatchSink;
      readonly databaseName: string;
      readonly canonicalPrefix: string;
      readonly workerId: string;
      readonly now?: () => Date;
    },
  ) {
    this.now = dependencies.now ?? (() => new Date());
  }

  async persist(
    batch: CanonicalAnalyticsBatch,
  ): Promise<AnalyticsBatchReceipt> {
    assertAnalyticsBatchBoundary(batch);
    let operation = await this.getOperation(batch);
    if (operation.terminalAt) return this.terminalReceipt(operation);

    const artifact = await this.ensureCanonicalArtifact(batch);
    operation = await this.getOperation(batch);

    if (operation.manifestState === "CANDIDATE_PUBLISHED") {
      await this.claimHeadsAndFreeze(operation, artifact);
      operation = await this.getOperation(batch);
    }
    if (operation.manifestState !== "FROZEN") {
      throw new AnalyticsPersistenceError("ANALYTICS_UNAVAILABLE", true, {
        tags: { operationId: batch.operationId, phase: "manifest" },
      });
    }

    await this.loadFrozenBatches(operation, artifact);
    const completion = await completeAnalyticsIngestionOperation({
      client: this.dependencies.client,
      operationId: batch.operationId,
      projectId: batch.projectId,
      now: this.now(),
    });
    if (
      completion.outcome === "pending" ||
      completion.status === "PARTIAL_FAILED" ||
      completion.status === "QUARANTINED" ||
      completion.status === "UNRECOVERABLE"
    ) {
      throw new AnalyticsPersistenceError(
        completion.status === "QUARANTINED"
          ? "ANALYTICS_QUARANTINED"
          : "ANALYTICS_UNAVAILABLE",
        completion.status !== "QUARANTINED",
        { tags: { operationId: batch.operationId, phase: "completion" } },
      );
    }
    return {
      operationId: batch.operationId,
      status: completion.status === "VISIBLE" ? "VISIBLE" : "PERSISTED",
    };
  }

  private async getOperation(batch: CanonicalAnalyticsBatch) {
    const operation = await findAnalyticsIngestionOperationForProject({
      client: this.dependencies.client,
      operationId: batch.operationId,
      projectId: batch.projectId,
    });
    if (!operation) {
      throw new AnalyticsPersistenceError("ANALYTICS_NOT_FOUND", false);
    }
    if (!operationMatchesBatch(operation, batch)) {
      throw new AnalyticsPersistenceError("ANALYTICS_CONFLICT", false);
    }
    return operation;
  }

  private terminalReceipt(operation: {
    readonly id: string;
    readonly status: string;
  }): AnalyticsBatchReceipt {
    if (operation.status === "VISIBLE") {
      return { operationId: operation.id, status: "VISIBLE" };
    }
    if (
      operation.status === "CANCELLED_BY_DELETION" ||
      operation.status === "COMPLETED_WITH_CANCELLATIONS"
    ) {
      return { operationId: operation.id, status: "PERSISTED" };
    }
    throw new AnalyticsPersistenceError("ANALYTICS_QUARANTINED", false, {
      tags: { operationId: operation.id },
    });
  }

  private async ensureCanonicalArtifact(
    sourceBatch: CanonicalAnalyticsBatch,
  ): Promise<CanonicalAnalyticsBatch> {
    for (let attempt = 0; attempt < 5; attempt += 1) {
      const operation = await this.getOperation(sourceBatch);
      if (operation.canonicalObjectKey && operation.canonicalArtifactChecksum) {
        const persisted = await this.dependencies.artifactStore.get(
          operation.canonicalObjectKey,
          operation.canonicalArtifactChecksum,
        );
        assertArtifactForOperation(operation, persisted);
        return persisted;
      }

      const existing = operation.reservedCanonicalObjectKey
        ? await this.dependencies.artifactStore.getIfExists(
            operation.reservedCanonicalObjectKey,
          )
        : null;
      if (existing) {
        assertArtifactForOperation(operation, existing.batch);
        const published = await publishCanonicalArtifact({
          client: this.dependencies.client,
          operationId: operation.id,
          projectId: operation.projectId,
          fence: operation.canonicalizationFence,
          leaseOwner:
            operation.canonicalizationLeaseOwner ?? this.dependencies.workerId,
          canonicalObjectKey: operation.reservedCanonicalObjectKey!,
          artifactChecksum: existing.checksum,
          candidates: candidatePublication(
            describeCanonicalCandidates(existing.batch),
          ),
        });
        if (
          published.outcome === "published" ||
          published.outcome === "already_published"
        ) {
          return existing.batch;
        }
        continue;
      }

      const nextFence = operation.canonicalizationFence + 1n;
      const key = canonicalArtifactObjectKey({
        prefix: this.dependencies.canonicalPrefix,
        projectId: operation.projectId,
        operationId: operation.id,
        fenceGeneration: nextFence,
      });
      const now = this.now();
      const reservation = await reserveCanonicalizationFence({
        client: this.dependencies.client,
        operationId: operation.id,
        projectId: operation.projectId,
        expectedFence: operation.canonicalizationFence,
        nextFence,
        leaseOwner: this.dependencies.workerId,
        leaseUntil: new Date(now.getTime() + LEASE_MS),
        now,
        reservedObjectKey: key,
        confirmedAbsentObjectKey: operation.reservedCanonicalObjectKey,
      });
      if (reservation.outcome === "leased") {
        throw new AnalyticsPersistenceError("ANALYTICS_UNAVAILABLE", true, {
          tags: { operationId: operation.id, phase: "canonicalization" },
        });
      }
      if (reservation.outcome !== "reserved") continue;

      const fencedBatch: CanonicalAnalyticsBatch = {
        ...sourceBatch,
        children: sourceBatch.children.map((claim) => ({
          ...claim,
          fenceGeneration: reservation.fence,
        })),
      };
      const stored = await this.dependencies.artifactStore.putIfAbsent(
        reservation.reservedObjectKey,
        fencedBatch,
      );
      const published = await publishCanonicalArtifact({
        client: this.dependencies.client,
        operationId: operation.id,
        projectId: operation.projectId,
        fence: reservation.fence,
        leaseOwner: this.dependencies.workerId,
        canonicalObjectKey: reservation.reservedObjectKey,
        artifactChecksum: stored.checksum,
        candidates: candidatePublication(
          describeCanonicalCandidates(fencedBatch),
        ),
      });
      if (
        published.outcome === "published" ||
        published.outcome === "already_published"
      ) {
        return fencedBatch;
      }
    }
    throw new AnalyticsPersistenceError("ANALYTICS_UNAVAILABLE", true, {
      tags: { operationId: sourceBatch.operationId, phase: "publication" },
    });
  }

  private async claimHeadsAndFreeze(
    operation: Awaited<ReturnType<AnalyticsWriter["getOperation"]>>,
    batch: CanonicalAnalyticsBatch,
  ): Promise<void> {
    const descriptors = describeCanonicalCandidates(batch);
    const dispositionResults: FrozenCandidateDisposition[] = [];
    const requiredKeys = new Set<string>();

    for (const descriptor of descriptors) {
      const projectGeneration = await getProjectDeletionGeneration({
        client: this.dependencies.client,
        projectId: operation.projectId,
      });
      const traceGeneration = descriptor.owningTraceId
        ? await getTraceDeletionGeneration({
            client: this.dependencies.client,
            projectId: operation.projectId,
            traceId: descriptor.owningTraceId,
          })
        : 0n;
      if (
        projectGeneration > descriptor.claim.projectDeletionGeneration ||
        traceGeneration > descriptor.claim.traceDeletionGeneration
      ) {
        dispositionResults.push({
          candidateKey: descriptor.candidateKey,
          disposition: "CANCELLED_BY_DELETION" as const,
          loadBatchId: null,
          reasonCode: "DELETION_BARRIER",
          quarantineExpiresAt: null,
        });
        continue;
      }

      const claim = await claimAnalyticsEntityHead({
        client: this.dependencies.client,
        projectId: operation.projectId,
        operationId: operation.id,
        entityType: descriptor.entityType,
        entityKey: descriptor.entityKey,
        lookupId: descriptor.lookupId,
        owningTraceId: descriptor.owningTraceId,
        expectedSourceVersion: descriptor.claim.expectedSourceVersion,
        sourceVersion: descriptor.claim.entity.sourceVersion,
        canonicalPayloadHash: descriptor.claim.entity.canonicalPayloadHash,
        partitionDate: partitionDate(descriptor.partitionDate),
        canonicalizerVersion: batch.canonicalizerVersion,
        fenceGeneration: operation.canonicalizationFence,
        traceDeletionGeneration: descriptor.claim.traceDeletionGeneration,
        projectDeletionGeneration: descriptor.claim.projectDeletionGeneration,
      });
      const recoveredWinner =
        claim.outcome === "noop" && claim.head.operationId === operation.id;
      if (claim.outcome === "won" || recoveredWinner) {
        requiredKeys.add(descriptor.candidateKey);
        dispositionResults.push({
          candidateKey: descriptor.candidateKey,
          disposition: "LOAD_REQUIRED" as const,
          loadBatchId: "",
          reasonCode: null,
          quarantineExpiresAt: null,
        });
      } else if (claim.outcome === "noop" || claim.outcome === "superseded") {
        dispositionResults.push({
          candidateKey: descriptor.candidateKey,
          disposition: "NOOP" as const,
          loadBatchId: null,
          reasonCode: claim.outcome.toUpperCase(),
          quarantineExpiresAt: null,
        });
      } else if (claim.outcome === "stale_fence") {
        dispositionResults.push({
          candidateKey: descriptor.candidateKey,
          disposition: "CANCELLED_BY_DELETION" as const,
          loadBatchId: null,
          reasonCode: "STALE_FENCE",
          quarantineExpiresAt: null,
        });
      } else {
        dispositionResults.push({
          candidateKey: descriptor.candidateKey,
          disposition: "QUARANTINED" as const,
          loadBatchId: null,
          reasonCode: claim.outcome.toUpperCase(),
          quarantineExpiresAt: operation.recoverableUntil,
        });
      }
    }

    const descriptorByKey = new Map(
      descriptors.map((descriptor) => [descriptor.candidateKey, descriptor]),
    );
    for (const disposition of dispositionResults) {
      if (disposition.disposition !== "LOAD_REQUIRED") continue;
      const descriptor = descriptorByKey.get(disposition.candidateKey);
      if (!descriptor) {
        throw new AnalyticsPersistenceError("ANALYTICS_CONFLICT", false);
      }
      const [projectGeneration, traceGeneration] = await Promise.all([
        getProjectDeletionGeneration({
          client: this.dependencies.client,
          projectId: operation.projectId,
        }),
        descriptor.owningTraceId
          ? getTraceDeletionGeneration({
              client: this.dependencies.client,
              projectId: operation.projectId,
              traceId: descriptor.owningTraceId,
            })
          : 0n,
      ]);
      if (
        projectGeneration <= descriptor.claim.projectDeletionGeneration &&
        traceGeneration <= descriptor.claim.traceDeletionGeneration
      ) {
        continue;
      }
      requiredKeys.delete(descriptor.candidateKey);
      disposition.disposition = "CANCELLED_BY_DELETION";
      disposition.loadBatchId = null;
      disposition.reasonCode = "DELETION_BARRIER";
    }

    const prepared = prepareDorisLoadBatches(batch, requiredKeys);
    const loadManifests = prepared.map((loadBatch) => {
      const identity = loadBatchIdentity({
        projectId: operation.projectId,
        operationId: operation.id,
        logicalBatchId: loadBatch.logicalBatchId,
        attempt: 0,
      });
      for (const disposition of dispositionResults) {
        if (loadBatch.candidateKeys.includes(disposition.candidateKey)) {
          disposition.loadBatchId = identity.id;
        }
      }
      return {
        id: identity.id,
        databaseName: this.dependencies.databaseName,
        targetTable: loadBatch.targetTable,
        logicalBatchId: loadBatch.logicalBatchId,
        attempt: 0,
        label: identity.label,
        payloadHash: loadBatch.payloadHash,
        partitionDate: partitionDate(loadBatch.partitionDate),
        candidateKeys: loadBatch.candidateKeys,
      };
    });
    const frozen = await freezeAnalyticsIngestionManifest({
      client: this.dependencies.client,
      operationId: operation.id,
      projectId: operation.projectId,
      fence: operation.canonicalizationFence,
      canonicalObjectKey: operation.canonicalObjectKey!,
      dispositions: dispositionResults,
      loadBatches: loadManifests,
    });
    if (frozen.outcome === "stale_fence") {
      throw new AnalyticsPersistenceError("ANALYTICS_UNAVAILABLE", true, {
        tags: { operationId: operation.id, phase: "manifest_fence" },
      });
    }
  }

  private async loadFrozenBatches(
    operation: Awaited<ReturnType<AnalyticsWriter["getOperation"]>>,
    batch: CanonicalAnalyticsBatch,
  ): Promise<void> {
    const requiredKeys = new Set(
      operation.candidates
        .filter(({ disposition }) => disposition === "LOAD_REQUIRED")
        .map(({ candidateKey }) => candidateKey),
    );
    const preparedByLogicalId = new Map(
      prepareDorisLoadBatches(batch, requiredKeys).map((prepared) => [
        prepared.logicalBatchId,
        prepared,
      ]),
    );

    for (const ledger of operation.loadBatches) {
      const prepared = preparedByLogicalId.get(ledger.logicalBatchId);
      if (!prepared || prepared.payloadHash !== ledger.payloadHash) {
        throw new AnalyticsPersistenceError("ANALYTICS_CONFLICT", false);
      }
      if (ledger.status === "VISIBLE") continue;
      if (
        ledger.status === "FAILED" ||
        ledger.status === "CANCELLED_BY_DELETION"
      ) {
        continue;
      }
      if (ledger.status === "UNKNOWN" || ledger.status === "LOADING") {
        await this.reconcileLoad(ledger, prepared);
        continue;
      }
      if (ledger.status !== "PENDING") {
        throw new AnalyticsPersistenceError("ANALYTICS_QUARANTINED", false);
      }

      const deletionRevalidation = await cancelAnalyticsLoadBatchIfDeleted({
        client: this.dependencies.client,
        loadBatchId: ledger.id,
        projectId: operation.projectId,
      });
      if (
        deletionRevalidation.outcome === "cancelled" ||
        deletionRevalidation.outcome === "terminal"
      ) {
        continue;
      }
      if (deletionRevalidation.outcome !== "current") {
        throw new AnalyticsPersistenceError(
          deletionRevalidation.outcome === "mixed_generation"
            ? "ANALYTICS_CONFLICT"
            : "ANALYTICS_UNAVAILABLE",
          deletionRevalidation.outcome !== "mixed_generation",
          {
            tags: {
              operationId: operation.id,
              phase: "pre_load_deletion_revalidation",
            },
          },
        );
      }

      const now = this.now();
      const claimed = await claimAnalyticsLoadBatch({
        client: this.dependencies.client,
        loadBatchId: ledger.id,
        projectId: operation.projectId,
        expectedFence: ledger.fenceGeneration,
        nextFence: ledger.fenceGeneration + 1n,
        leaseOwner: this.dependencies.workerId,
        leaseUntil: new Date(now.getTime() + LEASE_MS),
        now,
      });
      if (claimed.outcome === "already_visible") continue;
      if (claimed.outcome === "reconciliation_required") {
        await this.reconcileLoad(claimed.loadBatch, prepared);
        continue;
      }
      if (claimed.outcome !== "claimed") {
        throw new AnalyticsPersistenceError("ANALYTICS_UNAVAILABLE", true);
      }

      const claimedDeletionRevalidation =
        await cancelAnalyticsLoadBatchIfDeleted({
          client: this.dependencies.client,
          loadBatchId: ledger.id,
          projectId: operation.projectId,
          claimedFence: claimed.fence,
          leaseOwner: this.dependencies.workerId,
        });
      if (claimedDeletionRevalidation.outcome === "cancelled") continue;
      if (claimedDeletionRevalidation.outcome !== "current") {
        throw new AnalyticsPersistenceError(
          claimedDeletionRevalidation.outcome === "mixed_generation"
            ? "ANALYTICS_CONFLICT"
            : "ANALYTICS_UNAVAILABLE",
          claimedDeletionRevalidation.outcome !== "mixed_generation",
          {
            tags: {
              operationId: operation.id,
              phase: "claimed_load_deletion_revalidation",
            },
          },
        );
      }

      try {
        const result = await this.dependencies.doris.load(
          prepared,
          claimed.loadBatch.label,
        );
        if (
          result.committed &&
          result.numberFilteredRows === 0 &&
          result.numberTotalRows === prepared.rowCount
        ) {
          const recorded = await recordAnalyticsLoadOutcome({
            client: this.dependencies.client,
            loadBatchId: ledger.id,
            projectId: operation.projectId,
            fence: claimed.fence,
            leaseOwner: this.dependencies.workerId,
            outcome: "VISIBLE",
            transactionId: null,
            totalRows: result.numberTotalRows,
            filteredRows: result.numberFilteredRows,
            errorCode: null,
            now: this.now(),
          });
          this.assertLoadOutcomeRecorded(recorded);
          continue;
        }
        const unknown = result.requiresReconciliation;
        const recorded = await recordAnalyticsLoadOutcome({
          client: this.dependencies.client,
          loadBatchId: ledger.id,
          projectId: operation.projectId,
          fence: claimed.fence,
          leaseOwner: this.dependencies.workerId,
          outcome: unknown ? "UNKNOWN" : "FAILED",
          transactionId: null,
          totalRows: result.numberTotalRows,
          filteredRows: result.numberFilteredRows,
          errorCode: unknown ? "LOAD_UNKNOWN" : "LOAD_REJECTED",
          now: this.now(),
        });
        this.assertLoadOutcomeRecorded(recorded);
        if (unknown) {
          await this.reconcileLoad(
            { ...claimed.loadBatch, fenceGeneration: claimed.fence },
            prepared,
          );
          continue;
        }
        continue;
      } catch (error) {
        if (error instanceof AnalyticsPersistenceError) throw error;
        const permanent = error instanceof DorisError && !error.retryable;
        const recorded = await recordAnalyticsLoadOutcome({
          client: this.dependencies.client,
          loadBatchId: ledger.id,
          projectId: operation.projectId,
          fence: claimed.fence,
          leaseOwner: this.dependencies.workerId,
          outcome: permanent ? "FAILED" : "UNKNOWN",
          transactionId: null,
          totalRows: null,
          filteredRows: null,
          errorCode:
            error instanceof DorisError ? error.code : "ANALYTICS_UNAVAILABLE",
          now: this.now(),
        });
        this.assertLoadOutcomeRecorded(recorded);
        if (permanent) continue;
        throw new AnalyticsPersistenceError("ANALYTICS_UNAVAILABLE", true);
      }
    }
  }

  private async reconcileLoad(
    ledger: {
      readonly id: string;
      readonly projectId: string;
      readonly label: string;
      readonly fenceGeneration: bigint;
      readonly totalRows: number | null;
      readonly filteredRows: number | null;
    },
    prepared: PreparedDorisLoadBatch,
  ): Promise<void> {
    const reconciliation = await this.dependencies.doris.reconcile(
      ledger.label,
    );
    if (!RECONCILIATION_STATES.has(reconciliation.status)) {
      throw new AnalyticsPersistenceError("ANALYTICS_UNAVAILABLE", true, {
        tags: {
          projectId: ledger.projectId,
          phase: "load_reconciliation_status",
        },
      });
    }
    const recorded = await recordAnalyticsLoadReconciliation({
      client: this.dependencies.client,
      loadBatchId: ledger.id,
      projectId: ledger.projectId,
      fence: ledger.fenceGeneration,
      status: reconciliation.status as
        | "UNKNOWN"
        | "PREPARE"
        | "COMMITTED"
        | "VISIBLE"
        | "ABORTED",
      transactionId: null,
      totalRows: ledger.totalRows ?? prepared.rowCount,
      filteredRows: ledger.filteredRows ?? 0,
      now: this.now(),
    });
    this.assertLoadOutcomeRecorded(recorded);
    if (reconciliation.status === "ABORTED") return;
    if (!reconciliation.visible) {
      throw new AnalyticsPersistenceError("ANALYTICS_UNAVAILABLE", true);
    }
  }

  private assertLoadOutcomeRecorded(recorded: boolean): void {
    if (!recorded) {
      throw new AnalyticsPersistenceError("ANALYTICS_UNAVAILABLE", true, {
        tags: { phase: "load_fence", reasonCode: "STALE_LOAD_FENCE" },
      });
    }
  }
}
