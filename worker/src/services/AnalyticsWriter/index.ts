import type { PrismaClient } from "@prisma/client";
import {
  AnalyticsPersistenceError,
  assertAnalyticsBatchBoundary,
  type AnalyticsBatchReceipt,
  type AnalyticsBatchSink,
  type CanonicalAnalyticsBatch,
  type CanonicalAnalyticsEntity,
} from "@langfuse/shared/analytics-persistence";
import {
  acquireAnalyticsMutationPermit,
  analyticsLoadBatchIdentity,
  claimAnalyticsEntityHead,
  claimAnalyticsLoadBatch,
  cancelAnalyticsLoadBatchIfDeleted,
  cancelAnalyticsLoadBatchIfRetained,
  completeAnalyticsIngestionOperation,
  findAnalyticsIngestionOperationForProject,
  freezeAnalyticsIngestionManifest,
  getProjectDeletionGeneration,
  getDatasetDeletionGeneration,
  getDatasetRunDeletionGeneration,
  getAnalyticsRetentionBarrier,
  getTraceDeletionGeneration,
  initializeTraceControlState,
  publishCanonicalArtifact,
  recordAnalyticsLoadOutcome,
  recordAnalyticsLoadReconciliation,
  reserveCanonicalizationFence,
  DorisError,
  type AnalyticsEvaluationDispatchTargetInput,
  type AnalyticsIntegrationDeliveryTargetInput,
  type AnalyticsRuntimeAdmissionContext,
} from "@langfuse/shared/src/server";

import {
  CanonicalArtifactIntegrityError,
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

async function currentDeletionGenerations(input: {
  readonly client: PrismaClient;
  readonly projectId: string;
  readonly descriptor: CanonicalCandidateDescriptor;
}): Promise<{
  readonly project: bigint;
  readonly trace: bigint;
  readonly dataset: bigint;
  readonly run: bigint;
}> {
  const [project, trace, dataset, run] = await Promise.all([
    getProjectDeletionGeneration({
      client: input.client,
      projectId: input.projectId,
    }),
    input.descriptor.owningTraceId
      ? getTraceDeletionGeneration({
          client: input.client,
          projectId: input.projectId,
          traceId: input.descriptor.owningTraceId,
        })
      : 0n,
    input.descriptor.owningDatasetId
      ? getDatasetDeletionGeneration({
          client: input.client,
          projectId: input.projectId,
          datasetId: input.descriptor.owningDatasetId,
        })
      : 0n,
    input.descriptor.owningDatasetRunId
      ? getDatasetRunDeletionGeneration({
          client: input.client,
          projectId: input.projectId,
          datasetRunId: input.descriptor.owningDatasetRunId,
        })
      : 0n,
  ]);
  return { project, trace, dataset, run };
}

function hasDeletionBarrier(
  generations: Awaited<ReturnType<typeof currentDeletionGenerations>>,
): boolean {
  return Object.values(generations).some((generation) => generation !== 0n);
}

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
    owningDatasetId: descriptor.owningDatasetId,
    owningDatasetRunId: descriptor.owningDatasetRunId,
    partitionDate: partitionDate(descriptor.partitionDate),
    sourceVersion: descriptor.claim.entity.sourceVersion,
    canonicalPayloadHash: descriptor.claim.entity.canonicalPayloadHash,
    traceDeletionGeneration: descriptor.claim.traceDeletionGeneration,
    projectDeletionGeneration: descriptor.claim.projectDeletionGeneration,
    datasetDeletionGeneration: descriptor.claim.datasetDeletionGeneration ?? 0n,
    runDeletionGeneration: descriptor.claim.runDeletionGeneration ?? 0n,
  }));
}

function nanosDate(value: bigint): Date {
  const milliseconds = value / 1_000_000n;
  if (
    milliseconds < BigInt(Number.MIN_SAFE_INTEGER) ||
    milliseconds > BigInt(Number.MAX_SAFE_INTEGER)
  ) {
    throw new AnalyticsPersistenceError("ANALYTICS_VALIDATION_ERROR", false);
  }
  const date = new Date(Number(milliseconds));
  if (!Number.isFinite(date.getTime())) {
    throw new AnalyticsPersistenceError("ANALYTICS_VALIDATION_ERROR", false);
  }
  return date;
}

export function analyticsEvaluationTargetsFromCanonicalBatch(
  batch: CanonicalAnalyticsBatch,
): readonly AnalyticsEvaluationDispatchTargetInput[] {
  const targets: AnalyticsEvaluationDispatchTargetInput[] = [];
  for (const descriptor of describeCanonicalCandidates(batch)) {
    const entity: CanonicalAnalyticsEntity = descriptor.claim.entity;
    if (entity.kind === "event") {
      const targetTimestamp = nanosDate(entity.startTime);
      targets.push(
        {
          candidateKey: descriptor.candidateKey,
          targetType: "TRACE_UPSERT",
          targetId: entity.traceId,
          traceId: entity.traceId,
          observationId: null,
          datasetItemId: null,
          targetTimestamp,
          traceEnvironment: entity.environment,
        },
        {
          candidateKey: descriptor.candidateKey,
          targetType: "OBSERVATION_UPSERT",
          targetId: entity.spanId,
          traceId: entity.traceId,
          observationId: entity.spanId,
          datasetItemId: null,
          targetTimestamp,
          traceEnvironment: entity.environment,
        },
      );
    } else if (entity.kind === "datasetRunItem") {
      targets.push({
        candidateKey: descriptor.candidateKey,
        targetType: "DATASET_RUN_ITEM_UPSERT",
        targetId: entity.runItemId,
        traceId: entity.traceId,
        observationId: entity.observationId,
        datasetItemId: entity.datasetItemId,
        datasetItemValidFrom:
          entity.datasetItemVersion === null
            ? null
            : nanosDate(entity.datasetItemVersion),
        targetTimestamp: nanosDate(entity.createdAt),
        traceEnvironment: null,
      });
    }
  }
  return targets;
}

const MAX_INTEGRATION_DELIVERY_ESTIMATED_BYTES = 16 * 1024 * 1024;

function analyticsIntegrationEntityEstimatedBytes(
  entity: CanonicalAnalyticsEntity,
): number {
  const serialized = JSON.stringify(entity, (_key, value: unknown) =>
    typeof value === "bigint" ? value.toString() : value,
  );
  return Math.min(
    Buffer.byteLength(serialized, "utf8"),
    MAX_INTEGRATION_DELIVERY_ESTIMATED_BYTES,
  );
}

export function analyticsIntegrationTargetsFromCanonicalBatch(
  batch: CanonicalAnalyticsBatch,
): readonly AnalyticsIntegrationDeliveryTargetInput[] {
  const targets: AnalyticsIntegrationDeliveryTargetInput[] = [];
  for (const descriptor of describeCanonicalCandidates(batch)) {
    const entity = descriptor.claim.entity;
    const estimatedBytes = analyticsIntegrationEntityEstimatedBytes(entity);
    if (entity.kind === "event") {
      targets.push(
        {
          candidateKey: descriptor.candidateKey,
          deliveryKind: "TRACE",
          entityKey: entity.traceId,
          estimatedBytes,
        },
        {
          candidateKey: descriptor.candidateKey,
          deliveryKind: "OBSERVATION",
          entityKey: entity.spanId,
          estimatedBytes,
        },
      );
      if (entity.type === "GENERATION") {
        targets.push({
          candidateKey: descriptor.candidateKey,
          deliveryKind: "GENERATION",
          entityKey: entity.spanId,
          estimatedBytes,
        });
      }
    } else if (entity.kind === "score") {
      targets.push({
        candidateKey: descriptor.candidateKey,
        deliveryKind: "SCORE",
        entityKey: entity.scoreId,
        estimatedBytes,
      });
    }
  }
  return targets;
}

function controlStateRepresentative(
  left: CanonicalCandidateDescriptor,
  right: CanonicalCandidateDescriptor,
): CanonicalCandidateDescriptor {
  const leftEvent = left.claim.entity;
  const rightEvent = right.claim.entity;
  if (leftEvent.kind !== "event" || rightEvent.kind !== "event") return left;
  if (leftEvent.isAppRoot !== rightEvent.isAppRoot) {
    return leftEvent.isAppRoot ? left : right;
  }
  const leftIsRoot =
    leftEvent.parentSpanId === null || leftEvent.parentSpanId === "";
  const rightIsRoot =
    rightEvent.parentSpanId === null || rightEvent.parentSpanId === "";
  if (leftIsRoot !== rightIsRoot) return leftIsRoot ? left : right;
  if (leftEvent.startTime !== rightEvent.startTime) {
    return leftEvent.startTime < rightEvent.startTime ? left : right;
  }
  return leftEvent.spanId <= rightEvent.spanId ? left : right;
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
      readonly getAdmissionContext?: () => AnalyticsRuntimeAdmissionContext | null;
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
    if (operation.terminalAt) return this.terminalReceipt(operation);

    if (operation.manifestState === "CANDIDATE_PUBLISHED") {
      await this.claimHeadsAndFreeze(operation, artifact);
      operation = await this.getOperation(batch);
      if (operation.terminalAt) return this.terminalReceipt(operation);
    }
    if (operation.manifestState !== "FROZEN") {
      throw new AnalyticsPersistenceError("ANALYTICS_UNAVAILABLE", true, {
        tags: { operationId: batch.operationId, phase: "manifest" },
      });
    }

    await this.loadFrozenBatches(operation, artifact);
    const admissionContext = this.dependencies.getAdmissionContext?.() ?? null;
    const completion = await completeAnalyticsIngestionOperation({
      client: this.dependencies.client,
      operationId: batch.operationId,
      projectId: batch.projectId,
      now: this.now(),
      ...(admissionContext
        ? {
            evaluationCapture: {
              admissionContext,
              targets: analyticsEvaluationTargetsFromCanonicalBatch(artifact),
            },
            integrationCapture: {
              admissionContext,
              targets: analyticsIntegrationTargetsFromCanonicalBatch(artifact),
            },
          }
        : {}),
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

  async reconcileUnresolvedOperation(input: {
    readonly operationId: string;
    readonly projectId: string;
  }): Promise<boolean> {
    const operation = await findAnalyticsIngestionOperationForProject({
      client: this.dependencies.client,
      operationId: input.operationId,
      projectId: input.projectId,
    });
    if (!operation) {
      throw new AnalyticsPersistenceError("ANALYTICS_NOT_FOUND", false);
    }
    if (operation.terminalAt) return true;
    const unresolved = operation.loadBatches.filter(
      ({ status }) => status === "UNKNOWN" || status === "LOADING",
    );
    if (unresolved.length === 0) return false;

    let aborted = false;
    let pending = false;
    for (const ledger of unresolved) {
      let reconciliation;
      try {
        reconciliation = await this.dependencies.doris.reconcile(ledger.label);
      } catch {
        throw new AnalyticsPersistenceError("ANALYTICS_UNAVAILABLE", true, {
          tags: {
            operationId: operation.id,
            phase: "load_reconciliation",
          },
        });
      }
      if (!RECONCILIATION_STATES.has(reconciliation.status)) {
        throw new AnalyticsPersistenceError("ANALYTICS_UNAVAILABLE", true, {
          tags: {
            operationId: operation.id,
            phase: "load_reconciliation_status",
          },
        });
      }
      const candidateRows = operation.candidates.filter(
        ({ loadBatchId }) => loadBatchId === ledger.id,
      ).length;
      const recorded = await recordAnalyticsLoadReconciliation({
        client: this.dependencies.client,
        loadBatchId: ledger.id,
        projectId: operation.projectId,
        fence: ledger.fenceGeneration,
        status: reconciliation.status as
          | "UNKNOWN"
          | "PREPARE"
          | "COMMITTED"
          | "VISIBLE"
          | "ABORTED",
        transactionId: null,
        totalRows: ledger.totalRows ?? candidateRows,
        filteredRows: ledger.filteredRows ?? 0,
        now: this.now(),
      });
      this.assertLoadOutcomeRecorded(recorded);
      aborted ||= reconciliation.status === "ABORTED";
      pending ||=
        reconciliation.status !== "ABORTED" && !reconciliation.visible;
    }
    if (pending) {
      throw new AnalyticsPersistenceError("ANALYTICS_UNAVAILABLE", true, {
        tags: {
          operationId: operation.id,
          phase: "load_reconciliation_pending",
        },
      });
    }
    if (aborted) return false;

    const canonicalArtifact =
      operation.canonicalObjectKey && operation.canonicalArtifactChecksum
        ? await this.dependencies.artifactStore.get(
            operation.canonicalObjectKey,
            operation.canonicalArtifactChecksum,
          )
        : null;
    if (canonicalArtifact) {
      assertArtifactForOperation(operation, canonicalArtifact);
    }
    const admissionContext = this.dependencies.getAdmissionContext?.() ?? null;
    const completion = await completeAnalyticsIngestionOperation({
      client: this.dependencies.client,
      operationId: operation.id,
      projectId: operation.projectId,
      now: this.now(),
      ...(admissionContext && canonicalArtifact
        ? {
            evaluationCapture: {
              admissionContext,
              targets:
                analyticsEvaluationTargetsFromCanonicalBatch(canonicalArtifact),
            },
            integrationCapture: {
              admissionContext,
              targets:
                analyticsIntegrationTargetsFromCanonicalBatch(
                  canonicalArtifact,
                ),
            },
          }
        : {}),
    });
    if (completion.outcome === "pending") {
      throw new AnalyticsPersistenceError("ANALYTICS_UNAVAILABLE", true, {
        tags: { operationId: operation.id, phase: "completion" },
      });
    }
    return true;
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
    try {
      for (let attempt = 0; attempt < 5; attempt += 1) {
        const operation = await this.getOperation(sourceBatch);
        if (
          operation.canonicalObjectKey &&
          operation.canonicalArtifactChecksum
        ) {
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
              operation.canonicalizationLeaseOwner ??
              this.dependencies.workerId,
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
    } catch (error) {
      if (error instanceof AnalyticsPersistenceError) throw error;
      if (error instanceof CanonicalArtifactIntegrityError) {
        throw new AnalyticsPersistenceError(
          error.reasonCode === "ARTIFACT_UNAVAILABLE"
            ? "ANALYTICS_UNRECOVERABLE"
            : "ANALYTICS_QUARANTINED",
          false,
          {
            tags: {
              operationId: sourceBatch.operationId,
              phase: "canonical_artifact",
              reasonCode: error.reasonCode,
            },
          },
        );
      }
      throw new AnalyticsPersistenceError("ANALYTICS_UNAVAILABLE", true, {
        tags: {
          operationId: sourceBatch.operationId,
          phase: "canonical_artifact_storage",
        },
      });
    }
  }

  private async claimHeadsAndFreeze(
    operation: Awaited<ReturnType<AnalyticsWriter["getOperation"]>>,
    batch: CanonicalAnalyticsBatch,
  ): Promise<void> {
    const descriptors = describeCanonicalCandidates(batch);
    const dispositionResults: FrozenCandidateDisposition[] = [];
    const requiredKeys = new Set<string>();
    const retentionBarrier = await getAnalyticsRetentionBarrier({
      client: this.dependencies.client,
      projectId: operation.projectId,
    });

    for (const descriptor of descriptors) {
      if (
        retentionBarrier &&
        partitionDate(descriptor.partitionDate) < retentionBarrier
      ) {
        dispositionResults.push({
          candidateKey: descriptor.candidateKey,
          disposition: "CANCELLED_BY_DELETION" as const,
          loadBatchId: null,
          reasonCode: "RETENTION_BARRIER",
          quarantineExpiresAt: null,
        });
        continue;
      }
      const deletionGenerations = await currentDeletionGenerations({
        client: this.dependencies.client,
        projectId: operation.projectId,
        descriptor,
      });
      if (hasDeletionBarrier(deletionGenerations)) {
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
        owningDatasetId: descriptor.owningDatasetId,
        owningDatasetRunId: descriptor.owningDatasetRunId,
        expectedSourceVersion: descriptor.claim.expectedSourceVersion,
        sourceVersion: descriptor.claim.entity.sourceVersion,
        canonicalPayloadHash: descriptor.claim.entity.canonicalPayloadHash,
        partitionDate: partitionDate(descriptor.partitionDate),
        canonicalizerVersion: batch.canonicalizerVersion,
        fenceGeneration: operation.canonicalizationFence,
        traceDeletionGeneration: descriptor.claim.traceDeletionGeneration,
        projectDeletionGeneration: descriptor.claim.projectDeletionGeneration,
        datasetDeletionGeneration:
          descriptor.claim.datasetDeletionGeneration ?? 0n,
        runDeletionGeneration: descriptor.claim.runDeletionGeneration ?? 0n,
      });
      // An identical head only proves that another operation won the CAS; it
      // does not prove that winner's Doris load is already visible. Re-loading
      // the identical canonical row is idempotent and closes that race.
      if (claim.outcome === "won" || claim.outcome === "noop") {
        requiredKeys.add(descriptor.candidateKey);
        dispositionResults.push({
          candidateKey: descriptor.candidateKey,
          disposition: "LOAD_REQUIRED" as const,
          loadBatchId: "",
          reasonCode: null,
          quarantineExpiresAt: null,
        });
      } else if (claim.outcome === "superseded") {
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
      const deletionGenerations = await currentDeletionGenerations({
        client: this.dependencies.client,
        projectId: operation.projectId,
        descriptor,
      });
      if (!hasDeletionBarrier(deletionGenerations)) {
        continue;
      }
      requiredKeys.delete(descriptor.candidateKey);
      disposition.disposition = "CANCELLED_BY_DELETION";
      disposition.loadBatchId = null;
      disposition.reasonCode = "DELETION_BARRIER";
    }

    const traceControls = new Map<string, CanonicalCandidateDescriptor>();
    for (const descriptor of descriptors) {
      if (
        !requiredKeys.has(descriptor.candidateKey) ||
        descriptor.claim.entity.kind !== "event"
      ) {
        continue;
      }
      const current = traceControls.get(descriptor.claim.entity.traceId);
      traceControls.set(
        descriptor.claim.entity.traceId,
        current ? controlStateRepresentative(current, descriptor) : descriptor,
      );
    }
    await Promise.all(
      [...traceControls.values()].map((descriptor) => {
        const entity = descriptor.claim.entity;
        if (entity.kind !== "event") return Promise.resolve();
        return initializeTraceControlState({
          client: this.dependencies.client,
          projectId: operation.projectId,
          traceId: entity.traceId,
          initializedByOperationId: operation.id,
          bookmarked: entity.bookmarked,
          public: entity.public,
        });
      }),
    );

    const prepared = prepareDorisLoadBatches(batch, requiredKeys);
    const loadManifests = prepared.map((loadBatch) => {
      const identity = analyticsLoadBatchIdentity({
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

      const retentionRevalidation = await cancelAnalyticsLoadBatchIfRetained({
        client: this.dependencies.client,
        loadBatchId: ledger.id,
        projectId: operation.projectId,
      });
      if (
        retentionRevalidation.outcome === "cancelled" ||
        retentionRevalidation.outcome === "terminal"
      ) {
        continue;
      }
      if (retentionRevalidation.outcome !== "current") {
        throw new AnalyticsPersistenceError("ANALYTICS_UNAVAILABLE", true, {
          tags: {
            operationId: operation.id,
            phase: "pre_load_retention_revalidation",
          },
        });
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

      const mutationPermit = await acquireAnalyticsMutationPermit({
        client: this.dependencies.client,
        mutation: {
          kind: "ingestion",
          checkpointGeneration: operation.checkpointGeneration,
          operationAcceptedAtNanos: operation.acceptedAtNanos,
        },
        now: this.now(),
      });
      if (mutationPermit.outcome === "held") {
        throw new AnalyticsPersistenceError("ANALYTICS_UNAVAILABLE", true, {
          tags: {
            operationId: operation.id,
            phase: "checkpoint_fence",
            reasonCode: "CHECKPOINT_FENCE",
          },
        });
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

      const claimedRetentionRevalidation =
        await cancelAnalyticsLoadBatchIfRetained({
          client: this.dependencies.client,
          loadBatchId: ledger.id,
          projectId: operation.projectId,
          claimedFence: claimed.fence,
          leaseOwner: this.dependencies.workerId,
        });
      if (claimedRetentionRevalidation.outcome === "cancelled") continue;
      if (claimedRetentionRevalidation.outcome !== "current") {
        throw new AnalyticsPersistenceError("ANALYTICS_UNAVAILABLE", true, {
          tags: {
            operationId: operation.id,
            phase: "claimed_load_retention_revalidation",
          },
        });
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
