import type { AnalyticsIngestionOperation, PrismaClient } from "@prisma/client";
import {
  UnrecoverableError,
  type Job,
  type JobsOptions,
  type Processor,
} from "bullmq";
import {
  AnalyticsIngestionQueue,
  AnalyticsIngestionQueueEventSchema,
  AnalyticsPersistenceError,
  analyticsDurableProvenanceFromRecord,
  analyticsDurableProvenanceMatches,
  claimAnalyticsIngestionOutbox,
  createAnalyticsBackendClaimLease,
  deserializeAnalyticsDurableProvenance,
  findAnalyticsIngestionOperationForProject,
  lockAnalyticsBackendClaimLeaseForIo,
  lockLegacyAnalyticsAdmission,
  markAnalyticsIngestionOutboxPublished,
  markAnalyticsIngestionTerminalFailure,
  releaseAnalyticsBackendClaimLease,
  resolveAnalyticsIngestionAttemptFailure,
  serializeAnalyticsDurableProvenance,
  logger,
  QueueJobs,
  QueueName,
  recordIncrement,
  type AnalyticsBatchSink,
  type AnalyticsBackend,
  type CanonicalAnalyticsBatch,
  type AnalyticsDurableProvenance,
  type AnalyticsRuntimeAdmissionContext,
  type TQueueJobTypes,
} from "@langfuse/shared/src/server";
import { prisma } from "@langfuse/shared/src/db";

interface AnalyticsIngestionQueueProducer {
  add(
    name: QueueJobs.AnalyticsIngestionJob,
    data: TQueueJobTypes[QueueName.AnalyticsIngestionQueue],
    options: JobsOptions,
  ): Promise<{
    getState(): Promise<string>;
    retry(state: "failed"): Promise<void>;
  }>;
}

type AnalyticsIngestionProvenanceRecord = Pick<
  AnalyticsIngestionOperation,
  | "id"
  | "analyticsBackend"
  | "deploymentGeneration"
  | "workloadEpochFingerprint"
  | "runtimeContractVersion"
  | "producerRuntimeLeaseId"
  | "capability"
  | "capabilityActivationGeneration"
  | "capabilityContractVersion"
  | "canonicalizerVersion"
  | "schemaVersion"
>;

const ANALYTICS_INGESTION_CLAIM_MS = 30 * 60_000;
const ANALYTICS_INGESTION_FENCE_TIMEOUT_MS = 35 * 60_000;

function operationProvenance(
  operation: AnalyticsIngestionProvenanceRecord,
): AnalyticsDurableProvenance | null {
  return analyticsDurableProvenanceFromRecord({
    analyticsBackend: operation.analyticsBackend ?? null,
    deploymentGeneration: operation.deploymentGeneration ?? null,
    workloadEpochFingerprint: operation.workloadEpochFingerprint ?? null,
    runtimeContractVersion: operation.runtimeContractVersion ?? null,
    producerRuntimeLeaseId: operation.producerRuntimeLeaseId ?? null,
    capability: operation.capability ?? null,
    capabilityActivationGeneration:
      operation.capabilityActivationGeneration ?? null,
    capabilityContractVersion: operation.capabilityContractVersion ?? null,
  });
}

function assertQueueProvenanceMatchesOperation(input: {
  readonly operation: AnalyticsIngestionProvenanceRecord;
  readonly serializedProvenance: unknown;
}): void {
  const authoritative = operationProvenance(input.operation);
  const conflict = () =>
    new AnalyticsPersistenceError("ANALYTICS_CONFLICT", false, {
      tags: {
        operationId: input.operation.id,
        phase: "queue_provenance",
      },
    });
  let delivered: AnalyticsDurableProvenance | null;
  try {
    delivered =
      input.serializedProvenance === undefined
        ? null
        : deserializeAnalyticsDurableProvenance(input.serializedProvenance);
  } catch {
    throw conflict();
  }
  if (
    (authoritative === null) !== (delivered === null) ||
    (authoritative &&
      delivered &&
      !analyticsDurableProvenanceMatches(authoritative, delivered))
  ) {
    throw conflict();
  }
}

async function withAnalyticsIngestionWorkFence(input: {
  readonly client: PrismaClient;
  readonly operation: AnalyticsIngestionProvenanceRecord;
  readonly admissionContext: AnalyticsRuntimeAdmissionContext | null;
  readonly claimKind:
    | "analytics-ingestion-publish"
    | "analytics-ingestion-process";
  readonly run: () => Promise<void>;
}): Promise<void> {
  const provenance = operationProvenance(input.operation);
  if (!provenance) {
    await input.client.$transaction(
      async (transaction) => {
        await lockLegacyAnalyticsAdmission(transaction);
        await input.run();
      },
      { timeout: ANALYTICS_INGESTION_FENCE_TIMEOUT_MS },
    );
    return;
  }

  const expectedBackend: AnalyticsBackend =
    provenance.analyticsBackend === "DORIS" ? "doris" : "clickhouse";
  if (
    !input.admissionContext ||
    input.admissionContext.backend !== expectedBackend ||
    input.admissionContext.deploymentGeneration !==
      provenance.deploymentGeneration
  ) {
    throw new Error("Analytics ingestion runtime is not admitted");
  }
  const fence = {
    runtimeLeaseId: input.admissionContext.runtimeLeaseId,
    expectedBackend,
    expectedDeploymentGeneration: provenance.deploymentGeneration,
    expectedWorkloadEpochFingerprint: provenance.workloadEpochFingerprint,
    expectedRuntimeContractVersion: provenance.runtimeContractVersion,
    requiredContract: {
      canonicalizerVersion: input.operation.canonicalizerVersion,
      schemaVersion: input.operation.schemaVersion,
    },
    ...(provenance.capability
      ? {
          capability: provenance.capability,
          expectedCapabilityActivationGeneration:
            provenance.capabilityActivationGeneration,
          expectedCapabilityContractVersion:
            provenance.capabilityContractVersion,
          action:
            input.claimKind === "analytics-ingestion-publish"
              ? ("recovery" as const)
              : ("claimExisting" as const),
        }
      : { action: "foundation" as const }),
  };
  const claim = await createAnalyticsBackendClaimLease({
    client: input.client,
    ...fence,
    claimKind: input.claimKind,
    resourceIdentity: input.operation.id,
    leaseMs: ANALYTICS_INGESTION_CLAIM_MS,
  });
  if (!claim) {
    throw new AnalyticsPersistenceError("ANALYTICS_UNAVAILABLE", true, {
      tags: {
        operationId: input.operation.id,
        phase: "analytics_claim",
      },
    });
  }
  try {
    await input.client.$transaction(
      async (transaction) => {
        await lockAnalyticsBackendClaimLeaseForIo({
          transaction,
          claimLeaseId: claim.id,
          fence,
        });
        await input.run();
      },
      { timeout: ANALYTICS_INGESTION_FENCE_TIMEOUT_MS },
    );
  } finally {
    await releaseAnalyticsBackendClaimLease({
      client: input.client,
      claimLeaseId: claim.id,
      runtimeLeaseId: input.admissionContext.runtimeLeaseId,
    });
  }
}

function recordAttemptResolution(
  resolution: "requeued" | "terminalized" | "unchanged",
): void {
  recordIncrement("langfuse.analytics.ingestion.queue", 1, {
    status:
      resolution === "requeued"
        ? "outbox_requeued"
        : resolution === "terminalized"
          ? "terminalized"
          : "stale_delivery",
  });
}

export async function publishAnalyticsIngestionOutboxBatch(input: {
  readonly client?: PrismaClient;
  readonly queue?: AnalyticsIngestionQueueProducer;
  readonly workerId: string;
  readonly now?: Date;
  readonly limit?: number;
  readonly lockMs?: number;
  readonly claimOutbox?: typeof claimAnalyticsIngestionOutbox;
  readonly markPublished?: typeof markAnalyticsIngestionOutboxPublished;
  readonly getAdmissionContext?: () => AnalyticsRuntimeAdmissionContext | null;
  readonly withPublicationFence?: (
    operation: AnalyticsIngestionProvenanceRecord,
    run: () => Promise<void>,
  ) => Promise<void>;
}): Promise<number> {
  const queue = input.queue ?? AnalyticsIngestionQueue.getInstance();
  if (!queue) {
    throw new AnalyticsPersistenceError("ANALYTICS_UNAVAILABLE", true, {
      tags: { phase: "outbox_queue" },
    });
  }
  const client = input.client ?? prisma;
  const now = input.now ?? new Date();
  const claimOutbox = input.claimOutbox ?? claimAnalyticsIngestionOutbox;
  const markPublished =
    input.markPublished ?? markAnalyticsIngestionOutboxPublished;
  const claimed = await claimOutbox({
    client,
    workerId: input.workerId,
    now,
    lockedUntil: new Date(now.getTime() + (input.lockMs ?? 60_000)),
    limit: input.limit ?? 100,
  });

  let published = 0;
  for (const outbox of claimed) {
    const provenance = operationProvenance(outbox.operation);
    const withPublicationFence =
      input.withPublicationFence ??
      ((operation, run) =>
        withAnalyticsIngestionWorkFence({
          client,
          operation,
          admissionContext: input.getAdmissionContext?.() ?? null,
          claimKind: "analytics-ingestion-publish",
          run,
        }));
    await withPublicationFence(outbox.operation, async () => {
      const delivery = await queue.add(
        QueueJobs.AnalyticsIngestionJob,
        {
          timestamp: now,
          id: outbox.operationId,
          payload: {
            operationId: outbox.operationId,
            projectId: outbox.operation.projectId,
            generation: outbox.generation,
            ...(provenance
              ? {
                  analyticsProvenance:
                    serializeAnalyticsDurableProvenance(provenance),
                }
              : {}),
          },
          name: QueueJobs.AnalyticsIngestionJob,
        },
        {
          jobId: `${outbox.operationId}-g${outbox.generation}`,
          attempts: 1,
        },
      );
      if ((await delivery.getState()) === "failed") {
        await delivery.retry("failed");
      }
      const marked = await markPublished({
        client,
        operationId: outbox.operationId,
        generation: outbox.generation,
        workerId: input.workerId,
        now,
      });
      if (!marked) {
        throw new AnalyticsPersistenceError("ANALYTICS_UNAVAILABLE", true, {
          tags: {
            operationId: outbox.operationId,
            phase: "outbox_publish_fence",
          },
        });
      }
    });
    published += 1;
    recordIncrement("langfuse.analytics.ingestion.outbox", 1, {
      status: "published",
    });
  }
  return published;
}

type CanonicalizationOperation = Pick<
  AnalyticsIngestionOperation,
  | "id"
  | "projectId"
  | "sourceOperationId"
  | "sourceChecksum"
  | "rawObjectKey"
  | "acceptedAtNanos"
  | "canonicalizerVersion"
  | "schemaVersion"
>;

export function analyticsIngestionQueueProcessorBuilder(input: {
  readonly sink: AnalyticsBatchSink;
  readonly canonicalize: (
    operation: CanonicalizationOperation,
  ) => Promise<CanonicalAnalyticsBatch>;
  readonly client?: PrismaClient;
  readonly findOperation?: typeof findAnalyticsIngestionOperationForProject;
  readonly markTerminalFailure?: typeof markAnalyticsIngestionTerminalFailure;
  readonly resolveAttemptFailure?: typeof resolveAnalyticsIngestionAttemptFailure;
  readonly reconcileUnresolved?: (input: {
    readonly operationId: string;
    readonly projectId: string;
  }) => Promise<boolean>;
  readonly assertReady?: () => Promise<void>;
  readonly withOperationLock?: (
    operation: CanonicalizationOperation,
    run: () => Promise<void>,
  ) => Promise<void>;
  readonly getAdmissionContext?: () => AnalyticsRuntimeAdmissionContext | null;
  readonly withAnalyticsWorkFence?: (
    operation: AnalyticsIngestionProvenanceRecord,
    run: () => Promise<void>,
  ) => Promise<void>;
}): Processor<TQueueJobTypes[QueueName.AnalyticsIngestionQueue]> {
  return async (
    job: Job<TQueueJobTypes[QueueName.AnalyticsIngestionQueue]>,
  ): Promise<void> => {
    const payload = AnalyticsIngestionQueueEventSchema.parse(job.data.payload);
    const operation = await (
      input.findOperation ?? findAnalyticsIngestionOperationForProject
    )({
      client: input.client ?? prisma,
      operationId: payload.operationId,
      projectId: payload.projectId,
    });
    if (!operation) {
      throw new AnalyticsPersistenceError("ANALYTICS_NOT_FOUND", false);
    }
    const generation = payload.generation;
    if (operation.outboxV2?.generation !== generation) return;
    if (operation.terminalAt) {
      if (
        operation.status === "VISIBLE" ||
        operation.status === "CANCELLED_BY_DELETION" ||
        operation.status === "COMPLETED_WITH_CANCELLATIONS"
      ) {
        return;
      }
      const terminalError = new AnalyticsPersistenceError(
        operation.status === "UNRECOVERABLE"
          ? "ANALYTICS_UNRECOVERABLE"
          : "ANALYTICS_QUARANTINED",
        false,
        { tags: { operationId: operation.id, phase: "queue_terminal" } },
      );
      throw new UnrecoverableError(terminalError.message);
    }

    try {
      assertQueueProvenanceMatchesOperation({
        operation,
        serializedProvenance: payload.analyticsProvenance,
      });
      const run = async () => {
        await input.assertReady?.();
        if (
          await input.reconcileUnresolved?.({
            operationId: operation.id,
            projectId: operation.projectId,
          })
        ) {
          recordIncrement("langfuse.analytics.ingestion.queue", 1, {
            status: "terminal",
          });
          return;
        }
        const batch = await input.canonicalize(operation);
        await input.sink.persist(batch);
        recordIncrement("langfuse.analytics.ingestion.queue", 1, {
          status: "terminal",
        });
      };
      const withFence =
        input.withAnalyticsWorkFence ??
        ((fencedOperation, fencedRun) =>
          withAnalyticsIngestionWorkFence({
            client: input.client ?? prisma,
            operation: fencedOperation,
            admissionContext: input.getAdmissionContext?.() ?? null,
            claimKind: "analytics-ingestion-process",
            run: fencedRun,
          }));
      await withFence(operation, async () => {
        if (input.withOperationLock) {
          await input.withOperationLock(operation, run);
        } else {
          await run();
        }
      });
    } catch (error) {
      const persistenceError =
        error instanceof AnalyticsPersistenceError
          ? error
          : new AnalyticsPersistenceError("ANALYTICS_UNAVAILABLE", true, {
              tags: {
                operationId: operation.id,
                phase: "unexpected_worker_error",
              },
            });
      if (!(error instanceof AnalyticsPersistenceError)) {
        logger.error("Unexpected Doris analytics ingestion worker error", {
          operationId: operation.id,
          projectId: operation.projectId,
          error,
        });
      }
      if (!persistenceError.retryable) {
        const terminalized = await (
          input.markTerminalFailure ?? markAnalyticsIngestionTerminalFailure
        )({
          client: input.client ?? prisma,
          operationId: operation.id,
          projectId: operation.projectId,
          status:
            persistenceError.code === "ANALYTICS_QUARANTINED" ||
            persistenceError.code === "ANALYTICS_CONFLICT"
              ? "QUARANTINED"
              : "UNRECOVERABLE",
          reasonCode: persistenceError.code,
          expectedGeneration: generation,
        });
        if (!terminalized) {
          const resolution = await (
            input.resolveAttemptFailure ??
            resolveAnalyticsIngestionAttemptFailure
          )({
            client: input.client ?? prisma,
            operationId: operation.id,
            projectId: operation.projectId,
            reasonCode: persistenceError.code,
            expectedGeneration: generation,
          });
          recordAttemptResolution(resolution);
        }
        throw new UnrecoverableError(persistenceError.message);
      }
      const resolution = await (
        input.resolveAttemptFailure ?? resolveAnalyticsIngestionAttemptFailure
      )({
        client: input.client ?? prisma,
        operationId: operation.id,
        projectId: operation.projectId,
        reasonCode: persistenceError.code,
        expectedGeneration: generation,
      });
      recordAttemptResolution(resolution);
      throw new UnrecoverableError(persistenceError.message);
    }
  };
}
