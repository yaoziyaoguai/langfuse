import {
  ActionId,
  BatchEvalSourceTable,
  BatchEvalSourceTableSchema,
  BatchActionQuerySchema,
  BatchActionStatus,
  BatchActionType,
  ObservationAddToDatasetConfigSchema,
} from "@langfuse/shared";
import { type AnalyticsBackend } from "@langfuse/shared/analytics-backend";
import { prisma } from "@langfuse/shared/src/db";
import {
  BatchActionProcessingEventSchema,
  BatchActionQueue,
  lockAnalyticsAdmission,
  lockLegacyAnalyticsAdmission,
  logger,
  QueueJobs,
  QueueName,
  type AnalyticsRuntimeAdmissionContext,
  type TQueueJobTypes,
} from "@langfuse/shared/src/server";
import { z } from "zod";

import { PeriodicExclusiveRunner } from "../../utils/PeriodicExclusiveRunner";

export const BATCH_ACTION_PUBLICATION_RECOVERY_LOCK_KEY =
  "langfuse:batch-action-publication-recovery";

const DEFAULT_INTERVAL_MS = 10_000;
const DEFAULT_BATCH_SIZE = 50;
const DEFAULT_LOCK_TTL_SECONDS = 300;
const MAX_FAILURE_LOG_LENGTH = 2_000;
const PUBLICATION_FENCE_TIMEOUT_MS = 35 * 60_000;

const RECOVERABLE_ACTION_TYPES = [
  ActionId.ObservationAddToDataset,
  ActionId.ObservationBatchEvaluation,
] as const;

const StrictBatchActionQuerySchema = BatchActionQuerySchema.strict();
const StrictAddToDatasetConfigSchema =
  ObservationAddToDatasetConfigSchema.strict();
const StrictBatchEvaluationConfigSchema = z
  .object({
    evaluatorIds: z.array(z.string().min(1)).min(1),
    sourceTable: BatchEvalSourceTableSchema.default(
      BatchEvalSourceTable.EVENTS,
    ),
  })
  .strict();

export type RecoverableBatchAction = {
  readonly id: string;
  readonly projectId: string;
  readonly actionType: string;
  readonly tableName: string;
  readonly status: string;
  readonly query: unknown;
  readonly config: unknown;
  readonly createdAt: Date;
};

type RecoveryCursor = {
  readonly id: string;
  readonly createdAt: Date;
};

type RecoveredBatchActionJob = TQueueJobTypes[QueueName.BatchActionQueue];
export type BatchActionPublicationRecoveryOutcome =
  | "published"
  | "terminal-failure"
  | "stale";

type FindQueuedActions = (input: {
  readonly after: RecoveryCursor | null;
  readonly limit: number;
}) => Promise<readonly RecoverableBatchAction[]>;

type PublishRecoveredAction = (
  job: RecoveredBatchActionJob,
  action: RecoverableBatchAction,
) => Promise<BatchActionPublicationRecoveryOutcome>;

type MarkFailed = (
  batchActionId: string,
  projectId: string,
  failureLog: string,
) => Promise<void>;

type RunExclusive = (
  operation: () => Promise<number | void>,
) => Promise<number | void>;

type RecoveryDependencies = {
  readonly findQueuedActions: FindQueuedActions;
  readonly publish: PublishRecoveredAction;
  readonly markFailed: MarkFailed;
  readonly runExclusive?: RunExclusive;
};

type PublicationFenceDependencies = {
  readonly client: Pick<typeof prisma, "$transaction">;
  readonly getQueue: typeof BatchActionQueue.getInstance;
  readonly lockAdmission: typeof lockAnalyticsAdmission;
  readonly lockLegacyAdmission: typeof lockLegacyAnalyticsAdmission;
};

const defaultPublicationFenceDependencies: PublicationFenceDependencies = {
  client: prisma,
  getQueue: () => BatchActionQueue.getInstance(),
  lockAdmission: lockAnalyticsAdmission,
  lockLegacyAdmission: lockLegacyAnalyticsAdmission,
};

class TerminalPublicationIntentError extends Error {}

function boundedFailureLog(message: string): string {
  return message.slice(0, MAX_FAILURE_LOG_LENGTH);
}

function safeFailureKind(error: unknown): string {
  return error instanceof Error ? error.name : "UnknownError";
}

function summarizeValidationFailure(error: unknown): string {
  if (error instanceof z.ZodError) {
    return error.issues
      .slice(0, 10)
      .map((issue) => {
        const path = issue.path.length > 0 ? issue.path.join(".") : "root";
        return `${path}: ${issue.message}`;
      })
      .join("; ");
  }
  return error instanceof Error ? error.message : "unknown validation error";
}

function invalidPublicationIntent(
  error: unknown,
): TerminalPublicationIntentError {
  return new TerminalPublicationIntentError(
    boundedFailureLog(
      `Invalid durable BatchAction publication intent: ${summarizeValidationFailure(error)}`,
    ),
  );
}

function requireIdentifier(value: string, field: string): string {
  const parsed = z.string().min(1).safeParse(value);
  if (!parsed.success) {
    throw invalidPublicationIntent(
      new z.ZodError(
        parsed.error.issues.map((issue) => ({
          ...issue,
          path: [field, ...issue.path],
        })),
      ),
    );
  }
  return parsed.data;
}

/**
 * Recreates the exact queue envelope from the durable PostgreSQL intent.
 * BullMQ jobId is the BatchAction primary key, so every scan is idempotent.
 */
export function buildRecoveredBatchActionJob(
  action: RecoverableBatchAction,
): RecoveredBatchActionJob {
  try {
    const id = requireIdentifier(action.id, "id");
    const projectId = requireIdentifier(action.projectId, "projectId");
    const query = StrictBatchActionQuerySchema.parse(action.query);

    const payload =
      action.actionType === ActionId.ObservationAddToDataset
        ? BatchActionProcessingEventSchema.parse({
            actionId: ActionId.ObservationAddToDataset,
            batchActionId: id,
            projectId,
            tableName: action.tableName,
            cutoffCreatedAt: action.createdAt,
            query,
            config: StrictAddToDatasetConfigSchema.parse(action.config),
            type: BatchActionType.Create,
          })
        : action.actionType === ActionId.ObservationBatchEvaluation
          ? (() => {
              const config = StrictBatchEvaluationConfigSchema.parse(
                action.config,
              );
              return BatchActionProcessingEventSchema.parse({
                actionId: ActionId.ObservationBatchEvaluation,
                batchActionId: id,
                projectId,
                cutoffCreatedAt: action.createdAt,
                query,
                evaluatorIds: config.evaluatorIds,
                sourceTable: config.sourceTable,
              });
            })()
          : (() => {
              throw new TerminalPublicationIntentError(
                `Batch action ${action.actionType} is not a recoverable publication intent`,
              );
            })();

    return {
      id,
      name: QueueJobs.BatchActionProcessingJob,
      timestamp: action.createdAt,
      payload,
    };
  } catch (error) {
    if (error instanceof TerminalPublicationIntentError) throw error;
    throw invalidPublicationIntent(error);
  }
}

export async function processBatchActionPublicationRecoveryPage(input: {
  readonly backend: AnalyticsBackend;
  readonly actions: readonly RecoverableBatchAction[];
  readonly publish: PublishRecoveredAction;
  readonly markFailed: MarkFailed;
}): Promise<{
  readonly published: number;
  readonly terminalFailures: number;
  readonly retryableFailures: number;
  readonly nextCursor: RecoveryCursor | null;
}> {
  let published = 0;
  let terminalFailures = 0;
  let retryableFailures = 0;
  let nextCursor: RecoveryCursor | null = null;

  for (const action of input.actions) {
    nextCursor = { id: action.id, createdAt: action.createdAt };
    let job: RecoveredBatchActionJob;
    try {
      job = buildRecoveredBatchActionJob(action);
    } catch (error) {
      if (!(error instanceof TerminalPublicationIntentError)) throw error;
      try {
        await input.markFailed(
          action.id,
          action.projectId,
          boundedFailureLog(error.message),
        );
        terminalFailures += 1;
        logger.warn("Terminated invalid BatchAction publication intent", {
          batchActionId: action.id,
          projectId: action.projectId,
          actionType: action.actionType,
          reason: boundedFailureLog(error.message),
        });
      } catch (markError) {
        retryableFailures += 1;
        logger.warn("Failed to terminate invalid BatchAction intent", {
          batchActionId: action.id,
          projectId: action.projectId,
          actionType: action.actionType,
          errorKind: safeFailureKind(markError),
        });
        break;
      }
      continue;
    }

    try {
      const outcome = await input.publish(job, action);
      if (outcome === "published") published += 1;
      else if (outcome === "terminal-failure") terminalFailures += 1;
    } catch (error) {
      // Queue.add may have reached Redis even when the response was lost. The
      // PostgreSQL row remains QUEUED and the stable jobId makes retry safe.
      retryableFailures += 1;
      logger.warn("BatchAction publication recovery will retry", {
        batchActionId: action.id,
        projectId: action.projectId,
        actionType: action.actionType,
        errorKind: safeFailureKind(error),
      });
      break;
    }
  }

  return {
    published,
    terminalFailures,
    retryableFailures,
    nextCursor,
  };
}

const findQueuedActions: FindQueuedActions = async ({ after, limit }) =>
  prisma.batchAction.findMany({
    where: {
      status: BatchActionStatus.Queued,
      actionType: { in: [...RECOVERABLE_ACTION_TYPES] },
      ...(after
        ? {
            OR: [
              { createdAt: { gt: after.createdAt } },
              { createdAt: after.createdAt, id: { gt: after.id } },
            ],
          }
        : {}),
    },
    orderBy: [{ createdAt: "asc" }, { id: "asc" }],
    take: limit,
    select: {
      id: true,
      projectId: true,
      actionType: true,
      tableName: true,
      status: true,
      query: true,
      config: true,
      createdAt: true,
    },
  });

const markFailed: MarkFailed = async (batchActionId, projectId, failureLog) => {
  await prisma.batchAction.updateMany({
    where: {
      id: batchActionId,
      projectId,
      status: BatchActionStatus.Queued,
    },
    data: {
      status: BatchActionStatus.Failed,
      finishedAt: new Date(),
      totalCount: 0,
      processedCount: 0,
      failedCount: 0,
      log: boundedFailureLog(failureLog),
    },
  });
};

export async function publishRecoveredBatchAction(
  input: {
    readonly backend: AnalyticsBackend;
    readonly getAdmissionContext: () => AnalyticsRuntimeAdmissionContext | null;
    readonly job: RecoveredBatchActionJob;
    readonly action: RecoverableBatchAction;
  },
  dependencies: PublicationFenceDependencies = defaultPublicationFenceDependencies,
): Promise<BatchActionPublicationRecoveryOutcome> {
  const queue = dependencies.getQueue();
  if (!queue) throw new Error("BatchActionQueue is not initialized");

  const admissionContext = input.getAdmissionContext();
  if (admissionContext && admissionContext.backend !== input.backend) {
    throw new Error("BatchAction recovery runtime backend changed");
  }

  return dependencies.client.$transaction(
    async (transaction) => {
      if (admissionContext) {
        const capability =
          input.action.actionType === ActionId.ObservationBatchEvaluation
            ? ("evaluations" as const)
            : undefined;
        await dependencies.lockAdmission({
          transaction,
          runtimeLeaseId: admissionContext.runtimeLeaseId,
          expectedBackend: input.backend,
          expectedDeploymentGeneration: admissionContext.deploymentGeneration,
          ...(capability
            ? { capability, action: "externalProducer" as const }
            : { action: "foundation" as const }),
        });
      } else {
        await dependencies.lockLegacyAdmission(transaction);
      }

      const current = await transaction.batchAction.findUnique({
        where: {
          id: input.action.id,
          projectId: input.action.projectId,
        },
        select: { status: true, actionType: true },
      });
      if (
        !current ||
        current.status !== BatchActionStatus.Queued ||
        current.actionType !== input.action.actionType
      ) {
        return "stale";
      }

      const publishedJob = await queue.add(
        QueueJobs.BatchActionProcessingJob,
        input.job,
        {
          jobId: input.action.id,
        },
      );
      const publishedState = await publishedJob.getState();
      if (publishedState === "unknown") {
        throw new Error("BatchAction publication state is unknown");
      }
      if (publishedState !== "failed" && publishedState !== "completed") {
        return "published";
      }

      // retry(expectedState) 在 Redis 内原子校验并移动 job。若 DLQ retry
      // 先赢，本次调用会失败并保留 QUEUED intent，绝不能删除已重新排队的任务。
      await publishedJob.retry(publishedState);
      return "published";
    },
    {
      maxWait: 120_000,
      timeout: PUBLICATION_FENCE_TIMEOUT_MS,
    },
  );
}

export class BatchActionPublicationRecoveryRunner extends PeriodicExclusiveRunner {
  private readonly backend: AnalyticsBackend;
  private readonly intervalMs: number;
  private readonly batchSize: number;
  private readonly dependencies: RecoveryDependencies;
  private after: RecoveryCursor | null = null;

  protected get defaultIntervalMs(): number {
    return this.intervalMs;
  }

  constructor(input: {
    readonly backend: AnalyticsBackend;
    readonly getAdmissionContext: () => AnalyticsRuntimeAdmissionContext | null;
    readonly intervalMs?: number;
    readonly batchSize?: number;
    readonly lockTtlSeconds?: number;
    readonly dependencies?: Partial<RecoveryDependencies>;
  }) {
    const intervalMs = input.intervalMs ?? DEFAULT_INTERVAL_MS;
    const batchSize = input.batchSize ?? DEFAULT_BATCH_SIZE;
    const lockTtlSeconds = input.lockTtlSeconds ?? DEFAULT_LOCK_TTL_SECONDS;
    if (
      !Number.isSafeInteger(intervalMs) ||
      intervalMs < 1_000 ||
      !Number.isSafeInteger(batchSize) ||
      batchSize < 1 ||
      batchSize > 1_000 ||
      !Number.isSafeInteger(lockTtlSeconds) ||
      lockTtlSeconds < 60
    ) {
      throw new TypeError(
        "Invalid BatchAction publication recovery configuration",
      );
    }

    super({
      name: "BatchActionPublicationRecoveryRunner",
      lockKey: BATCH_ACTION_PUBLICATION_RECOVERY_LOCK_KEY,
      lockTtlSeconds,
      onUnavailable: "fail",
    });
    this.backend = input.backend;
    this.intervalMs = intervalMs;
    this.batchSize = batchSize;
    this.dependencies = {
      findQueuedActions,
      markFailed,
      publish: async (job, action) => {
        if (!(await this.lock.extend())) {
          throw new Error("BatchAction publication recovery lock was lost");
        }
        return publishRecoveredBatchAction({
          backend: input.backend,
          getAdmissionContext: input.getAdmissionContext,
          job,
          action,
        });
      },
      ...input.dependencies,
    };
  }

  public override start(): void {
    logger.info(`Starting ${this.instanceName}`, {
      backend: this.backend,
      intervalMs: this.intervalMs,
      batchSize: this.batchSize,
    });
    super.start();
  }

  protected async execute(): Promise<number | void> {
    const operation = async (): Promise<number | void> => {
      const actions = await this.dependencies.findQueuedActions({
        after: this.after,
        limit: this.batchSize,
      });
      if (actions.length === 0) {
        if (this.after) {
          this.after = null;
          return 0;
        }
        return;
      }

      const result = await processBatchActionPublicationRecoveryPage({
        backend: this.backend,
        actions,
        publish: this.dependencies.publish,
        markFailed: this.dependencies.markFailed,
      });
      this.after = result.nextCursor;

      if (
        result.published > 0 ||
        result.terminalFailures > 0 ||
        result.retryableFailures > 0
      ) {
        logger.info("Processed BatchAction publication recovery page", {
          scanned: actions.length,
          published: result.published,
          terminalFailures: result.terminalFailures,
          retryableFailures: result.retryableFailures,
        });
      }

      if (result.retryableFailures > 0) return;
      if (actions.length === this.batchSize) return 0;
      this.after = null;
      return;
    };

    return this.dependencies.runExclusive
      ? this.dependencies.runExclusive(operation)
      : this.withLock(operation);
  }
}
