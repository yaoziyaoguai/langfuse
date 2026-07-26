import { createHash } from "node:crypto";

import {
  acceptAnalyticsIngestion,
  CURRENT_ANALYTICS_CANONICALIZER_VERSION,
  CURRENT_ANALYTICS_SCHEMA_VERSION,
  getAnalyticsIngestionStatusForProject,
  getS3EventStorageClient,
  type AnalyticsRuntimeAdmissionContext,
  type StorageService,
} from "@langfuse/shared/src/server";

import { env } from "../../env";
import { getWorkerAnalyticsAdmissionContext } from "../../analyticsRuntime";
import type { EvalScoreWritePayload } from "./evalScoreEvent";

const TERMINAL_FAILURE_STATUSES = new Set([
  "PARTIAL_FAILED",
  "QUARANTINED",
  "UNRECOVERABLE",
  "CANCELLED_BY_DELETION",
  "COMPLETED_WITH_CANCELLATIONS",
]);

type DorisEvalScorePersistenceDependencies = {
  readonly accept: typeof acceptAnalyticsIngestion;
  readonly getStatus: typeof getAnalyticsIngestionStatusForProject;
  readonly getAdmissionContext: () => AnalyticsRuntimeAdmissionContext | null;
  readonly getStorageService: () => StorageService;
  readonly now: () => Date;
  readonly wait: (delayMs: number) => Promise<void>;
};

export function buildDorisEvalScoreOperationId(jobExecutionId: string): string {
  if (!jobExecutionId) throw new TypeError("Invalid evaluation job ID");
  return createHash("sha256")
    .update("langfuse-doris-eval-score-v1\0")
    .update(jobExecutionId)
    .digest("hex");
}

export async function persistDorisEvalScoreBatch(
  input: {
    readonly projectId: string;
    readonly jobExecutionId: string;
    readonly scoreWritePayloads: readonly EvalScoreWritePayload[];
    readonly maxWaitMs?: number;
    readonly pollIntervalMs?: number;
  },
  overrides: Partial<DorisEvalScorePersistenceDependencies> = {},
): Promise<{ readonly operationId: string }> {
  const dependencies: DorisEvalScorePersistenceDependencies = {
    accept: acceptAnalyticsIngestion,
    getStatus: getAnalyticsIngestionStatusForProject,
    getAdmissionContext: getWorkerAnalyticsAdmissionContext,
    getStorageService: () =>
      getS3EventStorageClient(env.LANGFUSE_S3_EVENT_UPLOAD_BUCKET),
    now: () => new Date(),
    wait: (delayMs) =>
      new Promise((resolve) => {
        setTimeout(resolve, delayMs);
      }),
    ...overrides,
  };
  const maxWaitMs = input.maxWaitMs ?? 90_000;
  const pollIntervalMs = input.pollIntervalMs ?? 250;
  if (
    !input.projectId ||
    !input.jobExecutionId ||
    input.scoreWritePayloads.length === 0 ||
    !Number.isSafeInteger(maxWaitMs) ||
    maxWaitMs < 1 ||
    !Number.isSafeInteger(pollIntervalMs) ||
    pollIntervalMs < 1
  ) {
    throw new TypeError("Invalid Doris evaluation score persistence input");
  }
  const acceptedAt = dependencies.now();
  if (!Number.isFinite(acceptedAt.getTime())) {
    throw new TypeError("Evaluation score acceptance time is invalid");
  }
  const admissionContext = dependencies.getAdmissionContext();
  if (!admissionContext || admissionContext.backend !== "doris") {
    throw new Error("Doris evaluation score runtime is not admitted");
  }
  const operationId = buildDorisEvalScoreOperationId(input.jobExecutionId);
  let status = await dependencies.getStatus({
    operationId,
    projectId: input.projectId,
  });
  if (!status) {
    try {
      await dependencies.accept({
        projectId: input.projectId,
        operationId,
        sourceOperationId: `evaluation:${input.jobExecutionId}`,
        envelope: {
          formatVersion: 1,
          source: "score",
          payload: input.scoreWritePayloads.map(({ event }) => event),
          attribution: {
            ingestionApiKey: "",
            ingestionSdkName: "langfuse-evaluator",
            ingestionSdkVersion: "internal",
          },
        },
        canonicalizerVersion: CURRENT_ANALYTICS_CANONICALIZER_VERSION,
        schemaVersion: CURRENT_ANALYTICS_SCHEMA_VERSION,
        storageService: dependencies.getStorageService(),
        rawPrefix: env.LANGFUSE_S3_EVENT_UPLOAD_PREFIX,
        admissionContext,
        acceptedAt,
        acceptedAtNanos: BigInt(acceptedAt.getTime()) * 1_000_000n,
      });
    } catch (error) {
      // 同一 JobExecution 的并发重试可能已用相同 operationId 抢先提交；
      // 只有 durable row 确实存在时才复用，其他 accept 失败仍向外传播。
      status = await dependencies.getStatus({
        operationId,
        projectId: input.projectId,
      });
      if (!status) throw error;
    }
    status ??= await dependencies.getStatus({
      operationId,
      projectId: input.projectId,
    });
  }

  const deadline = Date.now() + maxWaitMs;
  while (Date.now() <= deadline) {
    if (status?.status === "VISIBLE") return { operationId };
    if (status && TERMINAL_FAILURE_STATUSES.has(status.status)) {
      throw new Error(
        `Doris evaluation score ingestion ${operationId} terminalized as ${status.status}${
          status.reasonCode ? ` (${status.reasonCode})` : ""
        }`,
      );
    }
    await dependencies.wait(pollIntervalMs);
    status = await dependencies.getStatus({
      operationId,
      projectId: input.projectId,
    });
  }
  throw new Error(
    `Doris evaluation score ingestion ${operationId} did not become VISIBLE`,
  );
}
