import type { AnalyticsIntegrationType } from "@prisma/client";
import {
  claimAnalyticsIntegrationExecution,
  completeAnalyticsIntegrationExecution,
  deferAnalyticsIntegrationExecution,
  DorisAnalyticsIntegrationExportSource,
  logger,
  renewAnalyticsIntegrationExecutionClaim,
  type AnalyticsIntegrationExecutionEnvelope,
  type AnalyticsIntegrationSemanticRecord,
} from "@langfuse/shared/src/server";

import {
  getWorkerAnalyticsAdmissionContext,
  isWorkerAnalyticsRuntimeFenced,
} from "../../analyticsRuntime";
import { WORKER_HOST_ID } from "../../utils/hostId";
import { withAnalyticsIntegrationLeaseHeartbeat } from "./AnalyticsIntegrationLeaseHeartbeat";

const EXECUTION_LEASE_MS = 5 * 60 * 1000;
const EXECUTION_HEARTBEAT_MS = 60 * 1000;

export type ManagedAnalyticsIntegrationPayload =
  AnalyticsIntegrationExecutionEnvelope;

type ManagedAnalyticsIntegrationFailureCode =
  | "INTEGRATION_DELIVERY_FAILED"
  | "BLOB_DELIVERY_FAILED";

export class ManagedAnalyticsIntegrationDeliveryError extends Error {
  constructor(readonly code: ManagedAnalyticsIntegrationFailureCode) {
    super(`Analytics integration delivery failed (${code})`);
    this.name = "ManagedAnalyticsIntegrationDeliveryError";
  }
}

export function managedAnalyticsIntegrationDeliveryFailure(input: {
  readonly code: ManagedAnalyticsIntegrationFailureCode;
  readonly executionId: string;
  readonly integrationType: AnalyticsIntegrationType;
  readonly error: unknown;
}): ManagedAnalyticsIntegrationDeliveryError {
  logger.error("Managed analytics integration delivery failed", {
    failureCode: input.code,
    executionId: input.executionId,
    integrationType: input.integrationType,
    // Do not serialize the error or its message: adapters may include a DSN,
    // Authorization header, SQL, or telemetry payload in their raw cause.
    causeType:
      input.error instanceof Error ? input.error.name : typeof input.error,
  });
  return new ManagedAnalyticsIntegrationDeliveryError(input.code);
}

export function isManagedAnalyticsIntegrationPayload(payload: {
  readonly projectId: string;
}): payload is ManagedAnalyticsIntegrationPayload {
  return "executionId" in payload;
}

export async function processDorisAnalyticsIntegrationExecution(input: {
  readonly payload: ManagedAnalyticsIntegrationPayload;
  readonly expectedIntegrationType: AnalyticsIntegrationType;
  readonly projectName: string;
  readonly send: (
    records: readonly AnalyticsIntegrationSemanticRecord[],
  ) => Promise<void>;
  readonly source?: DorisAnalyticsIntegrationExportSource;
  readonly now?: () => Date;
}): Promise<{ readonly sent: number; readonly sourceDeleted: number } | null> {
  if (input.payload.integrationType !== input.expectedIntegrationType) {
    throw new Error("Analytics integration execution type mismatch");
  }
  const admissionContext = getWorkerAnalyticsAdmissionContext();
  if (
    !admissionContext ||
    admissionContext.backend !== "doris" ||
    isWorkerAnalyticsRuntimeFenced()
  ) {
    throw new Error("Doris analytics integration worker is not admitted");
  }
  const now = input.now ?? (() => new Date());
  const claimed = await claimAnalyticsIntegrationExecution({
    admissionContext,
    envelope: input.payload,
    workerId: WORKER_HOST_ID,
    now: now(),
  });
  if (!claimed) return null;

  try {
    const source = input.source ?? new DorisAnalyticsIntegrationExportSource();
    const renew = () =>
      renewAnalyticsIntegrationExecutionClaim({
        admissionContext,
        executionId: claimed.execution.id,
        workerId: WORKER_HOST_ID,
        now: now(),
        leaseMs: EXECUTION_LEASE_MS,
      });
    const read = await withAnalyticsIntegrationLeaseHeartbeat({
      intervalMs: EXECUTION_HEARTBEAT_MS,
      renew,
      run: async () => {
        const exactRead = await source.readExact({
          projectId: claimed.execution.projectId,
          projectName: input.projectName,
          items: claimed.manifest.items,
        });
        await input.send(exactRead.records);
        return exactRead;
      },
    });
    await renew();
    const sourceDeletedDeliveryIds = read.missing.flatMap(
      ({ deliveryIds }) => deliveryIds,
    );
    const completedAt = now();
    await completeAnalyticsIntegrationExecution({
      executionId: claimed.execution.id,
      workerId: WORKER_HOST_ID,
      sourceDeletedDeliveryIds,
      lastSyncAt: completedAt,
      now: completedAt,
    });
    return {
      sent: read.records.length,
      sourceDeleted: sourceDeletedDeliveryIds.length,
    };
  } catch (error) {
    try {
      await deferAnalyticsIntegrationExecution({
        executionId: claimed.execution.id,
        workerId: WORKER_HOST_ID,
        failureCode: "INTEGRATION_DELIVERY_FAILED",
        now: now(),
      });
    } catch (deferError) {
      logger.error("Failed to defer analytics integration delivery", {
        failureCode: "INTEGRATION_DELIVERY_FAILED",
        executionId: claimed.execution.id,
        integrationType: input.expectedIntegrationType,
        causeType:
          deferError instanceof Error ? deferError.name : typeof deferError,
      });
    }
    throw managedAnalyticsIntegrationDeliveryFailure({
      code: "INTEGRATION_DELIVERY_FAILED",
      executionId: claimed.execution.id,
      integrationType: input.expectedIntegrationType,
      error,
    });
  }
}
