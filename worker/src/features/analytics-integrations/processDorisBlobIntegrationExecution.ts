import {
  claimAnalyticsIntegrationExecution,
  completeAnalyticsIntegrationExecution,
  deferAnalyticsIntegrationExecution,
  DorisBlobAnalyticsExportSource,
  logger,
  renewAnalyticsIntegrationExecutionClaim,
  type DorisBlobExactReadResult,
} from "@langfuse/shared/src/server";
import type { ObservationFieldGroupFull } from "@langfuse/shared";

import {
  getWorkerAnalyticsAdmissionContext,
  isWorkerAnalyticsRuntimeFenced,
} from "../../analyticsRuntime";
import { WORKER_HOST_ID } from "../../utils/hostId";
import { withAnalyticsIntegrationLeaseHeartbeat } from "./AnalyticsIntegrationLeaseHeartbeat";
import {
  managedAnalyticsIntegrationDeliveryFailure,
  type ManagedAnalyticsIntegrationPayload,
} from "./processDorisAnalyticsIntegrationExecution";

const EXECUTION_LEASE_MS = 15 * 60 * 1000;
const EXECUTION_HEARTBEAT_MS = 60 * 1000;

export async function processDorisBlobIntegrationExecution(input: {
  readonly payload: ManagedAnalyticsIntegrationPayload;
  readonly observationTable: "observations" | "observations_v2";
  readonly observationFieldGroups: readonly ObservationFieldGroupFull[];
  readonly upload: (
    read: DorisBlobExactReadResult,
    executionId: string,
  ) => Promise<void>;
  readonly source?: DorisBlobAnalyticsExportSource;
  readonly now?: () => Date;
}): Promise<{ readonly sent: number; readonly sourceDeleted: number } | null> {
  if (input.payload.integrationType !== "BLOB_STORAGE") {
    throw new Error("Blob analytics integration execution type mismatch");
  }
  const admissionContext = getWorkerAnalyticsAdmissionContext();
  if (
    !admissionContext ||
    admissionContext.backend !== "doris" ||
    isWorkerAnalyticsRuntimeFenced()
  ) {
    throw new Error("Doris blob integration worker is not admitted");
  }
  const now = input.now ?? (() => new Date());
  const claimed = await claimAnalyticsIntegrationExecution({
    admissionContext,
    envelope: input.payload,
    workerId: WORKER_HOST_ID,
    now: now(),
    leaseMs: EXECUTION_LEASE_MS,
  });
  if (!claimed) return null;

  try {
    const source = input.source ?? new DorisBlobAnalyticsExportSource();
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
          items: claimed.manifest.items,
          observationTable: input.observationTable,
          observationFieldGroups: input.observationFieldGroups,
        });
        await input.upload(exactRead, claimed.execution.id);
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
        failureCode: "BLOB_DELIVERY_FAILED",
        now: now(),
      });
    } catch (deferError) {
      logger.error("Failed to defer blob analytics integration delivery", {
        failureCode: "BLOB_DELIVERY_FAILED",
        executionId: claimed.execution.id,
        integrationType: "BLOB_STORAGE",
        causeType:
          deferError instanceof Error ? deferError.name : typeof deferError,
      });
    }
    throw managedAnalyticsIntegrationDeliveryFailure({
      code: "BLOB_DELIVERY_FAILED",
      executionId: claimed.execution.id,
      integrationType: "BLOB_STORAGE",
      error,
    });
  }
}
