import { randomUUID } from "node:crypto";

import type { Job, Processor } from "bullmq";
import { prisma } from "@langfuse/shared/src/db";
import {
  AnalyticsEvaluationDispatchEventSchema,
  AnalyticsEvaluationDispatchProvenanceError,
  claimAnalyticsEvaluationDispatch,
  completeAnalyticsEvaluationDispatch,
  logger,
  quarantineAnalyticsEvaluationDispatch,
  QueueName,
  requeueAnalyticsEvaluationDispatch,
  type AnalyticsRuntimeAdmissionContext,
  type TQueueJobTypes,
} from "@langfuse/shared/src/server";

import { getWorkerAnalyticsAdmissionContext } from "../analyticsRuntime";
import {
  createEvalJobs,
  type ManagedEvaluationDispatch,
} from "../features/evaluation/evalService";
import {
  createObservationEvalSchedulerDeps,
  fetchObservationEvalConfigs,
  isObservationAllowedForQueuedObservationEvals,
  scheduleObservationEvals,
} from "../features/evaluation/observationEval";
import { getAnalyticsEvaluationTargetSource } from "../features/evaluation/analyticsEvaluationTargetRuntime";

type EvaluationDispatchJob = Job<
  TQueueJobTypes[QueueName.AnalyticsEvaluationDispatch]
>;

type EvaluationDispatchProcessorDependencies = {
  readonly getAdmissionContext: () => AnalyticsRuntimeAdmissionContext | null;
  readonly claim: typeof claimAnalyticsEvaluationDispatch;
  readonly complete: typeof completeAnalyticsEvaluationDispatch;
  readonly requeue: typeof requeueAnalyticsEvaluationDispatch;
  readonly quarantine: typeof quarantineAnalyticsEvaluationDispatch;
  readonly createJobs: typeof createEvalJobs;
  readonly getTargetSource: typeof getAnalyticsEvaluationTargetSource;
  readonly fetchObservationConfigs: typeof fetchObservationEvalConfigs;
  readonly createObservationScheduler: typeof createObservationEvalSchedulerDeps;
  readonly scheduleObservation: typeof scheduleObservationEvals;
  readonly leaseOwner: () => string;
  readonly logError: (
    message: string,
    metadata: Readonly<Record<string, unknown>>,
  ) => void;
};

export function analyticsEvaluationDispatchQueueProcessorBuilder(
  overrides: Partial<EvaluationDispatchProcessorDependencies> = {},
): Processor {
  const dependencies: EvaluationDispatchProcessorDependencies = {
    getAdmissionContext: getWorkerAnalyticsAdmissionContext,
    claim: claimAnalyticsEvaluationDispatch,
    complete: completeAnalyticsEvaluationDispatch,
    requeue: requeueAnalyticsEvaluationDispatch,
    quarantine: quarantineAnalyticsEvaluationDispatch,
    createJobs: createEvalJobs,
    getTargetSource: getAnalyticsEvaluationTargetSource,
    fetchObservationConfigs: fetchObservationEvalConfigs,
    createObservationScheduler: createObservationEvalSchedulerDeps,
    scheduleObservation: scheduleObservationEvals,
    leaseOwner: () => `evaluation-dispatch-${process.pid}-${randomUUID()}`,
    logError: (message, metadata) => logger.error(message, metadata),
    ...overrides,
  };

  return async (job: EvaluationDispatchJob): Promise<boolean> => {
    const envelope = AnalyticsEvaluationDispatchEventSchema.parse(
      job.data.payload,
    );
    const admissionContext = dependencies.getAdmissionContext();
    if (!admissionContext) {
      throw new AnalyticsEvaluationDispatchProvenanceError(
        "Evaluation dispatch consumer runtime is not admitted",
      );
    }
    const leaseOwner = dependencies.leaseOwner();
    const dispatch = await dependencies.claim({
      client: prisma,
      admissionContext,
      envelope,
      leaseOwner,
      leaseMs: 5 * 60_000,
    });
    if (!dispatch) return true;

    try {
      const managedDispatch: ManagedEvaluationDispatch = {
        envelope,
      };
      switch (dispatch.targetType) {
        case "TRACE_UPSERT":
          await dependencies.createJobs({
            sourceEventType: "trace-upsert",
            event: {
              projectId: dispatch.projectId,
              traceId: dispatch.traceId,
              exactTimestamp: dispatch.targetTimestamp,
              ...(dispatch.traceEnvironment
                ? { traceEnvironment: dispatch.traceEnvironment }
                : {}),
            },
            jobTimestamp: job.data.timestamp,
            enforcedJobTimeScope: "NEW",
            analyticsEvaluationDispatch: managedDispatch.envelope,
          });
          break;
        case "DATASET_RUN_ITEM_UPSERT":
          if (!dispatch.datasetItemId) {
            throw new AnalyticsEvaluationDispatchProvenanceError(
              "Dataset evaluation dispatch target is incomplete",
            );
          }
          await dependencies.createJobs({
            sourceEventType: "dataset-run-item-upsert",
            event: {
              projectId: dispatch.projectId,
              datasetItemId: dispatch.datasetItemId,
              ...(dispatch.datasetItemValidFrom
                ? { datasetItemValidFrom: dispatch.datasetItemValidFrom }
                : {}),
              traceId: dispatch.traceId,
              ...(dispatch.observationId
                ? { observationId: dispatch.observationId }
                : {}),
            },
            jobTimestamp: job.data.timestamp,
            enforcedJobTimeScope: "NEW",
            analyticsEvaluationDispatch: managedDispatch.envelope,
          });
          break;
        case "OBSERVATION_UPSERT": {
          if (!dispatch.observationId) {
            throw new AnalyticsEvaluationDispatchProvenanceError(
              "Observation evaluation dispatch target is incomplete",
            );
          }
          const observation = await dependencies
            .getTargetSource()
            .getObservationForEvaluation({
              projectId: dispatch.projectId,
              traceId: dispatch.traceId,
              observationId: dispatch.observationId,
            });
          if (!observation) {
            await dependencies.quarantine({
              client: prisma,
              dispatchId: dispatch.id,
              expectedGeneration: dispatch.dispatchGeneration,
              leaseOwner,
              failureCode: "EVALUATION_TARGET_NOT_FOUND",
            });
            return true;
          }
          if (isObservationAllowedForQueuedObservationEvals(observation)) {
            const configs = await dependencies.fetchObservationConfigs(
              dispatch.projectId,
            );
            await dependencies.scheduleObservation({
              observation,
              configs,
              schedulerDeps: dependencies.createObservationScheduler({
                analyticsEvaluationDispatch: envelope,
              }),
              failOnConfigError: true,
              jobIdentitySeed: envelope.dispatchId,
            });
          }
          break;
        }
        case "HISTORICAL": {
          if (!dispatch.jobConfigurationId) {
            throw new AnalyticsEvaluationDispatchProvenanceError(
              "Historical evaluation dispatch configuration is missing",
            );
          }
          if (dispatch.observationId && !dispatch.datasetItemId) {
            const observation = await dependencies
              .getTargetSource()
              .getObservationForEvaluation({
                projectId: dispatch.projectId,
                traceId: dispatch.traceId,
                observationId: dispatch.observationId,
              });
            if (!observation) {
              await dependencies.quarantine({
                client: prisma,
                dispatchId: dispatch.id,
                expectedGeneration: dispatch.dispatchGeneration,
                leaseOwner,
                failureCode: "EVALUATION_TARGET_NOT_FOUND",
              });
              return true;
            }
            const configs = (
              await dependencies.fetchObservationConfigs(dispatch.projectId, {
                jobConfigurationId: dispatch.jobConfigurationId,
                includeInactive: true,
              })
            ).filter(({ id }) => id === dispatch.jobConfigurationId);
            if (configs.length !== 1) {
              throw new AnalyticsEvaluationDispatchProvenanceError(
                "Historical observation evaluator is unavailable",
              );
            }
            await dependencies.scheduleObservation({
              observation,
              configs,
              schedulerDeps: dependencies.createObservationScheduler({
                analyticsEvaluationDispatch: envelope,
              }),
              executionMode: "MANUAL",
              failOnConfigError: true,
              jobIdentitySeed: envelope.dispatchId,
            });
            break;
          }
          await dependencies.createJobs({
            sourceEventType: "ui-create-eval",
            event: dispatch.datasetItemId
              ? {
                  projectId: dispatch.projectId,
                  datasetItemId: dispatch.datasetItemId,
                  ...(dispatch.datasetItemValidFrom
                    ? {
                        datasetItemValidFrom: dispatch.datasetItemValidFrom,
                      }
                    : {}),
                  traceId: dispatch.traceId,
                  ...(dispatch.observationId
                    ? { observationId: dispatch.observationId }
                    : {}),
                  configId: dispatch.jobConfigurationId,
                  timestamp: dispatch.targetTimestamp,
                }
              : {
                  projectId: dispatch.projectId,
                  traceId: dispatch.traceId,
                  configId: dispatch.jobConfigurationId,
                  timestamp: dispatch.targetTimestamp,
                  exactTimestamp: dispatch.targetTimestamp,
                  ...(dispatch.traceEnvironment
                    ? { traceEnvironment: dispatch.traceEnvironment }
                    : {}),
                },
            jobTimestamp: job.data.timestamp,
            executionMode: "MANUAL",
            analyticsEvaluationDispatch: managedDispatch.envelope,
          });
          break;
        }
      }

      const completed = await dependencies.complete({
        client: prisma,
        dispatchId: dispatch.id,
        expectedGeneration: dispatch.dispatchGeneration,
        leaseOwner,
      });
      if (!completed) {
        throw new AnalyticsEvaluationDispatchProvenanceError(
          "Evaluation dispatch completion was fenced",
        );
      }
      return true;
    } catch (error) {
      if (error instanceof AnalyticsEvaluationDispatchProvenanceError) {
        await dependencies.quarantine({
          client: prisma,
          dispatchId: dispatch.id,
          expectedGeneration: dispatch.dispatchGeneration,
          leaseOwner,
          failureCode: "EVALUATION_DISPATCH_PROVENANCE_MISMATCH",
        });
        dependencies.logError(
          "Quarantined invalid evaluation dispatch execution",
          {
            dispatchId: dispatch.id,
            errorCode: "EVALUATION_DISPATCH_PROVENANCE_MISMATCH",
          },
        );
        throw error;
      }
      await dependencies.requeue({
        client: prisma,
        dispatchId: dispatch.id,
        expectedGeneration: dispatch.dispatchGeneration,
        leaseOwner,
      });
      dependencies.logError("Requeued failed evaluation dispatch execution", {
        dispatchId: dispatch.id,
        errorKind: error instanceof Error ? error.name : "UnknownError",
      });
      return true;
    }
  };
}

export const analyticsEvaluationDispatchQueueProcessor =
  analyticsEvaluationDispatchQueueProcessorBuilder();
