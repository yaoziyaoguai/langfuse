import { randomUUID } from "node:crypto";

import type { Queue } from "bullmq";
import type { AnalyticsIntegrationType } from "@prisma/client";
import {
  DorisAnalyticsIntegrationExportSource,
  ensureDorisAnalyticsIntegrationState,
  publishAnalyticsIntegrationExecution,
  replayDorisAnalyticsIntegrationDrainCapture,
  sealDorisAnalyticsIntegrationBootstrapManifest,
  sealAnalyticsIntegrationExecution,
  type AnalyticsIntegrationExecutionEnvelope,
} from "@langfuse/shared/src/server";
import { prisma } from "@langfuse/shared/src/db";

import { getWorkerAnalyticsAdmissionContext } from "../../analyticsRuntime";

export async function scheduleDorisAnalyticsIntegrations(input: {
  readonly integrationType: AnalyticsIntegrationType;
  readonly projectIds: readonly string[];
  readonly queue: Queue;
  readonly jobName: string;
}): Promise<number> {
  const admissionContext = getWorkerAnalyticsAdmissionContext();
  if (!admissionContext || admissionContext.backend !== "doris") {
    throw new Error("Doris analytics integration scheduler is not admitted");
  }
  let scheduled = 0;
  const source = new DorisAnalyticsIntegrationExportSource();
  for (const projectId of input.projectIds) {
    let state = await ensureDorisAnalyticsIntegrationState({
      admissionContext,
      projectId,
      integrationType: input.integrationType,
    });
    if (state.rescanRequired) {
      await replayDorisAnalyticsIntegrationDrainCapture({
        admissionContext,
        projectId,
        integrationType: input.integrationType,
      });
      state = await prisma.analyticsIntegrationState.findUniqueOrThrow({
        where: {
          projectId_integrationType: {
            projectId,
            integrationType: input.integrationType,
          },
        },
      });
    }
    let envelopes: readonly AnalyticsIntegrationExecutionEnvelope[] = [];
    if (
      (state.status === "BOOTSTRAPPING_DARK" ||
        state.status === "BOOTSTRAPPING_ACTIVE") &&
      !state.bootstrapManifestChecksum
    ) {
      const identities = await source.scanBootstrapIdentities({
        projectId,
        limit: 100_000,
      });
      envelopes = await sealDorisAnalyticsIntegrationBootstrapManifest({
        admissionContext,
        projectId,
        integrationType: input.integrationType,
        identities,
      });
      state = await prisma.analyticsIntegrationState.findUniqueOrThrow({
        where: {
          projectId_integrationType: {
            projectId,
            integrationType: input.integrationType,
          },
        },
      });
    } else if (
      state.status === "BOOTSTRAPPING_DARK" ||
      state.status === "BOOTSTRAPPING_ACTIVE"
    ) {
      envelopes = await sealDorisAnalyticsIntegrationBootstrapManifest({
        admissionContext,
        projectId,
        integrationType: input.integrationType,
        identities:
          (
            state.bootstrapManifest as {
              readonly items?: readonly {
                readonly deliveryKind:
                  | "TRACE"
                  | "GENERATION"
                  | "OBSERVATION"
                  | "SCORE";
                readonly entityKey: string;
              }[];
            } | null
          )?.items ?? [],
      });
    }
    if (state.status === "BOOTSTRAPPING_DARK") continue;
    if (state.status === "ACTIVE" || state.status === "RESCANNING") {
      const incremental = await sealAnalyticsIntegrationExecution({
        admissionContext,
        projectId,
        integrationType: input.integrationType,
      });
      if (incremental) envelopes = [incremental];
    }
    for (const envelope of envelopes) {
      const queueJobId = envelope.executionId;
      const published = await publishAnalyticsIntegrationExecution({
        client: prisma,
        admissionContext,
        envelope,
        queueJobId,
        publish: async (authoritativeEnvelope) => {
          const job = await input.queue.add(
            input.jobName,
            {
              id: randomUUID(),
              name: input.jobName,
              timestamp: new Date(),
              payload:
                authoritativeEnvelope satisfies AnalyticsIntegrationExecutionEnvelope,
            },
            {
              jobId: queueJobId,
              removeOnFail: true,
            },
          );
          if ((await job.getState()) === "failed") {
            await job.retry("failed");
          }
        },
      });
      if (published) scheduled += 1;
    }
  }
  return scheduled;
}
