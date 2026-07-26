import {
  analyticsEvaluationDispatchIdentity,
  parseAnalyticsEvaluationReplayCutoff,
  replayVisibleAnalyticsEvaluationOperation,
  transferSuspendedAnalyticsEvaluationDispatches,
  verifyDurableAnalyticsEvaluationBootstrap,
  type AnalyticsRuntimeAdmissionContext,
} from "@langfuse/shared/src/server";
import type { PrismaClient } from "@langfuse/shared/src/db";

import type { CanonicalIngestionArtifactStore } from "../../services/CanonicalIngestionArtifactStore";
import { analyticsEvaluationTargetsFromCanonicalBatch } from "../../services/AnalyticsWriter";

const OPERATION_PAGE_SIZE = 100;

export async function replayAnalyticsEvaluationCutoff(input: {
  readonly client: PrismaClient;
  readonly artifactStore: Pick<CanonicalIngestionArtifactStore, "get">;
  readonly admissionContext: AnalyticsRuntimeAdmissionContext;
  readonly expectedCutoffDigest: string;
  readonly now?: () => Date;
}): Promise<{
  readonly operationsScanned: number;
  readonly targetsVerified: number;
  readonly bootstrapEvidenceDigest: string;
}> {
  const activation =
    await input.client.analyticsCapabilityActivation.findUniqueOrThrow({
      where: { capability: "EVALUATIONS" },
    });
  const cutoff = parseAnalyticsEvaluationReplayCutoff(activation.cutoffState);
  if (
    activation.status !== "DARK" ||
    !activation.captureEnabled ||
    !activation.rescanRequired ||
    !activation.captureRequired ||
    activation.cutoffDigest !== input.expectedCutoffDigest ||
    cutoff.captureHandoffAcceptanceSequence === undefined
  ) {
    throw new Error("Evaluation replay cutoff is not ready");
  }
  const lowerAcceptanceSequence = BigInt(cutoff.lowerAcceptanceSequence);
  const upperAcceptanceSequence = BigInt(
    cutoff.captureHandoffAcceptanceSequence,
  );
  if (upperAcceptanceSequence < lowerAcceptanceSequence) {
    throw new Error("Evaluation replay cutoff sequence is invalid");
  }

  await transferSuspendedAnalyticsEvaluationDispatches({
    client: input.client,
    admissionContext: input.admissionContext,
    expectedCutoffDigest: input.expectedCutoffDigest,
    now: input.now?.(),
  });

  const projects = await input.client.jobConfiguration.findMany({
    where: {
      jobType: "EVAL",
      status: "ACTIVE",
      evalTemplateId: { not: null },
    },
    select: { projectId: true },
    distinct: ["projectId"],
  });
  const projectIds = projects.map(({ projectId }) => projectId);
  let cursor: { acceptanceSequence: bigint; id: string } | undefined;
  let operationsScanned = 0;
  let targetsVerified = 0;

  if (projectIds.length > 0) {
    do {
      const page = await input.client.analyticsIngestionOperation.findMany({
        where: {
          projectId: { in: projectIds },
          analyticsBackend: "DORIS",
          deploymentGeneration: activation.deploymentGeneration,
          status: "VISIBLE",
          acceptanceSequence: {
            gt: lowerAcceptanceSequence,
            lte: upperAcceptanceSequence,
          },
          canonicalObjectKey: { not: null },
          canonicalArtifactChecksum: { not: null },
          ...(cursor
            ? {
                OR: [
                  { acceptanceSequence: { gt: cursor.acceptanceSequence } },
                  {
                    acceptanceSequence: cursor.acceptanceSequence,
                    id: { gt: cursor.id },
                  },
                ],
              }
            : {}),
        },
        select: {
          id: true,
          projectId: true,
          acceptanceSequence: true,
          canonicalObjectKey: true,
          canonicalArtifactChecksum: true,
          candidates: {
            select: {
              candidateKey: true,
              disposition: true,
              loadBatchId: true,
            },
          },
          loadBatches: {
            select: {
              id: true,
              status: true,
              filteredRows: true,
            },
          },
        },
        orderBy: [{ acceptanceSequence: "asc" }, { id: "asc" }],
        take: OPERATION_PAGE_SIZE,
      });
      for (const operation of page) {
        if (
          operation.acceptanceSequence === null ||
          !operation.canonicalObjectKey ||
          !operation.canonicalArtifactChecksum
        ) {
          throw new Error("Evaluation replay operation is incomplete");
        }
        const artifact = await input.artifactStore.get(
          operation.canonicalObjectKey,
          operation.canonicalArtifactChecksum,
        );
        const visibleLoadIds = new Set(
          operation.loadBatches
            .filter(
              ({ status, filteredRows }) =>
                status === "VISIBLE" && filteredRows === 0,
            )
            .map(({ id }) => id),
        );
        const visibleCandidateKeys = new Set(
          operation.candidates
            .filter(
              ({ disposition, loadBatchId }) =>
                disposition === "LOAD_REQUIRED" &&
                loadBatchId !== null &&
                visibleLoadIds.has(loadBatchId),
            )
            .map(({ candidateKey }) => candidateKey),
        );
        const targets = analyticsEvaluationTargetsFromCanonicalBatch(
          artifact,
        ).filter(({ candidateKey }) => visibleCandidateKeys.has(candidateKey));
        if (targets.length > 0) {
          await replayVisibleAnalyticsEvaluationOperation({
            client: input.client,
            admissionContext: input.admissionContext,
            operationId: operation.id,
            projectId: operation.projectId,
            targets,
            now: input.now?.(),
          });
          const expected = new Map(
            targets.map((target) => [
              `${target.targetType}\0${target.targetId}`,
              analyticsEvaluationDispatchIdentity({
                requestId: operation.id,
                targetType: target.targetType,
                targetId: target.targetId,
              }),
            ]),
          );
          const dispatches =
            await input.client.analyticsEvaluationDispatch.findMany({
              where: { id: { in: [...expected.values()] } },
              select: {
                id: true,
                operationId: true,
                projectId: true,
                status: true,
                deploymentGeneration: true,
                capabilityActivationGeneration: true,
              },
            });
          if (
            dispatches.length !== expected.size ||
            dispatches.some(
              (dispatch) =>
                dispatch.operationId !== operation.id ||
                dispatch.projectId !== operation.projectId ||
                dispatch.status !== "SUSPENDED" ||
                dispatch.deploymentGeneration !==
                  activation.deploymentGeneration ||
                dispatch.capabilityActivationGeneration !==
                  activation.generation,
            )
          ) {
            throw new Error(
              "Evaluation replay dispatch coverage is incomplete",
            );
          }
          targetsVerified += expected.size;
        }
        operationsScanned += 1;
      }
      const last = page.at(-1);
      if (page.length === OPERATION_PAGE_SIZE) {
        if (!last || last.acceptanceSequence === null) {
          throw new Error("Evaluation replay cursor is incomplete");
        }
        cursor = {
          acceptanceSequence: last.acceptanceSequence,
          id: last.id,
        };
      } else {
        cursor = undefined;
      }
    } while (cursor);
  }

  const evidence = await input.client.$transaction((transaction) =>
    verifyDurableAnalyticsEvaluationBootstrap(transaction, {
      deploymentGeneration: activation.deploymentGeneration,
      activationGeneration: activation.generation,
      expectedCutoffDigest: input.expectedCutoffDigest,
    }),
  );
  return { operationsScanned, targetsVerified, ...evidence };
}
