import { randomUUID } from "node:crypto";

import {
  Prisma,
  type DatasetRuns,
  type ExperimentExecutionDispatchOutbox,
  type PrismaClient,
} from "@prisma/client";

import type { ExperimentCreateEventType } from "../queues";
import {
  lockAnalyticsAdmission,
  type AnalyticsAdmissionStamp,
  type AnalyticsRuntimeAdmissionContext,
} from "../analytics-persistence/analyticsBackendAdmission";

const SHA256_HEX = /^[a-f0-9]{64}$/;

type ExperimentExecutionClient = PrismaClient;

export class ExperimentExecutionProvenanceError extends Error {
  constructor(message = "Experiment execution durable provenance is invalid") {
    super(message);
    this.name = "ExperimentExecutionProvenanceError";
  }
}

export class ExperimentExecutionBusyError extends Error {
  constructor() {
    super("Experiment execution is owned by an active worker");
    this.name = "ExperimentExecutionBusyError";
  }
}

function managedExperimentRun(row: DatasetRuns) {
  if (
    row.analyticsBackend !== "DORIS" ||
    row.deploymentGeneration === null ||
    row.deploymentGeneration < 1n ||
    row.workloadEpochFingerprint === null ||
    !SHA256_HEX.test(row.workloadEpochFingerprint) ||
    row.runtimeContractVersion === null ||
    row.runtimeContractVersion < 1 ||
    row.producerRuntimeLeaseId === null ||
    row.capabilityActivationGeneration === null ||
    row.capabilityActivationGeneration < 1n ||
    row.capabilityContractVersion === null ||
    row.capabilityContractVersion < 1 ||
    row.datasetRunIngestionActivationGeneration === null ||
    row.datasetRunIngestionActivationGeneration < 1n ||
    row.datasetRunIngestionContractVersion === null ||
    row.datasetRunIngestionContractVersion < 1 ||
    row.experimentExecutionState === null
  ) {
    throw new ExperimentExecutionProvenanceError();
  }
  return row as DatasetRuns & {
    readonly analyticsBackend: "DORIS";
    readonly deploymentGeneration: bigint;
    readonly workloadEpochFingerprint: string;
    readonly runtimeContractVersion: number;
    readonly producerRuntimeLeaseId: string;
    readonly capabilityActivationGeneration: bigint;
    readonly capabilityContractVersion: number;
    readonly datasetRunIngestionActivationGeneration: bigint;
    readonly datasetRunIngestionContractVersion: number;
    readonly experimentExecutionState:
      | "PENDING"
      | "PROCESSING"
      | "COMPLETED"
      | "FAILED"
      | "CANCELLED"
      | "QUARANTINED";
  };
}

function assertAdmissionMatches(
  admission: AnalyticsAdmissionStamp,
  row: ReturnType<typeof managedExperimentRun>,
): void {
  if (
    admission.analyticsBackend !== row.analyticsBackend ||
    admission.deploymentGeneration !== row.deploymentGeneration ||
    admission.workloadEpochFingerprint !== row.workloadEpochFingerprint ||
    admission.runtimeContractVersion !== row.runtimeContractVersion ||
    admission.capabilityActivationGeneration !==
      row.capabilityActivationGeneration ||
    admission.capabilityContractVersion !== row.capabilityContractVersion
  ) {
    throw new ExperimentExecutionProvenanceError(
      "Experiment execution provenance is no longer current",
    );
  }
}

async function databaseClock(
  transaction: Prisma.TransactionClient,
): Promise<Date> {
  const [row] = await transaction.$queryRaw<readonly { now: Date }[]>(
    Prisma.sql`SELECT clock_timestamp() AS now`,
  );
  if (!row) throw new Error("Postgres did not return its current timestamp");
  return row.now;
}

async function lockExperimentRun(input: {
  readonly transaction: Prisma.TransactionClient;
  readonly projectId: string;
  readonly runId: string;
}): Promise<DatasetRuns> {
  await input.transaction.$queryRaw(
    Prisma.sql`SELECT id FROM dataset_runs WHERE id = ${input.runId} AND project_id = ${input.projectId} FOR UPDATE`,
  );
  const row = await input.transaction.datasetRuns.findFirst({
    where: { id: input.runId, projectId: input.projectId },
  });
  if (!row) throw new Error("Experiment dataset run does not exist");
  return row;
}

async function lockConsumerAdmission(input: {
  readonly transaction: Prisma.TransactionClient;
  readonly row: DatasetRuns;
  readonly admissionContext: AnalyticsRuntimeAdmissionContext;
  readonly action: "claimExisting" | "recovery";
  readonly now?: Date;
}) {
  const row = managedExperimentRun(input.row);
  const admission = await lockAnalyticsAdmission({
    transaction: input.transaction,
    runtimeLeaseId: input.admissionContext.runtimeLeaseId,
    expectedBackend: input.admissionContext.backend,
    expectedDeploymentGeneration: input.admissionContext.deploymentGeneration,
    capability: "experiments",
    action: input.action,
    expectedCapabilityActivationGeneration: row.capabilityActivationGeneration,
    expectedCapabilityContractVersion: row.capabilityContractVersion,
    now: input.now,
  });
  assertAdmissionMatches(admission, row);
  const datasetRunAdmission = await lockDatasetRunIngestionAdmission({
    transaction: input.transaction,
    admissionContext: input.admissionContext,
    action: input.action,
    expectedActivationGeneration: row.datasetRunIngestionActivationGeneration,
    expectedContractVersion: row.datasetRunIngestionContractVersion,
    now: input.now,
  });
  assertDatasetRunIngestionAdmissionMatches(datasetRunAdmission, row);
  return row;
}

async function lockDatasetRunIngestionAdmission(input: {
  readonly transaction: Prisma.TransactionClient;
  readonly admissionContext: AnalyticsRuntimeAdmissionContext;
  readonly action: "externalProducer" | "claimExisting" | "recovery";
  readonly expectedActivationGeneration?: bigint;
  readonly expectedContractVersion?: number;
  readonly now?: Date;
}): Promise<AnalyticsAdmissionStamp> {
  const hasExpected = input.expectedActivationGeneration !== undefined;
  if (
    hasExpected !== (input.expectedContractVersion !== undefined) ||
    (input.action === "externalProducer") === hasExpected
  ) {
    throw new TypeError("Invalid dataset-run ingestion admission");
  }
  return lockAnalyticsAdmission({
    transaction: input.transaction,
    runtimeLeaseId: input.admissionContext.runtimeLeaseId,
    expectedBackend: input.admissionContext.backend,
    expectedDeploymentGeneration: input.admissionContext.deploymentGeneration,
    capability: "datasetRunIngestion",
    action: input.action,
    ...(hasExpected
      ? {
          expectedCapabilityActivationGeneration:
            input.expectedActivationGeneration,
          expectedCapabilityContractVersion: input.expectedContractVersion,
        }
      : {}),
    now: input.now,
  });
}

function assertDatasetRunIngestionAdmissionMatches(
  admission: AnalyticsAdmissionStamp,
  row: ReturnType<typeof managedExperimentRun>,
): void {
  if (
    admission.capabilityActivationGeneration !==
      row.datasetRunIngestionActivationGeneration ||
    admission.capabilityContractVersion !==
      row.datasetRunIngestionContractVersion
  ) {
    throw new ExperimentExecutionProvenanceError(
      "Dataset-run ingestion provenance is no longer current",
    );
  }
}

export async function createDorisExperimentExecutionIntent(input: {
  readonly client: ExperimentExecutionClient;
  readonly admissionContext: AnalyticsRuntimeAdmissionContext;
  readonly projectId: string;
  readonly datasetId: string;
  readonly name: string;
  readonly description?: string;
  readonly metadata: Prisma.InputJsonValue;
}): Promise<
  DatasetRuns & {
    readonly experimentDispatchOutbox: ExperimentExecutionDispatchOutbox;
  }
> {
  if (!input.projectId || !input.datasetId || !input.name) {
    throw new TypeError("Invalid Doris experiment execution intent");
  }
  return input.client.$transaction(
    async (transaction) => {
      const now = await databaseClock(transaction);
      const admission = await lockAnalyticsAdmission({
        transaction,
        runtimeLeaseId: input.admissionContext.runtimeLeaseId,
        expectedBackend: input.admissionContext.backend,
        expectedDeploymentGeneration:
          input.admissionContext.deploymentGeneration,
        capability: "experiments",
        action: "externalProducer",
        now,
      });
      if (
        admission.analyticsBackend !== "DORIS" ||
        admission.capabilityActivationGeneration === undefined ||
        admission.capabilityContractVersion === undefined
      ) {
        throw new ExperimentExecutionProvenanceError(
          "Doris experiment admission did not include capability provenance",
        );
      }
      const datasetRunAdmission = await lockDatasetRunIngestionAdmission({
        transaction,
        admissionContext: input.admissionContext,
        action: "externalProducer",
        now,
      });
      if (
        datasetRunAdmission.capabilityActivationGeneration === undefined ||
        datasetRunAdmission.capabilityContractVersion === undefined
      ) {
        throw new ExperimentExecutionProvenanceError(
          "Dataset-run ingestion admission did not include capability provenance",
        );
      }
      const created = await transaction.datasetRuns.create({
        data: {
          name: input.name,
          description: input.description,
          datasetId: input.datasetId,
          metadata: input.metadata,
          projectId: input.projectId,
          analyticsBackend: "DORIS",
          deploymentGeneration: admission.deploymentGeneration,
          workloadEpochFingerprint: admission.workloadEpochFingerprint,
          runtimeContractVersion: admission.runtimeContractVersion,
          producerRuntimeLeaseId: admission.admittingRuntimeLeaseId,
          capabilityActivationGeneration:
            admission.capabilityActivationGeneration,
          capabilityContractVersion: admission.capabilityContractVersion,
          datasetRunIngestionActivationGeneration:
            datasetRunAdmission.capabilityActivationGeneration,
          datasetRunIngestionContractVersion:
            datasetRunAdmission.capabilityContractVersion,
          experimentExecutionState: "PENDING",
          experimentDispatchOutbox: { create: {} },
        },
        include: { experimentDispatchOutbox: true },
      });
      if (!created.experimentDispatchOutbox) {
        throw new Error("Experiment dispatch outbox was not created");
      }
      return {
        ...created,
        experimentDispatchOutbox: created.experimentDispatchOutbox,
      };
    },
    { isolationLevel: "Serializable" },
  );
}

export function buildManagedExperimentExecutionJob(input: {
  readonly datasetRun: DatasetRuns;
  readonly dispatchGeneration: number;
}): ExperimentCreateEventType {
  const row = managedExperimentRun(input.datasetRun);
  if (
    !Number.isSafeInteger(input.dispatchGeneration) ||
    input.dispatchGeneration < 1
  ) {
    throw new TypeError("Invalid experiment dispatch generation");
  }
  return {
    projectId: row.projectId,
    datasetId: row.datasetId,
    runId: row.id,
    ...(row.description ? { description: row.description } : {}),
    dispatchGeneration: input.dispatchGeneration,
    analyticsBackend: row.analyticsBackend,
    deploymentGeneration: row.deploymentGeneration.toString(),
    workloadEpochFingerprint: row.workloadEpochFingerprint,
    runtimeContractVersion: row.runtimeContractVersion,
    capabilityActivationGeneration:
      row.capabilityActivationGeneration.toString(),
    capabilityContractVersion: row.capabilityContractVersion,
  };
}

export function isManagedExperimentExecutionJob(
  job: ExperimentCreateEventType,
): job is Extract<
  ExperimentCreateEventType,
  { readonly analyticsBackend: "DORIS" }
> {
  return "analyticsBackend" in job;
}

function assertJobMatches(input: {
  readonly row: DatasetRuns;
  readonly job: ExperimentCreateEventType;
  readonly dispatchGeneration: number;
}): void {
  const row = managedExperimentRun(input.row);
  if (
    !isManagedExperimentExecutionJob(input.job) ||
    input.job.projectId !== row.projectId ||
    input.job.datasetId !== row.datasetId ||
    input.job.runId !== row.id ||
    input.job.dispatchGeneration !== input.dispatchGeneration ||
    input.job.analyticsBackend !== row.analyticsBackend ||
    BigInt(input.job.deploymentGeneration) !== row.deploymentGeneration ||
    input.job.workloadEpochFingerprint !== row.workloadEpochFingerprint ||
    input.job.runtimeContractVersion !== row.runtimeContractVersion ||
    BigInt(input.job.capabilityActivationGeneration) !==
      row.capabilityActivationGeneration ||
    input.job.capabilityContractVersion !== row.capabilityContractVersion
  ) {
    throw new ExperimentExecutionProvenanceError(
      "Experiment queue provenance does not match its durable row",
    );
  }
}

export async function publishExperimentExecutionDispatch(input: {
  readonly client: ExperimentExecutionClient;
  readonly admissionContext: AnalyticsRuntimeAdmissionContext;
  readonly action: "externalProducer" | "recovery";
  readonly projectId: string;
  readonly runId: string;
  readonly expectedGeneration: number;
  readonly publish: (job: ExperimentCreateEventType) => Promise<void>;
}): Promise<boolean> {
  if (
    !input.projectId ||
    !input.runId ||
    !Number.isSafeInteger(input.expectedGeneration) ||
    input.expectedGeneration < 1
  ) {
    throw new TypeError("Invalid experiment dispatch publication");
  }
  return input.client.$transaction(
    async (transaction) => {
      await transaction.$queryRaw(
        Prisma.sql`SELECT id FROM experiment_execution_dispatch_outbox WHERE dataset_run_id = ${input.runId} AND project_id = ${input.projectId} FOR UPDATE`,
      );
      const outbox =
        await transaction.experimentExecutionDispatchOutbox.findUniqueOrThrow({
          where: {
            datasetRunId_projectId: {
              datasetRunId: input.runId,
              projectId: input.projectId,
            },
          },
        });
      if (outbox.generation !== input.expectedGeneration) {
        throw new ExperimentExecutionProvenanceError(
          "Experiment dispatch generation is stale",
        );
      }
      if (outbox.status !== "PENDING") return false;
      const row = managedExperimentRun(
        await lockExperimentRun({ ...input, transaction }),
      );
      const admission = await lockAnalyticsAdmission({
        transaction,
        runtimeLeaseId: input.admissionContext.runtimeLeaseId,
        expectedBackend: input.admissionContext.backend,
        expectedDeploymentGeneration:
          input.admissionContext.deploymentGeneration,
        capability: "experiments",
        action: input.action,
        ...(input.action === "recovery"
          ? {
              expectedCapabilityActivationGeneration:
                row.capabilityActivationGeneration,
              expectedCapabilityContractVersion: row.capabilityContractVersion,
            }
          : {}),
      });
      assertAdmissionMatches(admission, row);
      const datasetRunAdmission = await lockDatasetRunIngestionAdmission({
        transaction,
        admissionContext: input.admissionContext,
        action: input.action,
        ...(input.action === "recovery"
          ? {
              expectedActivationGeneration:
                row.datasetRunIngestionActivationGeneration,
              expectedContractVersion: row.datasetRunIngestionContractVersion,
            }
          : {}),
      });
      assertDatasetRunIngestionAdmissionMatches(datasetRunAdmission, row);
      if (row.experimentExecutionState === "CANCELLED") {
        await transaction.experimentExecutionDispatchOutbox.update({
          where: { id: outbox.id },
          data: { status: "CANCELLED" },
        });
        return false;
      }
      await input.publish(
        buildManagedExperimentExecutionJob({
          datasetRun: row,
          dispatchGeneration: outbox.generation,
        }),
      );
      const updated =
        await transaction.experimentExecutionDispatchOutbox.updateMany({
          where: {
            id: outbox.id,
            generation: outbox.generation,
            status: "PENDING",
          },
          data: {
            status: "PUBLISHED",
            publishedAt: await databaseClock(transaction),
            attempts: { increment: 1 },
          },
        });
      if (updated.count !== 1) {
        throw new ExperimentExecutionProvenanceError(
          "Experiment dispatch publication was fenced",
        );
      }
      return true;
    },
    { timeout: 30_000 },
  );
}

export async function findPendingExperimentExecutionDispatches(input: {
  readonly client: ExperimentExecutionClient;
  readonly now: Date;
  readonly limit: number;
}): Promise<
  readonly {
    readonly projectId: string;
    readonly runId: string;
    readonly generation: number;
  }[]
> {
  if (
    Number.isNaN(input.now.getTime()) ||
    !Number.isSafeInteger(input.limit) ||
    input.limit < 1 ||
    input.limit > 100
  ) {
    throw new TypeError("Invalid pending experiment dispatch query");
  }
  const rows = await input.client.experimentExecutionDispatchOutbox.findMany({
    where: { status: "PENDING", nextAttemptAt: { lte: input.now } },
    select: { projectId: true, datasetRunId: true, generation: true },
    orderBy: [{ nextAttemptAt: "asc" }, { createdAt: "asc" }],
    take: input.limit,
  });
  return rows.map((row) => ({
    projectId: row.projectId,
    runId: row.datasetRunId,
    generation: row.generation,
  }));
}

export async function deferExperimentExecutionDispatch(input: {
  readonly client: ExperimentExecutionClient;
  readonly projectId: string;
  readonly runId: string;
  readonly expectedGeneration: number;
}): Promise<boolean> {
  if (
    !input.projectId ||
    !input.runId ||
    !Number.isSafeInteger(input.expectedGeneration) ||
    input.expectedGeneration < 1
  ) {
    throw new TypeError("Invalid experiment dispatch deferral");
  }
  return input.client.$transaction(async (transaction) => {
    const now = await databaseClock(transaction);
    const outbox =
      await transaction.experimentExecutionDispatchOutbox.findUnique({
        where: {
          datasetRunId_projectId: {
            datasetRunId: input.runId,
            projectId: input.projectId,
          },
        },
      });
    if (
      !outbox ||
      outbox.status !== "PENDING" ||
      outbox.generation !== input.expectedGeneration
    ) {
      return false;
    }
    const attempts = outbox.attempts + 1;
    const updated =
      await transaction.experimentExecutionDispatchOutbox.updateMany({
        where: {
          id: outbox.id,
          generation: outbox.generation,
          status: "PENDING",
        },
        data: {
          attempts,
          nextAttemptAt: new Date(
            now.getTime() +
              Math.min(10 * 60_000, 10_000 * 2 ** Math.min(attempts, 6)),
          ),
        },
      });
    return updated.count === 1;
  });
}

export type ExperimentExecutionClaim = {
  readonly datasetRun: ReturnType<typeof managedExperimentRun>;
  readonly claimId: string;
  readonly generation: bigint;
  readonly leaseExpiresAt: Date;
};

export async function claimExperimentExecution(input: {
  readonly client: ExperimentExecutionClient;
  readonly admissionContext: AnalyticsRuntimeAdmissionContext;
  readonly job: ExperimentCreateEventType;
  readonly leaseOwner: string;
  readonly leaseMs: number;
}): Promise<ExperimentExecutionClaim | { readonly completed: DatasetRuns }> {
  if (
    !input.leaseOwner ||
    !Number.isSafeInteger(input.leaseMs) ||
    input.leaseMs < 10_000
  ) {
    throw new TypeError("Invalid experiment execution claim");
  }
  return input.client.$transaction(async (transaction) => {
    const now = await databaseClock(transaction);
    const locked = await lockExperimentRun({
      transaction,
      projectId: input.job.projectId,
      runId: input.job.runId,
    });
    const row = await lockConsumerAdmission({
      transaction,
      row: locked,
      admissionContext: input.admissionContext,
      action: "claimExisting",
      now,
    });
    const outbox =
      await transaction.experimentExecutionDispatchOutbox.findUniqueOrThrow({
        where: {
          datasetRunId_projectId: {
            datasetRunId: row.id,
            projectId: row.projectId,
          },
        },
      });
    assertJobMatches({
      row,
      job: input.job,
      dispatchGeneration: outbox.generation,
    });
    if (row.experimentExecutionState === "COMPLETED") {
      return { completed: row };
    }
    if (
      row.experimentExecutionState === "PROCESSING" &&
      row.experimentExecutionLeaseExpiresAt &&
      row.experimentExecutionLeaseExpiresAt > now &&
      row.experimentExecutionLeaseOwner !== input.leaseOwner
    ) {
      throw new ExperimentExecutionBusyError();
    }
    if (
      row.experimentExecutionState === "CANCELLED" ||
      row.experimentExecutionState === "QUARANTINED"
    ) {
      throw new ExperimentExecutionProvenanceError(
        "Experiment execution is terminal",
      );
    }
    const claimId = randomUUID();
    const generation = row.experimentExecutionGeneration + 1n;
    const leaseExpiresAt = new Date(now.getTime() + input.leaseMs);
    const datasetRun = await transaction.datasetRuns.update({
      where: { id_projectId: { id: row.id, projectId: row.projectId } },
      data: {
        experimentExecutionState: "PROCESSING",
        experimentExecutionGeneration: generation,
        experimentExecutionClaimId: claimId,
        experimentExecutionLeaseOwner: input.leaseOwner,
        experimentExecutionLeaseExpiresAt: leaseExpiresAt,
        experimentFailureCode: null,
        experimentCompletedAt: null,
      },
    });
    return {
      datasetRun: managedExperimentRun(datasetRun),
      claimId,
      generation,
      leaseExpiresAt,
    };
  });
}

export async function renewExperimentExecutionClaim(input: {
  readonly client: ExperimentExecutionClient;
  readonly admissionContext: AnalyticsRuntimeAdmissionContext;
  readonly projectId: string;
  readonly runId: string;
  readonly claimId: string;
  readonly generation: bigint;
  readonly leaseOwner: string;
  readonly leaseMs: number;
}): Promise<Date> {
  if (
    !input.projectId ||
    !input.runId ||
    !input.claimId ||
    !input.leaseOwner ||
    input.generation < 1n ||
    !Number.isSafeInteger(input.leaseMs) ||
    input.leaseMs < 10_000
  ) {
    throw new TypeError("Invalid experiment execution renewal");
  }
  return input.client.$transaction(async (transaction) => {
    const now = await databaseClock(transaction);
    const row = await lockConsumerAdmission({
      transaction,
      row: await lockExperimentRun({ ...input, transaction }),
      admissionContext: input.admissionContext,
      action: "claimExisting",
      now,
    });
    if (
      row.experimentExecutionState !== "PROCESSING" ||
      row.experimentExecutionGeneration !== input.generation ||
      row.experimentExecutionClaimId !== input.claimId ||
      row.experimentExecutionLeaseOwner !== input.leaseOwner ||
      !row.experimentExecutionLeaseExpiresAt ||
      row.experimentExecutionLeaseExpiresAt <= now
    ) {
      throw new ExperimentExecutionProvenanceError(
        "Experiment execution claim is fenced",
      );
    }
    const leaseExpiresAt = new Date(now.getTime() + input.leaseMs);
    await transaction.datasetRuns.update({
      where: { id_projectId: { id: row.id, projectId: row.projectId } },
      data: { experimentExecutionLeaseExpiresAt: leaseExpiresAt },
    });
    return leaseExpiresAt;
  });
}

async function finishExperimentExecution(input: {
  readonly client: ExperimentExecutionClient;
  readonly admissionContext: AnalyticsRuntimeAdmissionContext;
  readonly projectId: string;
  readonly runId: string;
  readonly claimId: string;
  readonly generation: bigint;
  readonly leaseOwner: string;
  readonly state: "COMPLETED" | "FAILED";
  readonly failureCode?: string;
}): Promise<boolean> {
  if (
    !input.projectId ||
    !input.runId ||
    !input.claimId ||
    !input.leaseOwner ||
    input.generation < 1n ||
    (input.state === "FAILED" && !input.failureCode) ||
    (input.state === "COMPLETED" && input.failureCode !== undefined)
  ) {
    throw new TypeError("Invalid experiment execution completion");
  }
  return input.client.$transaction(async (transaction) => {
    const now = await databaseClock(transaction);
    const row = await lockConsumerAdmission({
      transaction,
      row: await lockExperimentRun({ ...input, transaction }),
      admissionContext: input.admissionContext,
      action: "claimExisting",
      now,
    });
    if (
      row.experimentExecutionState !== "PROCESSING" ||
      row.experimentExecutionGeneration !== input.generation ||
      row.experimentExecutionClaimId !== input.claimId ||
      row.experimentExecutionLeaseOwner !== input.leaseOwner
    ) {
      return false;
    }
    await transaction.datasetRuns.update({
      where: { id_projectId: { id: row.id, projectId: row.projectId } },
      data: {
        experimentExecutionState: input.state,
        experimentExecutionClaimId: null,
        experimentExecutionLeaseOwner: null,
        experimentExecutionLeaseExpiresAt: null,
        experimentFailureCode: input.failureCode?.slice(0, 100) ?? null,
        experimentCompletedAt: now,
      },
    });
    return true;
  });
}

export const completeExperimentExecution = (
  input: Omit<Parameters<typeof finishExperimentExecution>[0], "state">,
) => finishExperimentExecution({ ...input, state: "COMPLETED" });

export const failExperimentExecution = (
  input: Omit<Parameters<typeof finishExperimentExecution>[0], "state">,
) => finishExperimentExecution({ ...input, state: "FAILED" });

export async function quarantineExperimentExecutionDispatch(input: {
  readonly client: ExperimentExecutionClient;
  readonly projectId: string;
  readonly runId: string;
  readonly expectedGeneration: number;
  readonly failureCode: string;
}): Promise<boolean> {
  if (
    !input.projectId ||
    !input.runId ||
    !input.failureCode ||
    !Number.isSafeInteger(input.expectedGeneration) ||
    input.expectedGeneration < 1
  ) {
    throw new TypeError("Invalid experiment dispatch quarantine");
  }
  return input.client.$transaction(async (transaction) => {
    await transaction.$queryRaw(
      Prisma.sql`SELECT id FROM experiment_execution_dispatch_outbox WHERE dataset_run_id = ${input.runId} AND project_id = ${input.projectId} FOR UPDATE`,
    );
    const outbox =
      await transaction.experimentExecutionDispatchOutbox.findUnique({
        where: {
          datasetRunId_projectId: {
            datasetRunId: input.runId,
            projectId: input.projectId,
          },
        },
      });
    if (
      !outbox ||
      outbox.generation !== input.expectedGeneration ||
      outbox.status !== "PENDING"
    ) {
      return false;
    }
    const now = await databaseClock(transaction);
    await transaction.datasetRuns.update({
      where: {
        id_projectId: { id: input.runId, projectId: input.projectId },
      },
      data: {
        experimentExecutionState: "QUARANTINED",
        experimentFailureCode: input.failureCode.slice(0, 100),
        experimentCompletedAt: now,
      },
    });
    await transaction.experimentExecutionDispatchOutbox.update({
      where: { id: outbox.id },
      data: { status: "CANCELLED" },
    });
    return true;
  });
}
