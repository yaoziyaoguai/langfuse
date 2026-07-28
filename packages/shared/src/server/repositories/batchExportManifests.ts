import { createHash, randomUUID } from "node:crypto";

import {
  Prisma,
  type BatchExport,
  type BatchExportDispatchOutbox,
  type PrismaClient,
} from "@prisma/client";

import type { BatchExportJobType } from "../queues";
import {
  lockAnalyticsAdmission,
  type AnalyticsRuntimeAdmissionContext,
  type AnalyticsAdmissionStamp,
} from "../analytics-persistence/analyticsBackendAdmission";

type BatchExportClient = PrismaClient;

const SHA256_HEX = /^[a-f0-9]{64}$/;

export class BatchExportManifestBusyError extends Error {
  constructor() {
    super("Batch export manifest is owned by an active claim");
    this.name = "BatchExportManifestBusyError";
  }
}

export class BatchExportManifestFencedError extends Error {
  constructor(message = "Batch export manifest claim is fenced") {
    super(message);
    this.name = "BatchExportManifestFencedError";
  }
}

export class BatchExportProvenanceError extends Error {
  constructor(message = "Batch export durable provenance is invalid") {
    super(message);
    this.name = "BatchExportProvenanceError";
  }
}

function stableJsonValue(value: unknown): unknown {
  if (value instanceof Date) return value.toISOString();
  if (typeof value === "bigint") return value.toString();
  if (Array.isArray(value)) return value.map(stableJsonValue);
  if (typeof value === "object" && value !== null) {
    return Object.fromEntries(
      Object.entries(value)
        .filter(([, child]) => child !== undefined)
        .sort(([left], [right]) => left.localeCompare(right))
        .map(([key, child]) => [key, stableJsonValue(child)]),
    );
  }
  return value;
}

export function batchExportFilterHash(input: {
  readonly projectId: string;
  readonly query: unknown;
  readonly cutoffCreatedAt: Date;
}): string {
  if (!input.projectId || Number.isNaN(input.cutoffCreatedAt.getTime())) {
    throw new TypeError("Invalid batch export filter hash input");
  }
  return createHash("sha256")
    .update(
      JSON.stringify(
        stableJsonValue({
          projectId: input.projectId,
          query: input.query,
          cutoffCreatedAt: input.cutoffCreatedAt,
        }),
      ),
    )
    .digest("hex");
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

function assertLeaseDuration(leaseMs: number): void {
  if (!Number.isSafeInteger(leaseMs) || leaseMs < 10_000) {
    throw new TypeError("Invalid batch export lease duration");
  }
}

function assertManagedDorisExport(
  row: BatchExport,
): asserts row is BatchExport & {
  analyticsBackend: "DORIS";
  deploymentGeneration: bigint;
  workloadEpochFingerprint: string;
  runtimeContractVersion: number;
  producerRuntimeLeaseId: string;
  capabilityActivationGeneration: bigint;
  capabilityContractVersion: number;
  manifestState: "PREPARING" | "SEALED";
  manifestFilterHash: string;
  executionState:
    | "PENDING"
    | "EXPORTING"
    | "COMPLETED"
    | "FAILED"
    | "CANCELLED"
    | "QUARANTINED";
} {
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
    row.manifestState === null ||
    row.manifestFilterHash === null ||
    !SHA256_HEX.test(row.manifestFilterHash) ||
    row.executionState === null
  ) {
    throw new BatchExportProvenanceError();
  }
}

function assertAdmissionMatchesExport(
  admission: AnalyticsAdmissionStamp,
  row: ReturnType<typeof managedDorisExport>,
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
    throw new BatchExportProvenanceError(
      "Batch export durable provenance is no longer current",
    );
  }
}

function managedDorisExport(row: BatchExport) {
  assertManagedDorisExport(row);
  return row;
}

function isDatasetRunItemsExport(query: unknown): boolean {
  return (
    typeof query === "object" &&
    query !== null &&
    !Array.isArray(query) &&
    "tableName" in query &&
    query.tableName === "dataset_run_items"
  );
}

function datasetRunExportProvenance(row: BatchExport): {
  readonly activationGeneration: bigint;
  readonly contractVersion: number;
} | null {
  if (!isDatasetRunItemsExport(row.query)) return null;
  if (
    row.datasetRunExportActivationGeneration === null ||
    row.datasetRunExportActivationGeneration < 1n ||
    row.datasetRunExportContractVersion === null ||
    row.datasetRunExportContractVersion < 1
  ) {
    throw new BatchExportProvenanceError(
      "Dataset-run export capability provenance is invalid",
    );
  }
  return {
    activationGeneration: row.datasetRunExportActivationGeneration,
    contractVersion: row.datasetRunExportContractVersion,
  };
}

async function lockDatasetRunExportAdmission(input: {
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
    throw new TypeError("Invalid dataset-run export admission");
  }
  return lockAnalyticsAdmission({
    transaction: input.transaction,
    runtimeLeaseId: input.admissionContext.runtimeLeaseId,
    expectedBackend: input.admissionContext.backend,
    expectedDeploymentGeneration: input.admissionContext.deploymentGeneration,
    capability: "datasetRunExports",
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

async function lockBatchExport(input: {
  readonly transaction: Prisma.TransactionClient;
  readonly projectId: string;
  readonly batchExportId: string;
}): Promise<BatchExport> {
  await input.transaction.$queryRaw(
    Prisma.sql`SELECT id FROM batch_exports WHERE id = ${input.batchExportId} AND project_id = ${input.projectId} FOR UPDATE`,
  );
  const row = await input.transaction.batchExport.findFirst({
    where: { id: input.batchExportId, projectId: input.projectId },
  });
  if (!row) throw new Error("Batch export does not exist");
  return row;
}

async function lockConsumerAdmission(input: {
  readonly transaction: Prisma.TransactionClient;
  readonly row: BatchExport;
  readonly admissionContext: AnalyticsRuntimeAdmissionContext;
  readonly action: "claimExisting" | "recovery";
  readonly now: Date;
}): Promise<ReturnType<typeof managedDorisExport>> {
  const row = managedDorisExport(input.row);
  const admission = await lockAnalyticsAdmission({
    transaction: input.transaction,
    runtimeLeaseId: input.admissionContext.runtimeLeaseId,
    expectedBackend: input.admissionContext.backend,
    expectedDeploymentGeneration: input.admissionContext.deploymentGeneration,
    capability: "coreBatchExports",
    action: input.action,
    expectedCapabilityActivationGeneration: row.capabilityActivationGeneration,
    expectedCapabilityContractVersion: row.capabilityContractVersion,
    now: input.now,
  });
  assertAdmissionMatchesExport(admission, row);
  const datasetRunProvenance = datasetRunExportProvenance(row);
  if (datasetRunProvenance) {
    await lockDatasetRunExportAdmission({
      transaction: input.transaction,
      admissionContext: input.admissionContext,
      action: input.action,
      expectedActivationGeneration: datasetRunProvenance.activationGeneration,
      expectedContractVersion: datasetRunProvenance.contractVersion,
      now: input.now,
    });
  }
  return row;
}

export async function createDorisBatchExportIntent(input: {
  readonly client: BatchExportClient;
  readonly admissionContext: AnalyticsRuntimeAdmissionContext;
  readonly projectId: string;
  readonly userId: string;
  readonly name: string;
  readonly format: string;
  readonly query: Prisma.InputJsonValue;
}): Promise<BatchExport & { dispatchOutbox: BatchExportDispatchOutbox }> {
  return input.client.$transaction(
    (transaction) =>
      createDorisBatchExportIntentInTransaction({
        transaction,
        admissionContext: input.admissionContext,
        projectId: input.projectId,
        userId: input.userId,
        name: input.name,
        format: input.format,
        query: input.query,
      }),
    { isolationLevel: "Serializable" },
  );
}

export async function createDorisBatchExportIntentInTransaction(input: {
  readonly transaction: Prisma.TransactionClient;
  readonly admissionContext: AnalyticsRuntimeAdmissionContext;
  readonly projectId: string;
  readonly userId: string;
  readonly name: string;
  readonly format: string;
  readonly query: Prisma.InputJsonValue;
}): Promise<BatchExport & { dispatchOutbox: BatchExportDispatchOutbox }> {
  if (!input.projectId || !input.userId || !input.name || !input.format) {
    throw new TypeError("Invalid Doris batch export intent");
  }
  const now = await databaseClock(input.transaction);
  const admission = await lockAnalyticsAdmission({
    transaction: input.transaction,
    runtimeLeaseId: input.admissionContext.runtimeLeaseId,
    expectedBackend: input.admissionContext.backend,
    expectedDeploymentGeneration: input.admissionContext.deploymentGeneration,
    capability: "coreBatchExports",
    action: "externalProducer",
    now,
  });
  if (
    admission.analyticsBackend !== "DORIS" ||
    admission.capabilityActivationGeneration === undefined ||
    admission.capabilityContractVersion === undefined
  ) {
    throw new BatchExportProvenanceError(
      "Doris batch export admission did not include capability provenance",
    );
  }
  const datasetRunAdmission = isDatasetRunItemsExport(input.query)
    ? await lockDatasetRunExportAdmission({
        transaction: input.transaction,
        admissionContext: input.admissionContext,
        action: "externalProducer",
        now,
      })
    : null;
  if (
    datasetRunAdmission &&
    (datasetRunAdmission.capabilityActivationGeneration === undefined ||
      datasetRunAdmission.capabilityContractVersion === undefined)
  ) {
    throw new BatchExportProvenanceError(
      "Dataset-run export admission did not include capability provenance",
    );
  }
  const filterHash = batchExportFilterHash({
    projectId: input.projectId,
    query: input.query,
    cutoffCreatedAt: now,
  });
  const created = await input.transaction.batchExport.create({
    data: {
      projectId: input.projectId,
      userId: input.userId,
      createdAt: now,
      status: "QUEUED",
      name: input.name,
      format: input.format,
      query: input.query,
      analyticsBackend: "DORIS",
      deploymentGeneration: admission.deploymentGeneration,
      workloadEpochFingerprint: admission.workloadEpochFingerprint,
      runtimeContractVersion: admission.runtimeContractVersion,
      producerRuntimeLeaseId: admission.admittingRuntimeLeaseId,
      capabilityActivationGeneration: admission.capabilityActivationGeneration,
      capabilityContractVersion: admission.capabilityContractVersion,
      datasetRunExportActivationGeneration:
        datasetRunAdmission?.capabilityActivationGeneration,
      datasetRunExportContractVersion:
        datasetRunAdmission?.capabilityContractVersion,
      manifestState: "PREPARING",
      manifestFilterHash: filterHash,
      executionState: "PENDING",
      dispatchOutbox: { create: {} },
    },
    include: { dispatchOutbox: true },
  });
  if (!created.dispatchOutbox) {
    throw new Error("Batch export dispatch outbox was not created");
  }
  return { ...created, dispatchOutbox: created.dispatchOutbox };
}

export async function findPendingBatchExportDispatchIds(input: {
  readonly client: BatchExportClient;
  readonly now: Date;
  readonly limit: number;
}): Promise<readonly { batchExportId: string; generation: number }[]> {
  if (
    Number.isNaN(input.now.getTime()) ||
    !Number.isSafeInteger(input.limit) ||
    input.limit < 1 ||
    input.limit > 100
  ) {
    throw new TypeError("Invalid batch export dispatch scan");
  }
  return input.client.batchExportDispatchOutbox.findMany({
    where: { status: "PENDING", nextAttemptAt: { lte: input.now } },
    select: { batchExportId: true, generation: true },
    orderBy: [{ nextAttemptAt: "asc" }, { createdAt: "asc" }],
    take: input.limit,
  });
}

export async function deferBatchExportDispatch(input: {
  readonly client: BatchExportClient;
  readonly batchExportId: string;
  readonly expectedGeneration: number;
}): Promise<boolean> {
  if (
    !input.batchExportId ||
    !Number.isSafeInteger(input.expectedGeneration) ||
    input.expectedGeneration < 1
  ) {
    throw new TypeError("Invalid batch export dispatch deferral");
  }
  return input.client.$transaction(async (transaction) => {
    await transaction.$queryRaw(
      Prisma.sql`SELECT id FROM batch_export_dispatch_outbox WHERE batch_export_id = ${input.batchExportId} FOR UPDATE`,
    );
    const outbox = await transaction.batchExportDispatchOutbox.findUnique({
      where: { batchExportId: input.batchExportId },
    });
    if (
      !outbox ||
      outbox.status !== "PENDING" ||
      outbox.generation !== input.expectedGeneration
    ) {
      return false;
    }
    const now = await databaseClock(transaction);
    const attempts = outbox.attempts + 1;
    const delayMs = Math.min(
      10 * 60_000,
      10_000 * 2 ** Math.min(attempts - 1, 6),
    );
    const updated = await transaction.batchExportDispatchOutbox.updateMany({
      where: {
        id: outbox.id,
        status: "PENDING",
        generation: input.expectedGeneration,
      },
      data: {
        attempts,
        nextAttemptAt: new Date(now.getTime() + delayMs),
        lockedBy: null,
        lockedUntil: null,
      },
    });
    return updated.count === 1;
  });
}

export async function publishBatchExportDispatch(input: {
  readonly client: BatchExportClient;
  readonly admissionContext: AnalyticsRuntimeAdmissionContext;
  readonly action: "externalProducer" | "recovery";
  readonly batchExportId: string;
  readonly expectedGeneration: number;
  readonly publish: (job: BatchExportJobType) => Promise<void>;
}): Promise<boolean> {
  if (
    !input.batchExportId ||
    !Number.isSafeInteger(input.expectedGeneration) ||
    input.expectedGeneration < 1
  ) {
    throw new TypeError("Invalid batch export dispatch publication");
  }
  return input.client.$transaction(
    async (transaction) => {
      await transaction.$queryRaw(
        Prisma.sql`SELECT id FROM batch_export_dispatch_outbox WHERE batch_export_id = ${input.batchExportId} FOR UPDATE`,
      );
      const outbox =
        await transaction.batchExportDispatchOutbox.findUniqueOrThrow({
          where: { batchExportId: input.batchExportId },
          include: { batchExport: true },
        });
      if (outbox.generation !== input.expectedGeneration) {
        throw new BatchExportProvenanceError(
          "Batch export dispatch generation is stale",
        );
      }
      if (outbox.status !== "PENDING") return false;

      const row = managedDorisExport(
        await lockBatchExport({
          transaction,
          projectId: outbox.batchExport.projectId,
          batchExportId: outbox.batchExportId,
        }),
      );
      if (row.status === "CANCELLED" || row.executionState === "CANCELLED") {
        await transaction.batchExportDispatchOutbox.update({
          where: { id: outbox.id },
          data: {
            status: "CANCELLED",
            lockedBy: null,
            lockedUntil: null,
          },
        });
        return false;
      }
      const admission = await lockAnalyticsAdmission({
        transaction,
        runtimeLeaseId: input.admissionContext.runtimeLeaseId,
        expectedBackend: input.admissionContext.backend,
        expectedDeploymentGeneration:
          input.admissionContext.deploymentGeneration,
        capability: "coreBatchExports",
        action: input.action,
        ...(input.action === "recovery"
          ? {
              expectedCapabilityActivationGeneration:
                row.capabilityActivationGeneration,
              expectedCapabilityContractVersion: row.capabilityContractVersion,
            }
          : {}),
      });
      assertAdmissionMatchesExport(admission, row);
      const datasetRunProvenance = datasetRunExportProvenance(row);
      if (datasetRunProvenance) {
        const datasetRunAdmission = await lockDatasetRunExportAdmission({
          transaction,
          admissionContext: input.admissionContext,
          action: input.action,
          ...(input.action === "recovery"
            ? {
                expectedActivationGeneration:
                  datasetRunProvenance.activationGeneration,
                expectedContractVersion: datasetRunProvenance.contractVersion,
              }
            : {}),
        });
        if (
          datasetRunAdmission.capabilityActivationGeneration !==
            datasetRunProvenance.activationGeneration ||
          datasetRunAdmission.capabilityContractVersion !==
            datasetRunProvenance.contractVersion
        ) {
          throw new BatchExportProvenanceError(
            "Dataset-run export durable provenance is no longer current",
          );
        }
      }

      await input.publish(
        buildManagedBatchExportJob({
          batchExport: row,
          dispatchGeneration: outbox.generation,
        }),
      );
      const updated = await transaction.batchExportDispatchOutbox.updateMany({
        where: {
          id: outbox.id,
          generation: outbox.generation,
          status: "PENDING",
        },
        data: {
          status: "PUBLISHED",
          publishedAt: await databaseClock(transaction),
          attempts: { increment: 1 },
          lockedBy: null,
          lockedUntil: null,
        },
      });
      if (updated.count !== 1) {
        throw new BatchExportProvenanceError(
          "Batch export dispatch publication was fenced",
        );
      }
      return true;
    },
    { timeout: 30_000 },
  );
}

export type BatchExportManifestClaim = {
  readonly batchExport: ReturnType<typeof managedDorisExport>;
  readonly claimId: string;
  readonly generation: bigint;
  readonly leaseExpiresAt: Date;
};

export async function renewBatchExportManifestLease(input: {
  readonly client: BatchExportClient;
  readonly admissionContext: AnalyticsRuntimeAdmissionContext;
  readonly projectId: string;
  readonly batchExportId: string;
  readonly claimId: string;
  readonly generation: bigint;
  readonly leaseOwner: string;
  readonly leaseMs: number;
}): Promise<Date> {
  assertLeaseDuration(input.leaseMs);
  return input.client.$transaction(async (transaction) => {
    const now = await databaseClock(transaction);
    const locked = await lockBatchExport({ ...input, transaction });
    const row = await lockConsumerAdmission({
      transaction,
      row: locked,
      admissionContext: input.admissionContext,
      action: "claimExisting",
      now,
    });
    if (
      row.manifestState !== "PREPARING" ||
      row.manifestGeneration !== input.generation ||
      row.manifestClaimId !== input.claimId ||
      row.manifestLeaseOwner !== input.leaseOwner ||
      !row.manifestLeaseExpiresAt ||
      row.manifestLeaseExpiresAt <= now ||
      row.status === "CANCELLED" ||
      row.executionState === "CANCELLED"
    ) {
      throw new BatchExportManifestFencedError();
    }
    const leaseExpiresAt = new Date(now.getTime() + input.leaseMs);
    await transaction.batchExport.update({
      where: { id: row.id, projectId: row.projectId },
      data: { manifestLeaseExpiresAt: leaseExpiresAt },
    });
    return leaseExpiresAt;
  });
}

export async function claimBatchExportManifest(input: {
  readonly client: BatchExportClient;
  readonly admissionContext: AnalyticsRuntimeAdmissionContext;
  readonly projectId: string;
  readonly batchExportId: string;
  readonly leaseOwner: string;
  readonly leaseMs: number;
  readonly claimId?: string;
}): Promise<BatchExportManifestClaim | { readonly sealed: BatchExport }> {
  assertLeaseDuration(input.leaseMs);
  if (!input.leaseOwner) throw new TypeError("Invalid manifest lease owner");
  return input.client.$transaction(async (transaction) => {
    const now = await databaseClock(transaction);
    const locked = await lockBatchExport({ ...input, transaction });
    const row = await lockConsumerAdmission({
      transaction,
      row: locked,
      admissionContext: input.admissionContext,
      action: "claimExisting",
      now,
    });
    if (row.manifestState === "SEALED") return { sealed: row };
    if (row.status === "CANCELLED" || row.executionState === "CANCELLED") {
      throw new BatchExportManifestFencedError("Batch export was cancelled");
    }
    if (
      row.manifestLeaseExpiresAt &&
      row.manifestLeaseExpiresAt > now &&
      row.manifestLeaseOwner !== input.leaseOwner
    ) {
      throw new BatchExportManifestBusyError();
    }
    const claimId = input.claimId ?? randomUUID();
    if (!claimId) throw new TypeError("Invalid manifest claim id");
    const generation = row.manifestGeneration + 1n;
    const leaseExpiresAt = new Date(now.getTime() + input.leaseMs);
    const batchExport = await transaction.batchExport.update({
      where: { id: row.id, projectId: row.projectId },
      data: {
        status: "QUEUED",
        executionState: "PENDING",
        failureCode: null,
        log: null,
        finishedAt: null,
        manifestGeneration: generation,
        manifestClaimId: claimId,
        manifestLeaseOwner: input.leaseOwner,
        manifestLeaseExpiresAt: leaseExpiresAt,
      },
    });
    return {
      batchExport: managedDorisExport(batchExport),
      claimId,
      generation,
      leaseExpiresAt,
    };
  });
}

export async function failBatchExportManifest(input: {
  readonly client: BatchExportClient;
  readonly admissionContext: AnalyticsRuntimeAdmissionContext;
  readonly projectId: string;
  readonly batchExportId: string;
  readonly claimId: string;
  readonly generation: bigint;
  readonly leaseOwner: string;
  readonly failureCode: string;
  readonly log: string;
}): Promise<boolean> {
  if (
    !input.claimId ||
    input.generation < 1n ||
    !input.leaseOwner ||
    !input.failureCode ||
    !input.log
  ) {
    throw new TypeError("Invalid batch export manifest failure");
  }
  return input.client.$transaction(async (transaction) => {
    const now = await databaseClock(transaction);
    const locked = await lockBatchExport({ ...input, transaction });
    const row = await lockConsumerAdmission({
      transaction,
      row: locked,
      admissionContext: input.admissionContext,
      action: "claimExisting",
      now,
    });
    if (
      row.manifestState !== "PREPARING" ||
      row.manifestGeneration !== input.generation ||
      row.manifestClaimId !== input.claimId ||
      row.manifestLeaseOwner !== input.leaseOwner ||
      !row.manifestLeaseExpiresAt ||
      row.manifestLeaseExpiresAt <= now ||
      row.status === "CANCELLED" ||
      row.executionState === "CANCELLED"
    ) {
      return false;
    }
    await transaction.batchExport.update({
      where: { id: row.id, projectId: row.projectId },
      data: {
        status: "FAILED",
        executionState: "FAILED",
        failureCode: input.failureCode.slice(0, 100),
        log: input.log.slice(0, 2_000),
        finishedAt: now,
        manifestLeaseOwner: null,
        manifestLeaseExpiresAt: null,
      },
    });
    return true;
  });
}

export function batchExportManifestAttemptObjectKey(input: {
  readonly prefix?: string;
  readonly batchExportId: string;
  readonly generation: bigint;
  readonly claimId: string;
}): string {
  if (
    !input.batchExportId ||
    input.generation < 1n ||
    !/^[A-Za-z0-9_-]+$/.test(input.claimId) ||
    (input.prefix?.includes("..") ?? false)
  ) {
    throw new TypeError("Invalid batch export manifest object identity");
  }
  const prefix = input.prefix ? `${input.prefix.replace(/\/+$/, "")}/` : "";
  return `${prefix}batch-export-manifests/${input.batchExportId}/${input.generation}/${input.claimId}.ndjson.gz.b64`;
}

export async function sealBatchExportManifest(input: {
  readonly client: BatchExportClient;
  readonly admissionContext: AnalyticsRuntimeAdmissionContext;
  readonly projectId: string;
  readonly batchExportId: string;
  readonly claimId: string;
  readonly generation: bigint;
  readonly objectKey: string;
  readonly checksum: string;
  readonly rowCount: number;
  readonly byteCount: bigint;
  readonly formatVersion: number;
}): Promise<BatchExport> {
  if (
    !input.claimId ||
    input.generation < 1n ||
    !input.objectKey ||
    !SHA256_HEX.test(input.checksum) ||
    !Number.isSafeInteger(input.rowCount) ||
    input.rowCount < 0 ||
    input.byteCount < 1n ||
    !Number.isSafeInteger(input.formatVersion) ||
    input.formatVersion < 1
  ) {
    throw new TypeError("Invalid batch export manifest descriptor");
  }
  return input.client.$transaction(async (transaction) => {
    const now = await databaseClock(transaction);
    const locked = await lockBatchExport({ ...input, transaction });
    const row = await lockConsumerAdmission({
      transaction,
      row: locked,
      admissionContext: input.admissionContext,
      action: "claimExisting",
      now,
    });
    if (row.manifestState === "SEALED") {
      if (
        row.manifestGeneration === input.generation &&
        row.manifestClaimId === input.claimId &&
        row.manifestObjectKey === input.objectKey &&
        row.manifestChecksum === input.checksum &&
        row.manifestRowCount === input.rowCount &&
        row.manifestByteCount === input.byteCount &&
        row.manifestFormatVersion === input.formatVersion
      ) {
        return row;
      }
      throw new BatchExportManifestFencedError();
    }
    if (
      row.manifestGeneration !== input.generation ||
      row.manifestClaimId !== input.claimId ||
      !row.manifestLeaseExpiresAt ||
      row.manifestLeaseExpiresAt <= now
    ) {
      throw new BatchExportManifestFencedError();
    }
    return transaction.batchExport.update({
      where: { id: row.id, projectId: row.projectId },
      data: {
        manifestState: "SEALED",
        manifestObjectKey: input.objectKey,
        manifestChecksum: input.checksum,
        manifestRowCount: input.rowCount,
        manifestByteCount: input.byteCount,
        manifestFormatVersion: input.formatVersion,
        manifestSealedAt: now,
        manifestLeaseOwner: null,
        manifestLeaseExpiresAt: null,
      },
    });
  });
}

export function buildManagedBatchExportJob(input: {
  readonly batchExport: BatchExport;
  readonly dispatchGeneration: number;
}): BatchExportJobType {
  const row = managedDorisExport(input.batchExport);
  if (
    !Number.isSafeInteger(input.dispatchGeneration) ||
    input.dispatchGeneration < 1
  ) {
    throw new TypeError("Invalid batch export dispatch generation");
  }
  return {
    projectId: row.projectId,
    batchExportId: row.id,
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

type ManagedBatchExportJob = Extract<
  BatchExportJobType,
  { readonly analyticsBackend: "DORIS" }
>;

export function isManagedBatchExportJob(
  job: BatchExportJobType,
): job is ManagedBatchExportJob {
  return "analyticsBackend" in job;
}

export function assertManagedBatchExportJobMatches(input: {
  readonly batchExport: BatchExport;
  readonly job: BatchExportJobType;
  readonly dispatchGeneration: number;
}): asserts input is {
  readonly batchExport: ReturnType<typeof managedDorisExport>;
  readonly job: ManagedBatchExportJob;
  readonly dispatchGeneration: number;
} {
  const row = managedDorisExport(input.batchExport);
  if (
    !isManagedBatchExportJob(input.job) ||
    input.job.projectId !== row.projectId ||
    input.job.batchExportId !== row.id ||
    input.job.dispatchGeneration !== input.dispatchGeneration ||
    input.job.analyticsBackend !== row.analyticsBackend ||
    BigInt(input.job.deploymentGeneration) !== row.deploymentGeneration ||
    input.job.workloadEpochFingerprint !== row.workloadEpochFingerprint ||
    input.job.runtimeContractVersion !== row.runtimeContractVersion ||
    BigInt(input.job.capabilityActivationGeneration) !==
      row.capabilityActivationGeneration ||
    input.job.capabilityContractVersion !== row.capabilityContractVersion
  ) {
    throw new BatchExportProvenanceError(
      "Batch export queue provenance does not match its durable row",
    );
  }
}

export type BatchExportExecutionClaim = {
  readonly batchExport: ReturnType<typeof managedDorisExport> & {
    readonly manifestState: "SEALED";
    readonly manifestObjectKey: string;
    readonly manifestChecksum: string;
    readonly manifestRowCount: number;
    readonly manifestByteCount: bigint;
    readonly manifestFormatVersion: number;
    readonly manifestClaimId: string;
  };
  readonly claimId: string;
  readonly generation: bigint;
  readonly leaseExpiresAt: Date;
};

export async function renewBatchExportExecutionLease(input: {
  readonly client: BatchExportClient;
  readonly admissionContext: AnalyticsRuntimeAdmissionContext;
  readonly projectId: string;
  readonly batchExportId: string;
  readonly claimId: string;
  readonly generation: bigint;
  readonly leaseOwner: string;
  readonly leaseMs: number;
}): Promise<Date> {
  assertLeaseDuration(input.leaseMs);
  return input.client.$transaction(async (transaction) => {
    const now = await databaseClock(transaction);
    const locked = await lockBatchExport({ ...input, transaction });
    const row = await lockConsumerAdmission({
      transaction,
      row: locked,
      admissionContext: input.admissionContext,
      action: "claimExisting",
      now,
    });
    if (
      row.executionState !== "EXPORTING" ||
      row.executionGeneration !== input.generation ||
      row.executionClaimId !== input.claimId ||
      row.executionLeaseOwner !== input.leaseOwner ||
      !row.executionLeaseExpiresAt ||
      row.executionLeaseExpiresAt <= now ||
      row.status === "CANCELLED"
    ) {
      throw new BatchExportManifestFencedError(
        "Batch export execution claim is fenced",
      );
    }
    const leaseExpiresAt = new Date(now.getTime() + input.leaseMs);
    await transaction.batchExport.update({
      where: { id: row.id, projectId: row.projectId },
      data: { executionLeaseExpiresAt: leaseExpiresAt },
    });
    return leaseExpiresAt;
  });
}

function sealedBatchExport(
  row: ReturnType<typeof managedDorisExport>,
): BatchExportExecutionClaim["batchExport"] {
  if (
    row.manifestState !== "SEALED" ||
    !row.manifestObjectKey ||
    !row.manifestChecksum ||
    row.manifestRowCount === null ||
    row.manifestByteCount === null ||
    row.manifestFormatVersion === null ||
    !row.manifestClaimId
  ) {
    throw new BatchExportManifestFencedError(
      "Batch export manifest is not sealed",
    );
  }
  return row as BatchExportExecutionClaim["batchExport"];
}

export async function claimBatchExportExecution(input: {
  readonly client: BatchExportClient;
  readonly admissionContext: AnalyticsRuntimeAdmissionContext;
  readonly projectId: string;
  readonly batchExportId: string;
  readonly leaseOwner: string;
  readonly leaseMs: number;
  readonly claimId?: string;
}): Promise<BatchExportExecutionClaim | { readonly completed: BatchExport }> {
  assertLeaseDuration(input.leaseMs);
  if (!input.leaseOwner) throw new TypeError("Invalid execution lease owner");
  return input.client.$transaction(async (transaction) => {
    const now = await databaseClock(transaction);
    const locked = await lockBatchExport({ ...input, transaction });
    const row = await lockConsumerAdmission({
      transaction,
      row: locked,
      admissionContext: input.admissionContext,
      action: "claimExisting",
      now,
    });
    if (row.executionState === "COMPLETED") return { completed: row };
    if (row.status === "CANCELLED" || row.executionState === "CANCELLED") {
      throw new BatchExportManifestFencedError("Batch export was cancelled");
    }
    sealedBatchExport(row);
    if (
      row.executionState === "EXPORTING" &&
      row.executionLeaseExpiresAt &&
      row.executionLeaseExpiresAt > now &&
      row.executionLeaseOwner !== input.leaseOwner
    ) {
      throw new BatchExportManifestBusyError();
    }
    const claimId = input.claimId ?? randomUUID();
    if (!claimId) throw new TypeError("Invalid execution claim id");
    const generation = row.executionGeneration + 1n;
    const leaseExpiresAt = new Date(now.getTime() + input.leaseMs);
    const updated = await transaction.batchExport.update({
      where: { id: row.id, projectId: row.projectId },
      data: {
        status: "PROCESSING",
        executionState: "EXPORTING",
        failureCode: null,
        log: null,
        finishedAt: null,
        executionGeneration: generation,
        executionClaimId: claimId,
        executionLeaseOwner: input.leaseOwner,
        executionLeaseExpiresAt: leaseExpiresAt,
      },
    });
    return {
      batchExport: sealedBatchExport(managedDorisExport(updated)),
      claimId,
      generation,
      leaseExpiresAt,
    };
  });
}

export async function completeBatchExportExecution(input: {
  readonly client: BatchExportClient;
  readonly admissionContext: AnalyticsRuntimeAdmissionContext;
  readonly projectId: string;
  readonly batchExportId: string;
  readonly claimId: string;
  readonly generation: bigint;
  readonly url: string;
  readonly expiresAt: Date;
}): Promise<BatchExport> {
  if (
    !input.claimId ||
    input.generation < 1n ||
    !input.url ||
    Number.isNaN(input.expiresAt.getTime())
  ) {
    throw new TypeError("Invalid batch export execution completion");
  }
  return input.client.$transaction(async (transaction) => {
    const now = await databaseClock(transaction);
    const locked = await lockBatchExport({ ...input, transaction });
    const row = await lockConsumerAdmission({
      transaction,
      row: locked,
      admissionContext: input.admissionContext,
      action: "claimExisting",
      now,
    });
    if (
      row.executionState === "COMPLETED" &&
      row.executionGeneration === input.generation &&
      row.executionClaimId === input.claimId &&
      row.url === input.url
    ) {
      return row;
    }
    if (
      row.executionState !== "EXPORTING" ||
      row.executionGeneration !== input.generation ||
      row.executionClaimId !== input.claimId ||
      !row.executionLeaseExpiresAt ||
      row.executionLeaseExpiresAt <= now
    ) {
      throw new BatchExportManifestFencedError(
        "Batch export execution claim is fenced",
      );
    }
    return transaction.batchExport.update({
      where: { id: row.id, projectId: row.projectId },
      data: {
        status: "COMPLETED",
        executionState: "COMPLETED",
        url: input.url,
        finishedAt: now,
        expiresAt: input.expiresAt,
        executionLeaseOwner: null,
        executionLeaseExpiresAt: null,
      },
    });
  });
}

export async function quarantineBatchExport(input: {
  readonly client: BatchExportClient;
  readonly projectId: string;
  readonly batchExportId: string;
  readonly failureCode: string;
  readonly log: string;
}): Promise<void> {
  if (!input.failureCode || !input.log) {
    throw new TypeError("Invalid batch export quarantine reason");
  }
  await input.client.batchExport.updateMany({
    where: {
      id: input.batchExportId,
      projectId: input.projectId,
      OR: [
        { executionState: null },
        { executionState: { notIn: ["COMPLETED", "CANCELLED"] } },
      ],
    },
    data: {
      status: "FAILED",
      executionState: "QUARANTINED",
      failureCode: input.failureCode.slice(0, 100),
      log: input.log.slice(0, 2_000),
      finishedAt: new Date(),
      executionLeaseOwner: null,
      executionLeaseExpiresAt: null,
    },
  });
}

export async function quarantineBatchExportDispatch(input: {
  readonly client: BatchExportClient;
  readonly batchExportId: string;
  readonly failureCode: string;
  readonly log: string;
}): Promise<void> {
  if (!input.batchExportId || !input.failureCode || !input.log) {
    throw new TypeError("Invalid batch export dispatch quarantine reason");
  }
  await input.client.$transaction(async (transaction) => {
    await transaction.$queryRaw(
      Prisma.sql`SELECT id FROM batch_export_dispatch_outbox WHERE batch_export_id = ${input.batchExportId} FOR UPDATE`,
    );
    const outbox =
      await transaction.batchExportDispatchOutbox.findUniqueOrThrow({
        where: { batchExportId: input.batchExportId },
        include: { batchExport: true },
      });
    await transaction.batchExport.updateMany({
      where: {
        id: outbox.batchExportId,
        projectId: outbox.batchExport.projectId,
        OR: [
          { executionState: null },
          { executionState: { notIn: ["COMPLETED", "CANCELLED"] } },
        ],
      },
      data: {
        status: "FAILED",
        executionState: "QUARANTINED",
        failureCode: input.failureCode.slice(0, 100),
        log: input.log.slice(0, 2_000),
        finishedAt: await databaseClock(transaction),
        manifestLeaseOwner: null,
        manifestLeaseExpiresAt: null,
        executionLeaseOwner: null,
        executionLeaseExpiresAt: null,
      },
    });
    await transaction.batchExportDispatchOutbox.update({
      where: { id: outbox.id },
      data: {
        status: "FAILED",
        attempts: { increment: 1 },
        lockedBy: null,
        lockedUntil: null,
      },
    });
  });
}

export async function failBatchExportExecution(input: {
  readonly client: BatchExportClient;
  readonly admissionContext: AnalyticsRuntimeAdmissionContext;
  readonly projectId: string;
  readonly batchExportId: string;
  readonly executionClaimId: string;
  readonly executionGeneration: bigint;
  readonly failureCode: string;
  readonly log: string;
}): Promise<boolean> {
  if (
    !input.failureCode ||
    !input.log ||
    !input.executionClaimId ||
    input.executionGeneration < 1n
  ) {
    throw new TypeError("Invalid batch export failure reason");
  }
  return input.client.$transaction(async (transaction) => {
    const now = await databaseClock(transaction);
    const locked = await lockBatchExport({ ...input, transaction });
    const row = await lockConsumerAdmission({
      transaction,
      row: locked,
      admissionContext: input.admissionContext,
      action: "claimExisting",
      now,
    });
    if (
      row.executionState !== "EXPORTING" ||
      row.executionClaimId !== input.executionClaimId ||
      row.executionGeneration !== input.executionGeneration ||
      !row.executionLeaseExpiresAt ||
      row.executionLeaseExpiresAt <= now
    ) {
      return false;
    }
    await transaction.batchExport.update({
      where: { id: row.id, projectId: row.projectId },
      data: {
        status: "FAILED",
        executionState: "FAILED",
        failureCode: input.failureCode.slice(0, 100),
        log: input.log.slice(0, 2_000),
        finishedAt: now,
        executionLeaseOwner: null,
        executionLeaseExpiresAt: null,
      },
    });
    return true;
  });
}

export async function cancelBatchExport(input: {
  readonly client: BatchExportClient;
  readonly projectId: string;
  readonly batchExportId: string;
}): Promise<BatchExport> {
  return input.client.$transaction(async (transaction) => {
    await transaction.$queryRaw(
      Prisma.sql`SELECT id FROM batch_export_dispatch_outbox WHERE batch_export_id = ${input.batchExportId} FOR UPDATE`,
    );
    const row = await lockBatchExport({ ...input, transaction });
    const cancelled = await transaction.batchExport.update({
      where: { id: row.id, projectId: row.projectId },
      data:
        row.analyticsBackend === "DORIS"
          ? {
              status: "CANCELLED",
              executionState: "CANCELLED",
              manifestLeaseOwner: null,
              manifestLeaseExpiresAt: null,
              executionLeaseOwner: null,
              executionLeaseExpiresAt: null,
            }
          : { status: "CANCELLED" },
    });
    if (row.analyticsBackend === "DORIS") {
      await transaction.batchExportDispatchOutbox.updateMany({
        where: { batchExportId: row.id, status: "PENDING" },
        data: {
          status: "CANCELLED",
          lockedBy: null,
          lockedUntil: null,
        },
      });
    }
    return cancelled;
  });
}
