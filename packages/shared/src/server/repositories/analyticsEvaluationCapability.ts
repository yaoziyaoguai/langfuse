import { createHash } from "node:crypto";

import { Prisma } from "@prisma/client";
import type {
  AnalyticsCapabilityActivation,
  PrismaClient,
} from "@prisma/client";

import { prisma } from "../../db";
import {
  lockAnalyticsAdmission,
  type AnalyticsRuntimeAdmissionContext,
} from "../analytics-persistence/analyticsBackendAdmission";

const DECIMAL_BIGINT = /^(0|[1-9][0-9]*)$/;
const SHA256_HEX = /^[a-f0-9]{64}$/;

type EvaluationConfigurationSnapshot = {
  readonly id: string;
  readonly projectId: string;
  readonly evalTemplateId: string;
  readonly targetObject: string;
  readonly updatedAt: string;
};

export type AnalyticsEvaluationReplayCutoff = {
  readonly kind: "evaluation_operation_cutoff";
  readonly version: 1;
  readonly deploymentGeneration: string;
  readonly sourceActivationGeneration: string;
  readonly configurationDigest: string;
  readonly configurationCount: number;
  readonly lowerAcceptanceSequence: string;
  readonly captureHandoffAcceptanceSequence?: string;
  readonly sealedAt: string;
  readonly captureHandoffAt?: string;
};

function stableJson(value: unknown): string {
  if (value === null) return "null";
  if (typeof value === "string" || typeof value === "boolean") {
    return JSON.stringify(value);
  }
  if (typeof value === "number") {
    if (!Number.isFinite(value)) throw new TypeError("Invalid cutoff number");
    return JSON.stringify(value);
  }
  if (Array.isArray(value)) {
    return `[${value.map(stableJson).join(",")}]`;
  }
  if (typeof value === "object") {
    const record = value as Record<string, unknown>;
    return `{${Object.keys(record)
      .sort()
      .map((key) => `${JSON.stringify(key)}:${stableJson(record[key])}`)
      .join(",")}}`;
  }
  throw new TypeError("Invalid replay cutoff value");
}

export function digestAnalyticsEvaluationReplayState(value: unknown): string {
  return createHash("sha256").update(stableJson(value), "utf8").digest("hex");
}

function parseIsoDate(value: unknown, field: string): string {
  if (
    typeof value !== "string" ||
    !Number.isFinite(new Date(value).getTime())
  ) {
    throw new Error(`Evaluation replay cutoff ${field} is invalid`);
  }
  return value;
}

function parseSequence(value: unknown, field: string): string {
  if (typeof value !== "string" || !DECIMAL_BIGINT.test(value)) {
    throw new Error(`Evaluation replay cutoff ${field} is invalid`);
  }
  return value;
}

export function parseAnalyticsEvaluationReplayCutoff(
  value: unknown,
): AnalyticsEvaluationReplayCutoff {
  if (typeof value !== "object" || value === null || Array.isArray(value)) {
    throw new Error("Evaluation replay cutoff is invalid");
  }
  const record = value as Record<string, unknown>;
  if (
    record.kind !== "evaluation_operation_cutoff" ||
    record.version !== 1 ||
    typeof record.deploymentGeneration !== "string" ||
    !DECIMAL_BIGINT.test(record.deploymentGeneration) ||
    typeof record.sourceActivationGeneration !== "string" ||
    !DECIMAL_BIGINT.test(record.sourceActivationGeneration) ||
    typeof record.configurationDigest !== "string" ||
    !SHA256_HEX.test(record.configurationDigest) ||
    typeof record.configurationCount !== "number" ||
    !Number.isSafeInteger(record.configurationCount) ||
    record.configurationCount < 0
  ) {
    throw new Error("Evaluation replay cutoff is invalid");
  }

  return {
    kind: "evaluation_operation_cutoff" as const,
    version: 1 as const,
    deploymentGeneration: record.deploymentGeneration,
    sourceActivationGeneration: record.sourceActivationGeneration,
    configurationDigest: record.configurationDigest,
    configurationCount: record.configurationCount,
    lowerAcceptanceSequence: parseSequence(
      record.lowerAcceptanceSequence,
      "lower sequence",
    ),
    ...(record.captureHandoffAcceptanceSequence === undefined
      ? {}
      : {
          captureHandoffAcceptanceSequence: parseSequence(
            record.captureHandoffAcceptanceSequence,
            "handoff sequence",
          ),
        }),
    sealedAt: parseIsoDate(record.sealedAt, "seal time"),
    ...(record.captureHandoffAt === undefined
      ? {}
      : {
          captureHandoffAt: parseIsoDate(
            record.captureHandoffAt,
            "handoff time",
          ),
        }),
  };
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

async function evaluationConfigurationSnapshot(
  transaction: Prisma.TransactionClient,
): Promise<readonly EvaluationConfigurationSnapshot[]> {
  const configurations = await transaction.jobConfiguration.findMany({
    where: {
      jobType: "EVAL",
      status: "ACTIVE",
      evalTemplateId: { not: null },
    },
    select: {
      id: true,
      projectId: true,
      evalTemplateId: true,
      targetObject: true,
      updatedAt: true,
    },
    orderBy: [{ projectId: "asc" }, { id: "asc" }],
  });
  return configurations.map((configuration) => {
    if (!configuration.evalTemplateId) {
      throw new Error("Active evaluation configuration has no template");
    }
    return {
      id: configuration.id,
      projectId: configuration.projectId,
      evalTemplateId: configuration.evalTemplateId,
      targetObject: configuration.targetObject,
      updatedAt: configuration.updatedAt.toISOString(),
    };
  });
}

async function acceptanceWatermark(input: {
  readonly transaction: Prisma.TransactionClient;
  readonly deploymentGeneration: bigint;
}): Promise<bigint> {
  const row = await input.transaction.analyticsIngestionOperation.aggregate({
    where: {
      analyticsBackend: "DORIS",
      deploymentGeneration: input.deploymentGeneration,
      status: "VISIBLE",
      acceptanceSequence: { not: null },
    },
    _max: { acceptanceSequence: true },
  });
  return row._max.acceptanceSequence ?? 0n;
}

export async function sealAnalyticsEvaluationReplayCutoff(input: {
  readonly transaction: Prisma.TransactionClient;
  readonly deploymentGeneration: bigint;
  readonly activationGeneration: bigint;
  readonly now?: Date;
}): Promise<{
  readonly cutoffState: Prisma.InputJsonValue;
  readonly cutoffDigest: string;
}> {
  const configurations = await evaluationConfigurationSnapshot(
    input.transaction,
  );
  const watermark = await acceptanceWatermark({
    transaction: input.transaction,
    deploymentGeneration: input.deploymentGeneration,
  });
  const now = input.now ?? (await databaseClock(input.transaction));
  const cutoff = {
    kind: "evaluation_operation_cutoff" as const,
    version: 1 as const,
    deploymentGeneration: input.deploymentGeneration.toString(),
    sourceActivationGeneration: input.activationGeneration.toString(),
    configurationDigest: digestAnalyticsEvaluationReplayState(configurations),
    configurationCount: configurations.length,
    lowerAcceptanceSequence: watermark.toString(),
    sealedAt: now.toISOString(),
  } satisfies AnalyticsEvaluationReplayCutoff;
  return {
    cutoffState: cutoff,
    cutoffDigest: digestAnalyticsEvaluationReplayState(cutoff),
  };
}

export async function prepareAnalyticsEvaluationReplayHandoff(input: {
  readonly transaction: Prisma.TransactionClient;
  readonly activation: AnalyticsCapabilityActivation;
  readonly now: Date;
}): Promise<{
  readonly cutoffState: Prisma.InputJsonValue;
  readonly cutoffDigest: string;
}> {
  const cutoff = parseAnalyticsEvaluationReplayCutoff(
    input.activation.cutoffState,
  );
  if (
    cutoff.deploymentGeneration !==
      input.activation.deploymentGeneration.toString() ||
    cutoff.sourceActivationGeneration !==
      (input.activation.generation - 1n).toString() ||
    input.activation.cutoffActivationGeneration !==
      input.activation.generation - 1n ||
    input.activation.cutoffDigest !==
      digestAnalyticsEvaluationReplayState(cutoff)
  ) {
    throw new Error("Evaluation replay cutoff provenance changed");
  }
  const configurations = await evaluationConfigurationSnapshot(
    input.transaction,
  );
  if (
    digestAnalyticsEvaluationReplayState(configurations) !==
      cutoff.configurationDigest ||
    configurations.length !== cutoff.configurationCount
  ) {
    throw new Error(
      "Evaluation configurations changed after the replay cutoff",
    );
  }
  const watermark = await acceptanceWatermark({
    transaction: input.transaction,
    deploymentGeneration: input.activation.deploymentGeneration,
  });
  const handoff = {
    ...cutoff,
    captureHandoffAcceptanceSequence: watermark.toString(),
    captureHandoffAt: input.now.toISOString(),
  } satisfies AnalyticsEvaluationReplayCutoff;
  return {
    cutoffState: handoff,
    cutoffDigest: digestAnalyticsEvaluationReplayState(handoff),
  };
}

export async function verifyDurableAnalyticsEvaluationDrain(
  transaction: Prisma.TransactionClient,
  provenance: {
    readonly deploymentGeneration: bigint;
    readonly activationGeneration: bigint;
  },
): Promise<void> {
  const [dispatches, executions] = await Promise.all([
    transaction.analyticsEvaluationDispatch.count({
      where: {
        deploymentGeneration: provenance.deploymentGeneration,
        capabilityActivationGeneration: provenance.activationGeneration,
        status: { in: ["PENDING", "PUBLISHED", "PROCESSING"] },
      },
    }),
    transaction.jobExecution.count({
      where: {
        status: { in: ["PENDING", "DELAYED"] },
        analyticsEvaluationDispatch: {
          is: {
            deploymentGeneration: provenance.deploymentGeneration,
            capabilityActivationGeneration: provenance.activationGeneration,
          },
        },
      },
    }),
  ]);
  if (dispatches > 0 || executions > 0) {
    throw new Error("Evaluation capability still has durable work to drain");
  }
}

export async function verifyDurableAnalyticsEvaluationBootstrap(
  transaction: Prisma.TransactionClient,
  input: {
    readonly deploymentGeneration: bigint;
    readonly activationGeneration: bigint;
    readonly expectedCutoffDigest?: string;
  },
): Promise<{ readonly bootstrapEvidenceDigest: string }> {
  if (
    input.expectedCutoffDigest !== undefined &&
    !SHA256_HEX.test(input.expectedCutoffDigest)
  ) {
    throw new TypeError("Invalid evaluation replay cutoff digest");
  }
  const activation =
    await transaction.analyticsCapabilityActivation.findUniqueOrThrow({
      where: { capability: "EVALUATIONS" },
    });
  const cutoff = input.expectedCutoffDigest
    ? parseAnalyticsEvaluationReplayCutoff(activation.cutoffState)
    : null;
  if (
    activation.status !== "DARK" ||
    !activation.captureEnabled ||
    activation.deploymentGeneration !== input.deploymentGeneration ||
    activation.generation !== input.activationGeneration ||
    (cutoff
      ? activation.cutoffDigest !== input.expectedCutoffDigest ||
        !cutoff.captureHandoffAt ||
        cutoff.captureHandoffAcceptanceSequence === undefined
      : activation.rescanRequired ||
        activation.cutoffState !== null ||
        activation.cutoffDigest !== null)
  ) {
    throw new Error("Evaluation replay bootstrap provenance changed");
  }
  const configurations = await evaluationConfigurationSnapshot(transaction);
  if (
    cutoff &&
    (configurations.length !== cutoff.configurationCount ||
      digestAnalyticsEvaluationReplayState(configurations) !==
        cutoff.configurationDigest)
  ) {
    throw new Error(
      "Evaluation configurations changed during replay bootstrap",
    );
  }
  const [olderSuspended, invalidCurrent] = await Promise.all([
    transaction.analyticsEvaluationDispatch.count({
      where: {
        deploymentGeneration: input.deploymentGeneration,
        capabilityActivationGeneration: {
          not: input.activationGeneration,
        },
        status: "SUSPENDED",
      },
    }),
    transaction.analyticsEvaluationDispatch.count({
      where: {
        deploymentGeneration: input.deploymentGeneration,
        capabilityActivationGeneration: input.activationGeneration,
        status: { in: ["PENDING", "PUBLISHED", "PROCESSING"] },
      },
    }),
  ]);
  if (olderSuspended > 0 || invalidCurrent > 0) {
    throw new Error("Evaluation replay bootstrap is not durably sealed");
  }

  const hash = createHash("sha256");
  hash.update(
    stableJson({
      kind: "evaluation_replay_bootstrap",
      version: 1,
      deploymentGeneration: input.deploymentGeneration.toString(),
      activationGeneration: input.activationGeneration.toString(),
      cutoffDigest: input.expectedCutoffDigest ?? null,
      configurationDigest: digestAnalyticsEvaluationReplayState(configurations),
    }),
    "utf8",
  );
  let cursor: string | undefined;
  do {
    const page = await transaction.analyticsEvaluationDispatch.findMany({
      where: {
        deploymentGeneration: input.deploymentGeneration,
        capabilityActivationGeneration: input.activationGeneration,
        status: "SUSPENDED",
      },
      select: {
        id: true,
        operationId: true,
        projectId: true,
        targetType: true,
        targetId: true,
        capabilityContractVersion: true,
      },
      orderBy: { id: "asc" },
      take: 1_000,
      ...(cursor ? { cursor: { id: cursor }, skip: 1 } : {}),
    });
    for (const row of page) {
      hash.update("\n", "utf8");
      hash.update(stableJson(row), "utf8");
    }
    cursor = page.length === 1_000 ? page.at(-1)?.id : undefined;
  } while (cursor);
  return { bootstrapEvidenceDigest: hash.digest("hex") };
}

export async function transferSuspendedAnalyticsEvaluationDispatches(input: {
  readonly client?: PrismaClient;
  readonly admissionContext: AnalyticsRuntimeAdmissionContext;
  readonly expectedCutoffDigest: string;
  readonly now?: Date;
}): Promise<number> {
  if (!SHA256_HEX.test(input.expectedCutoffDigest)) {
    throw new TypeError("Invalid evaluation replay cutoff digest");
  }
  return (input.client ?? prisma).$transaction(async (transaction) => {
    await transaction.$queryRaw(
      Prisma.sql`SELECT capability FROM analytics_capability_activations WHERE capability::text = 'evaluations' FOR UPDATE`,
    );
    const activation =
      await transaction.analyticsCapabilityActivation.findUniqueOrThrow({
        where: { capability: "EVALUATIONS" },
      });
    const cutoff = parseAnalyticsEvaluationReplayCutoff(activation.cutoffState);
    if (
      activation.status !== "DARK" ||
      !activation.captureEnabled ||
      !activation.rescanRequired ||
      !activation.captureRequired ||
      !cutoff.captureHandoffAt ||
      cutoff.captureHandoffAcceptanceSequence === undefined ||
      activation.cutoffDigest !== input.expectedCutoffDigest
    ) {
      throw new Error("Evaluation replay handoff is not ready");
    }
    const suspendedCount = await transaction.analyticsEvaluationDispatch.count({
      where: {
        deploymentGeneration: activation.deploymentGeneration,
        capabilityActivationGeneration: BigInt(
          cutoff.sourceActivationGeneration,
        ),
        status: "SUSPENDED",
      },
    });
    if (
      activation.captureRowBudget === null ||
      activation.captureRows + BigInt(suspendedCount) >
        BigInt(activation.captureRowBudget)
    ) {
      throw new Error("Evaluation replay exceeds the capture row budget");
    }
    const admission = await lockAnalyticsAdmission({
      transaction,
      runtimeLeaseId: input.admissionContext.runtimeLeaseId,
      expectedBackend: input.admissionContext.backend,
      expectedDeploymentGeneration: input.admissionContext.deploymentGeneration,
      capability: "evaluations",
      action: "internalBootstrap",
      now: input.now,
    });
    if (
      admission.capabilityActivationGeneration !== activation.generation ||
      admission.capabilityContractVersion !== activation.contractVersion
    ) {
      throw new Error("Evaluation replay admission provenance changed");
    }
    const transferred =
      await transaction.analyticsEvaluationDispatch.updateMany({
        where: {
          deploymentGeneration: activation.deploymentGeneration,
          capabilityActivationGeneration: BigInt(
            cutoff.sourceActivationGeneration,
          ),
          status: "SUSPENDED",
        },
        data: {
          capabilityActivationGeneration: activation.generation,
          capabilityContractVersion: activation.contractVersion,
          captureRuntimeLeaseId: admission.admittingRuntimeLeaseId,
          nextAttemptAt: input.now ?? admission.admittedAt,
          failureCode: null,
        },
      });
    if (transferred.count !== suspendedCount) {
      throw new Error("Evaluation suspended dispatch transfer was fenced");
    }
    if (transferred.count > 0) {
      await transaction.analyticsCapabilityActivation.update({
        where: { capability: "EVALUATIONS" },
        data: {
          captureRows: {
            increment: BigInt(transferred.count),
          },
        },
      });
    }
    return transferred.count;
  });
}
