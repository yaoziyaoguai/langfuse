import { createHash } from "node:crypto";

import { type Prisma, type AnalyticsCapability } from "@prisma/client";

import {
  ANALYTICS_CAPABILITY_CATALOG,
  type AnalyticsCapabilityName,
} from "../analytics-persistence/analyticsCapabilities";
import {
  fromPrismaAnalyticsCapability,
  toPrismaAnalyticsCapability,
} from "../analytics-persistence/analyticsBackendMapping";

const U6_CAPABILITIES = [
  "experiments",
  "datasetRunExports",
  "datasetRunIngestion",
] as const satisfies readonly AnalyticsCapabilityName[];

export type U6AnalyticsCapability = (typeof U6_CAPABILITIES)[number];

export function isU6AnalyticsCapability(
  capability: AnalyticsCapabilityName,
): capability is U6AnalyticsCapability {
  return U6_CAPABILITIES.some((candidate) => candidate === capability);
}

function sha256(value: unknown): string {
  return createHash("sha256").update(JSON.stringify(value)).digest("hex");
}

async function countCapabilityGenerationWork(input: {
  readonly transaction: Prisma.TransactionClient;
  readonly capability: U6AnalyticsCapability;
  readonly activationGeneration: bigint;
  readonly onlyNonTerminal: boolean;
}): Promise<number> {
  switch (input.capability) {
    case "datasetRunIngestion":
      return input.transaction.analyticsIngestionOperation.count({
        where: {
          capability: "DATASET_RUN_INGESTION",
          capabilityActivationGeneration: input.activationGeneration,
          ...(input.onlyNonTerminal ? { terminalAt: null } : {}),
        },
      });
    case "experiments":
      return input.transaction.datasetRuns.count({
        where: {
          capabilityActivationGeneration: input.activationGeneration,
          ...(input.onlyNonTerminal
            ? {
                OR: [
                  {
                    experimentExecutionState: {
                      notIn: ["COMPLETED", "CANCELLED", "QUARANTINED"],
                    },
                  },
                  {
                    experimentDispatchOutbox: {
                      is: {
                        status: "PENDING",
                      },
                    },
                  },
                ],
              }
            : {}),
        },
      });
    case "datasetRunExports":
      return input.transaction.batchExport.count({
        where: {
          datasetRunExportActivationGeneration: input.activationGeneration,
          ...(input.onlyNonTerminal
            ? {
                OR: [
                  {
                    executionState: {
                      notIn: ["COMPLETED", "CANCELLED", "QUARANTINED"],
                    },
                  },
                  {
                    dispatchOutbox: {
                      is: {
                        status: "PENDING",
                      },
                    },
                  },
                ],
              }
            : {}),
        },
      });
  }
}

async function dependencyEvidence(input: {
  readonly transaction: Prisma.TransactionClient;
  readonly capability: U6AnalyticsCapability;
  readonly deploymentGeneration: bigint;
}): Promise<
  readonly {
    readonly capability: AnalyticsCapability;
    readonly generation: string;
    readonly contractVersion: number;
  }[]
> {
  const evidence = [];
  for (const dependency of ANALYTICS_CAPABILITY_CATALOG[input.capability]
    .dependencies) {
    const activation =
      await input.transaction.analyticsCapabilityActivation.findUnique({
        where: { capability: toPrismaAnalyticsCapability(dependency) },
      });
    if (
      !activation ||
      activation.backend !== "DORIS" ||
      activation.deploymentGeneration !== input.deploymentGeneration ||
      activation.status !== "ACTIVE"
    ) {
      throw new Error("U6 capability dependency is not active");
    }
    evidence.push({
      capability: activation.capability,
      generation: activation.generation.toString(),
      contractVersion: activation.contractVersion,
    });
  }
  return evidence;
}

export async function verifyDurableU6CapabilityBootstrap(
  transaction: Prisma.TransactionClient,
  input: {
    readonly capability: U6AnalyticsCapability;
    readonly deploymentGeneration: bigint;
    readonly activationGeneration: bigint;
  },
): Promise<{ readonly bootstrapEvidenceDigest: string }> {
  if (input.deploymentGeneration < 1n || input.activationGeneration < 1n) {
    throw new TypeError("Invalid U6 capability bootstrap provenance");
  }
  const activation =
    await transaction.analyticsCapabilityActivation.findUniqueOrThrow({
      where: { capability: toPrismaAnalyticsCapability(input.capability) },
    });
  if (
    activation.backend !== "DORIS" ||
    activation.deploymentGeneration !== input.deploymentGeneration ||
    activation.generation !== input.activationGeneration ||
    activation.status !== "DARK"
  ) {
    throw new Error("U6 capability bootstrap provenance changed");
  }
  const durableWork = await countCapabilityGenerationWork({
    transaction,
    capability: input.capability,
    activationGeneration: input.activationGeneration,
    onlyNonTerminal: false,
  });
  if (durableWork !== 0) {
    throw new Error("U6 DARK capability contains unexpected durable work");
  }
  const dependencies = await dependencyEvidence({
    transaction,
    capability: input.capability,
    deploymentGeneration: input.deploymentGeneration,
  });
  return {
    bootstrapEvidenceDigest: sha256({
      kind: "u6-no-backfill-bootstrap-v1",
      capability: input.capability,
      deploymentGeneration: input.deploymentGeneration.toString(),
      activationGeneration: input.activationGeneration.toString(),
      contractVersion: activation.contractVersion,
      minimumRuntimeContract: activation.minimumRuntimeContract,
      durableWork,
      dependencies,
    }),
  };
}

export async function verifyDurableU6CapabilityDrain(
  transaction: Prisma.TransactionClient,
  provenance: {
    readonly capability: AnalyticsCapabilityName;
    readonly deploymentGeneration: bigint;
    readonly activationGeneration: bigint;
    readonly capabilityContractVersion: number;
  },
): Promise<void> {
  if (!isU6AnalyticsCapability(provenance.capability)) {
    throw new TypeError("Drain proof is not an U6 capability");
  }
  const activation =
    await transaction.analyticsCapabilityActivation.findUniqueOrThrow({
      where: {
        capability: toPrismaAnalyticsCapability(provenance.capability),
      },
    });
  if (
    fromPrismaAnalyticsCapability(activation.capability) !==
      provenance.capability ||
    activation.backend !== "DORIS" ||
    activation.deploymentGeneration !== provenance.deploymentGeneration ||
    activation.generation !== provenance.activationGeneration ||
    activation.contractVersion !== provenance.capabilityContractVersion ||
    activation.status !== "DRAINING"
  ) {
    throw new Error("U6 capability drain provenance changed");
  }
  const pending = await countCapabilityGenerationWork({
    transaction,
    capability: provenance.capability,
    activationGeneration: provenance.activationGeneration,
    onlyNonTerminal: true,
  });
  if (pending !== 0) {
    throw new Error("U6 capability still has durable work");
  }
}
