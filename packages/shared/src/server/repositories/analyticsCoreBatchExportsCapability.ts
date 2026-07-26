import { createHash } from "node:crypto";

import { type Prisma } from "@prisma/client";

import type { AnalyticsCapabilityName } from "../analytics-persistence/analyticsCapabilities";

function sha256(value: unknown): string {
  return createHash("sha256").update(JSON.stringify(value)).digest("hex");
}

async function countCoreBatchExportGenerationWork(input: {
  readonly transaction: Prisma.TransactionClient;
  readonly activationGeneration: bigint;
  readonly onlyNonTerminal: boolean;
}): Promise<number> {
  return input.transaction.batchExport.count({
    where: {
      capabilityActivationGeneration: input.activationGeneration,
      ...(input.onlyNonTerminal
        ? {
            OR: [
              { executionState: null },
              {
                executionState: {
                  notIn: ["COMPLETED", "FAILED", "CANCELLED", "QUARANTINED"],
                },
              },
              { dispatchOutbox: { is: { status: "PENDING" } } },
            ],
          }
        : {}),
    },
  });
}

export async function verifyDurableCoreBatchExportsBootstrap(
  transaction: Prisma.TransactionClient,
  input: {
    readonly deploymentGeneration: bigint;
    readonly activationGeneration: bigint;
  },
): Promise<{ readonly bootstrapEvidenceDigest: string }> {
  if (input.deploymentGeneration < 1n || input.activationGeneration < 1n) {
    throw new TypeError("Invalid core batch export bootstrap provenance");
  }
  const activation =
    await transaction.analyticsCapabilityActivation.findUniqueOrThrow({
      where: { capability: "CORE_BATCH_EXPORTS" },
    });
  if (
    activation.backend !== "DORIS" ||
    activation.deploymentGeneration !== input.deploymentGeneration ||
    activation.generation !== input.activationGeneration ||
    activation.status !== "DARK"
  ) {
    throw new Error("Core batch export bootstrap provenance changed");
  }
  const durableWork = await countCoreBatchExportGenerationWork({
    transaction,
    activationGeneration: input.activationGeneration,
    onlyNonTerminal: false,
  });
  if (durableWork !== 0) {
    throw new Error(
      "Core batch export DARK capability contains unexpected durable work",
    );
  }
  return {
    bootstrapEvidenceDigest: sha256({
      kind: "core-batch-exports-no-backfill-bootstrap-v1",
      capability: "coreBatchExports",
      deploymentGeneration: input.deploymentGeneration.toString(),
      activationGeneration: input.activationGeneration.toString(),
      contractVersion: activation.contractVersion,
      minimumRuntimeContract: activation.minimumRuntimeContract,
      durableWork,
    }),
  };
}

export async function verifyDurableCoreBatchExportsDrain(
  transaction: Prisma.TransactionClient,
  provenance: {
    readonly capability: AnalyticsCapabilityName;
    readonly deploymentGeneration: bigint;
    readonly activationGeneration: bigint;
    readonly capabilityContractVersion: number;
  },
): Promise<void> {
  if (provenance.capability !== "coreBatchExports") {
    throw new TypeError("Drain proof is not core batch exports");
  }
  const activation =
    await transaction.analyticsCapabilityActivation.findUniqueOrThrow({
      where: { capability: "CORE_BATCH_EXPORTS" },
    });
  if (
    activation.backend !== "DORIS" ||
    activation.deploymentGeneration !== provenance.deploymentGeneration ||
    activation.generation !== provenance.activationGeneration ||
    activation.contractVersion !== provenance.capabilityContractVersion ||
    activation.status !== "DRAINING"
  ) {
    throw new Error("Core batch export drain provenance changed");
  }
  const pending = await countCoreBatchExportGenerationWork({
    transaction,
    activationGeneration: provenance.activationGeneration,
    onlyNonTerminal: true,
  });
  if (pending !== 0) {
    throw new Error("Core batch export capability still has durable work");
  }
}
