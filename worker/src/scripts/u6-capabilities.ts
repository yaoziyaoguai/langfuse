import { readFile } from "node:fs/promises";
import { isAbsolute } from "node:path";
import { parseArgs } from "node:util";

import { prisma } from "@langfuse/shared/src/db";
import {
  ANALYTICS_CAPABILITY_CATALOG,
  activateAnalyticsCapability,
  beginAnalyticsCapabilityDark,
  beginAnalyticsCapabilityDrain,
  completeAnalyticsCapabilityBootstrap,
  disableAnalyticsCapability,
  isU6AnalyticsCapability,
  verifyDurableU6CapabilityBootstrap,
  verifyDurableU6CapabilityDrain,
  type AnalyticsCapabilityName,
  type U6AnalyticsCapability,
} from "@langfuse/shared/src/server";

const POSITIVE_INTEGER = /^[1-9][0-9]*$/;

function positiveBigInt(value: string | undefined, name: string): bigint {
  if (!value || !POSITIVE_INTEGER.test(value)) {
    throw new TypeError(`${name} must be a positive integer`);
  }
  return BigInt(value);
}

function required(value: string | undefined, name: string): string {
  if (!value) throw new TypeError(`Missing ${name}`);
  return value;
}

function capability(value: string | undefined): U6AnalyticsCapability {
  const candidate = required(value, "--capability") as AnalyticsCapabilityName;
  if (!isU6AnalyticsCapability(candidate)) {
    throw new TypeError("--capability must name an U6 capability");
  }
  return candidate;
}

async function runtimeInventory(path: string | undefined): Promise<string[]> {
  const resolved = required(path, "--runtime-inventory-file");
  if (!isAbsolute(resolved)) {
    throw new TypeError("--runtime-inventory-file must be absolute");
  }
  let value: unknown;
  try {
    value = JSON.parse(await readFile(resolved, "utf8"));
  } catch {
    throw new TypeError("Runtime inventory must contain valid JSON");
  }
  if (
    !Array.isArray(value) ||
    value.length === 0 ||
    value.some(
      (entry) =>
        typeof entry !== "object" ||
        entry === null ||
        Array.isArray(entry) ||
        typeof (entry as { instanceId?: unknown }).instanceId !== "string" ||
        !(entry as { instanceId: string }).instanceId,
    )
  ) {
    throw new TypeError("Runtime inventory is invalid");
  }
  const instanceIds = value.map(
    (entry) => (entry as { instanceId: string }).instanceId,
  );
  if (new Set(instanceIds).size !== instanceIds.length) {
    throw new TypeError("Runtime inventory contains duplicate instances");
  }
  return instanceIds;
}

function parseCommand() {
  const parsed = parseArgs({
    args:
      process.argv.slice(2)[0] === "--"
        ? process.argv.slice(3)
        : process.argv.slice(2),
    allowPositionals: true,
    strict: true,
    options: {
      capability: { type: "string" },
      "expected-generation": { type: "string" },
      "expected-deployment-generation": { type: "string" },
      "expected-activation-generation": { type: "string" },
      "runtime-inventory-file": { type: "string" },
    },
  });
  if (parsed.positionals.length !== 1) {
    throw new TypeError("Exactly one command is required");
  }
  return {
    command: parsed.positionals[0],
    capability: capability(parsed.values.capability),
    values: parsed.values,
  };
}

function writeStatus(value: {
  readonly capability: U6AnalyticsCapability;
  readonly status: string;
  readonly deploymentGeneration: bigint;
  readonly activationGeneration: bigint;
  readonly bootstrapEvidenceDigest?: string | null;
}): void {
  process.stdout.write(
    `${JSON.stringify({
      capability: value.capability,
      status: value.status.toLowerCase(),
      deploymentGeneration: value.deploymentGeneration.toString(),
      activationGeneration: value.activationGeneration.toString(),
      ...(value.bootstrapEvidenceDigest
        ? { bootstrapEvidenceDigest: value.bootstrapEvidenceDigest }
        : {}),
    })}\n`,
  );
}

async function main(): Promise<void> {
  const parsed = parseCommand();
  if (parsed.command === "status") {
    const activation =
      await prisma.analyticsCapabilityActivation.findUniqueOrThrow({
        where: {
          capability:
            parsed.capability === "experiments"
              ? "EXPERIMENTS"
              : parsed.capability === "datasetRunExports"
                ? "DATASET_RUN_EXPORTS"
                : "DATASET_RUN_INGESTION",
        },
      });
    writeStatus({
      capability: parsed.capability,
      status: activation.status,
      deploymentGeneration: activation.deploymentGeneration,
      activationGeneration: activation.generation,
      bootstrapEvidenceDigest: activation.bootstrapEvidenceDigest,
    });
    return;
  }

  if (parsed.command === "begin-dark") {
    const contract = ANALYTICS_CAPABILITY_CATALOG[parsed.capability];
    const activation = await beginAnalyticsCapabilityDark({
      capability: parsed.capability,
      expectedGeneration: positiveBigInt(
        parsed.values["expected-generation"],
        "--expected-generation",
      ),
      contractVersion: contract.contractVersion,
      minimumRuntimeContract: contract.minimumRuntimeContract,
    });
    writeStatus({
      capability: parsed.capability,
      status: activation.status,
      deploymentGeneration: activation.deploymentGeneration,
      activationGeneration: activation.generation,
    });
    return;
  }

  const expectedDeploymentGeneration = positiveBigInt(
    parsed.values["expected-deployment-generation"],
    "--expected-deployment-generation",
  );
  const expectedActivationGeneration = positiveBigInt(
    parsed.values["expected-activation-generation"],
    "--expected-activation-generation",
  );

  if (parsed.command === "begin-drain") {
    const activation = await beginAnalyticsCapabilityDrain({
      capability: parsed.capability,
      expectedDeploymentGeneration,
      expectedActivationGeneration,
    });
    writeStatus({
      capability: parsed.capability,
      status: activation.status,
      deploymentGeneration: activation.deploymentGeneration,
      activationGeneration: activation.generation,
    });
    return;
  }

  if (parsed.command === "disable") {
    const activation = await disableAnalyticsCapability({
      capability: parsed.capability,
      expectedDeploymentGeneration,
      expectedActivationGeneration,
      captureRequired: false,
      rescanRequired: false,
      verifyDurableDrain: (transaction, provenance) =>
        verifyDurableU6CapabilityDrain(transaction, provenance),
    });
    writeStatus({
      capability: parsed.capability,
      status: activation.status,
      deploymentGeneration: activation.deploymentGeneration,
      activationGeneration: activation.generation,
    });
    return;
  }

  if (parsed.command !== "activate") {
    throw new TypeError("Unsupported U6 capability command");
  }
  const expectedRuntimeInstanceIds = await runtimeInventory(
    parsed.values["runtime-inventory-file"],
  );
  const completed = await completeAnalyticsCapabilityBootstrap({
    capability: parsed.capability,
    expectedDeploymentGeneration,
    expectedActivationGeneration,
    verifyDurableBootstrap: (transaction) =>
      verifyDurableU6CapabilityBootstrap(transaction, {
        capability: parsed.capability,
        deploymentGeneration: expectedDeploymentGeneration,
        activationGeneration: expectedActivationGeneration,
      }),
  });
  if (!completed.bootstrapEvidenceDigest) {
    throw new Error("U6 capability bootstrap evidence is missing");
  }
  const activation = await activateAnalyticsCapability({
    capability: parsed.capability,
    expectedDeploymentGeneration,
    expectedActivationGeneration,
    expectedRuntimeInstanceIds,
    expectedBootstrapEvidenceDigest: completed.bootstrapEvidenceDigest,
  });
  writeStatus({
    capability: parsed.capability,
    status: activation.status,
    deploymentGeneration: activation.deploymentGeneration,
    activationGeneration: activation.generation,
    bootstrapEvidenceDigest: activation.bootstrapEvidenceDigest,
  });
}

main()
  .catch(() => {
    process.stderr.write(
      "U6 capability operator command failed (details redacted)\n",
    );
    process.exitCode = 1;
  })
  .finally(async () => {
    try {
      await prisma.$disconnect();
    } catch {
      process.stderr.write(
        "U6 capability operator cleanup failed (details redacted)\n",
      );
      process.exitCode = 1;
    }
  });
