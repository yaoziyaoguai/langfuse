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
  enableAnalyticsCapabilityDarkCapture,
  sealDorisAnalyticsIntegrationReplayCutoff,
  verifyDorisAnalyticsIntegrationBootstrap,
  verifyDorisAnalyticsIntegrationDrain,
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

async function runtimeInventory(path: string | undefined): Promise<string[]> {
  const resolved = required(path, "--runtime-inventory-file");
  if (!isAbsolute(resolved)) {
    throw new TypeError("--runtime-inventory-file must be absolute");
  }
  const value: unknown = JSON.parse(await readFile(resolved, "utf8"));
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
  const ids = value.map(
    (entry) => (entry as { instanceId: string }).instanceId,
  );
  if (new Set(ids).size !== ids.length) {
    throw new TypeError("Runtime inventory contains duplicate instances");
  }
  return ids;
}

const parsed = parseArgs({
  args:
    process.argv.slice(2)[0] === "--"
      ? process.argv.slice(3)
      : process.argv.slice(2),
  allowPositionals: true,
  strict: true,
  options: {
    "expected-generation": { type: "string" },
    "expected-deployment-generation": { type: "string" },
    "expected-activation-generation": { type: "string" },
    "runtime-inventory-file": { type: "string" },
  },
});

function status(value: {
  readonly status: string;
  readonly deploymentGeneration: bigint;
  readonly activationGeneration: bigint;
  readonly bootstrapEvidenceDigest?: string | null;
}): void {
  process.stdout.write(
    `${JSON.stringify({
      capability: "analyticsIntegrations",
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
  if (parsed.positionals.length !== 1) {
    throw new TypeError("Exactly one command is required");
  }
  const command = parsed.positionals[0];
  if (command === "status") {
    const activation =
      await prisma.analyticsCapabilityActivation.findUniqueOrThrow({
        where: { capability: "ANALYTICS_INTEGRATIONS" },
      });
    status({
      status: activation.status,
      deploymentGeneration: activation.deploymentGeneration,
      activationGeneration: activation.generation,
      bootstrapEvidenceDigest: activation.bootstrapEvidenceDigest,
    });
    return;
  }
  if (command === "begin-dark") {
    const contract = ANALYTICS_CAPABILITY_CATALOG.analyticsIntegrations;
    const activation = await beginAnalyticsCapabilityDark({
      capability: "analyticsIntegrations",
      expectedGeneration: positiveBigInt(
        parsed.values["expected-generation"],
        "--expected-generation",
      ),
      contractVersion: contract.contractVersion,
      minimumRuntimeContract: contract.minimumRuntimeContract,
    });
    status({
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
  if (command === "enable-capture") {
    const activation = await enableAnalyticsCapabilityDarkCapture({
      capability: "analyticsIntegrations",
      expectedDeploymentGeneration,
      expectedActivationGeneration,
      expectedRuntimeInstanceIds: await runtimeInventory(
        parsed.values["runtime-inventory-file"],
      ),
    });
    status({
      status: activation.status,
      deploymentGeneration: activation.deploymentGeneration,
      activationGeneration: activation.generation,
    });
    return;
  }
  if (command === "begin-drain") {
    const activation = await beginAnalyticsCapabilityDrain({
      capability: "analyticsIntegrations",
      expectedDeploymentGeneration,
      expectedActivationGeneration,
    });
    status({
      status: activation.status,
      deploymentGeneration: activation.deploymentGeneration,
      activationGeneration: activation.generation,
    });
    return;
  }
  if (command === "disable") {
    const activation = await disableAnalyticsCapability({
      capability: "analyticsIntegrations",
      expectedDeploymentGeneration,
      expectedActivationGeneration,
      captureRequired: true,
      rescanRequired: true,
      verifyDurableDrain: verifyDorisAnalyticsIntegrationDrain,
      sealReplayCutoff: sealDorisAnalyticsIntegrationReplayCutoff,
    });
    status({
      status: activation.status,
      deploymentGeneration: activation.deploymentGeneration,
      activationGeneration: activation.generation,
    });
    return;
  }
  if (command !== "activate") {
    throw new TypeError("Unsupported analytics integration capability command");
  }
  const completed = await completeAnalyticsCapabilityBootstrap({
    capability: "analyticsIntegrations",
    expectedDeploymentGeneration,
    expectedActivationGeneration,
    verifyDurableBootstrap: (transaction) =>
      verifyDorisAnalyticsIntegrationBootstrap({ transaction }),
  });
  if (!completed.bootstrapEvidenceDigest) {
    throw new Error("Analytics integration bootstrap evidence is missing");
  }
  const activation = await activateAnalyticsCapability({
    capability: "analyticsIntegrations",
    expectedDeploymentGeneration,
    expectedActivationGeneration,
    expectedRuntimeInstanceIds: await runtimeInventory(
      parsed.values["runtime-inventory-file"],
    ),
    expectedBootstrapEvidenceDigest: completed.bootstrapEvidenceDigest,
  });
  status({
    status: activation.status,
    deploymentGeneration: activation.deploymentGeneration,
    activationGeneration: activation.generation,
    bootstrapEvidenceDigest: activation.bootstrapEvidenceDigest,
  });
}

main()
  .catch(() => {
    process.stderr.write(
      "Analytics integration capability command failed (details redacted)\n",
    );
    process.exitCode = 1;
  })
  .finally(async () => {
    await prisma.$disconnect().catch(() => {
      process.exitCode = 1;
    });
  });
