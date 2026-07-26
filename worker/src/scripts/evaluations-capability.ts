import { readFile } from "node:fs/promises";
import { isAbsolute } from "node:path";
import { parseArgs } from "node:util";

import { prisma } from "@langfuse/shared/src/db";
import {
  activateAnalyticsCapability,
  beginAnalyticsCapabilityDark,
  beginAnalyticsCapabilityDrain,
  completeAnalyticsCapabilityBootstrap,
  disableAnalyticsCapability,
  enableAnalyticsCapabilityDarkCapture,
  getS3EventStorageClient,
  sealAnalyticsEvaluationReplayCutoff,
  verifyDurableAnalyticsEvaluationBootstrap,
  verifyDurableAnalyticsEvaluationDrain,
} from "@langfuse/shared/src/server";

import { env } from "../env";
import { replayAnalyticsEvaluationCutoff } from "../features/evaluation/analyticsEvaluationReplay";
import {
  CanonicalIngestionArtifactStore,
  StorageServiceCanonicalObjectStore,
} from "../services/CanonicalIngestionArtifactStore";
import {
  normalizePnpmRunArgs,
  runEvaluationCapabilityOperator,
} from "./evaluations-capability-runner";

const POSITIVE_INTEGER = /^[1-9][0-9]*$/;

function positiveBigInt(value: string | undefined, name: string): bigint {
  if (!value || !POSITIVE_INTEGER.test(value)) {
    throw new TypeError(`${name} must be a positive integer`);
  }
  return BigInt(value);
}

function positiveNumber(value: string | undefined, name: string): number {
  const parsed = Number(value);
  if (
    !value ||
    !POSITIVE_INTEGER.test(value) ||
    !Number.isSafeInteger(parsed)
  ) {
    throw new TypeError(`${name} must be a positive integer`);
  }
  return parsed;
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
  try {
    const parsed = parseArgs({
      args: normalizePnpmRunArgs(process.argv.slice(2)),
      allowPositionals: true,
      strict: true,
      options: {
        "expected-deployment-generation": { type: "string" },
        "expected-activation-generation": { type: "string" },
        "expected-generation": { type: "string" },
        "contract-version": { type: "string" },
        "minimum-runtime-contract": { type: "string" },
        "runtime-inventory-file": { type: "string" },
        "worker-runtime-lease-id": { type: "string" },
      },
    });
    if (parsed.positionals.length !== 1) {
      throw new TypeError("Exactly one command is required");
    }
    return {
      command: parsed.positionals[0],
      values: parsed.values,
    };
  } catch {
    throw new TypeError("Invalid evaluations capability command");
  }
}

async function main(): Promise<void> {
  const { command, values } = parseCommand();
  if (command === "status") {
    const activation =
      await prisma.analyticsCapabilityActivation.findUniqueOrThrow({
        where: { capability: "EVALUATIONS" },
      });
    process.stdout.write(
      `${JSON.stringify({
        capability: "evaluations",
        status: activation.status.toLowerCase(),
        deploymentGeneration: activation.deploymentGeneration.toString(),
        activationGeneration: activation.generation.toString(),
        captureEnabled: activation.captureEnabled,
        rescanRequired: activation.rescanRequired,
      })}\n`,
    );
    return;
  }

  if (command === "begin-dark") {
    const activation = await beginAnalyticsCapabilityDark({
      capability: "evaluations",
      expectedGeneration: positiveBigInt(
        values["expected-generation"],
        "--expected-generation",
      ),
      contractVersion: positiveNumber(
        values["contract-version"],
        "--contract-version",
      ),
      minimumRuntimeContract: positiveNumber(
        values["minimum-runtime-contract"],
        "--minimum-runtime-contract",
      ),
    });
    process.stdout.write(
      `${JSON.stringify({
        capability: "evaluations",
        status: activation.status.toLowerCase(),
        activationGeneration: activation.generation.toString(),
      })}\n`,
    );
    return;
  }

  const expectedDeploymentGeneration = positiveBigInt(
    values["expected-deployment-generation"],
    "--expected-deployment-generation",
  );
  const expectedActivationGeneration = positiveBigInt(
    values["expected-activation-generation"],
    "--expected-activation-generation",
  );

  if (command === "begin-drain") {
    const activation = await beginAnalyticsCapabilityDrain({
      capability: "evaluations",
      expectedDeploymentGeneration,
      expectedActivationGeneration,
    });
    process.stdout.write(
      `${JSON.stringify({
        capability: "evaluations",
        status: activation.status.toLowerCase(),
        activationGeneration: activation.generation.toString(),
      })}\n`,
    );
    return;
  }

  if (command === "disable") {
    const activation = await disableAnalyticsCapability({
      capability: "evaluations",
      expectedDeploymentGeneration,
      expectedActivationGeneration,
      captureRequired: true,
      rescanRequired: true,
      verifyDurableDrain: (transaction, provenance) =>
        verifyDurableAnalyticsEvaluationDrain(transaction, provenance),
      sealReplayCutoff: (transaction) =>
        sealAnalyticsEvaluationReplayCutoff({
          transaction,
          deploymentGeneration: expectedDeploymentGeneration,
          activationGeneration: expectedActivationGeneration,
        }),
    });
    process.stdout.write(
      `${JSON.stringify({
        capability: "evaluations",
        status: activation.status.toLowerCase(),
        activationGeneration: activation.generation.toString(),
        cutoffDigest: activation.cutoffDigest,
      })}\n`,
    );
    return;
  }

  if (command !== "activate") {
    throw new TypeError("Unsupported evaluations capability command");
  }
  const [expectedRuntimeInstanceIds, current] = await Promise.all([
    runtimeInventory(values["runtime-inventory-file"]),
    prisma.analyticsCapabilityActivation.findUniqueOrThrow({
      where: { capability: "EVALUATIONS" },
    }),
  ]);
  if (
    current.status !== "DARK" ||
    current.deploymentGeneration !== expectedDeploymentGeneration ||
    current.generation !== expectedActivationGeneration
  ) {
    throw new Error("Evaluations capability activation CAS failed");
  }
  const capture = current.captureEnabled
    ? current
    : await enableAnalyticsCapabilityDarkCapture({
        capability: "evaluations",
        expectedDeploymentGeneration,
        expectedActivationGeneration,
        expectedRuntimeInstanceIds,
      });
  const workerRuntimeLeaseId = required(
    values["worker-runtime-lease-id"],
    "--worker-runtime-lease-id",
  );
  if (capture.rescanRequired) {
    if (!capture.cutoffDigest) {
      throw new Error("Evaluation replay cutoff is missing");
    }
    await replayAnalyticsEvaluationCutoff({
      client: prisma,
      artifactStore: new CanonicalIngestionArtifactStore(
        new StorageServiceCanonicalObjectStore(
          getS3EventStorageClient(env.LANGFUSE_S3_EVENT_UPLOAD_BUCKET),
        ),
      ),
      admissionContext: {
        runtimeLeaseId: workerRuntimeLeaseId,
        backend: "doris",
        deploymentGeneration: expectedDeploymentGeneration,
      },
      expectedCutoffDigest: capture.cutoffDigest,
    });
  }
  const completed = await completeAnalyticsCapabilityBootstrap({
    capability: "evaluations",
    expectedDeploymentGeneration,
    expectedActivationGeneration,
    ...(capture.cutoffDigest
      ? { expectedCutoffDigest: capture.cutoffDigest }
      : {}),
    verifyDurableBootstrap: (transaction) =>
      verifyDurableAnalyticsEvaluationBootstrap(transaction, {
        deploymentGeneration: expectedDeploymentGeneration,
        activationGeneration: expectedActivationGeneration,
        ...(capture.cutoffDigest
          ? { expectedCutoffDigest: capture.cutoffDigest }
          : {}),
      }),
  });
  if (!completed.bootstrapEvidenceDigest) {
    throw new Error("Evaluation bootstrap evidence is missing");
  }
  const activated = await activateAnalyticsCapability({
    capability: "evaluations",
    expectedDeploymentGeneration,
    expectedActivationGeneration,
    expectedRuntimeInstanceIds,
    expectedBootstrapEvidenceDigest: completed.bootstrapEvidenceDigest,
  });
  process.stdout.write(
    `${JSON.stringify({
      capability: "evaluations",
      status: activated.status.toLowerCase(),
      activationGeneration: activated.generation.toString(),
      bootstrapEvidenceDigest: activated.bootstrapEvidenceDigest,
    })}\n`,
  );
}

runEvaluationCapabilityOperator({
  execute: main,
  disconnect: () => prisma.$disconnect(),
  writeFailure: (message) => process.stderr.write(message),
  exit: (code) => process.exit(code),
}).catch(() => {
  process.stderr.write(
    "Evaluations capability operator runner failed (details redacted)\n",
  );
  process.exit(1);
});
