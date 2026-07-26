import { isAbsolute } from "node:path";
import { parseArgs } from "node:util";

import type { AnalyticsBackendDeploymentState } from "@prisma/client";
import { z } from "zod/v4";

import type { AnalyticsBackend } from "../src/server/analytics-persistence/analyticsBackend";
import { resolveAnalyticsRuntimeWorkloadEpoch } from "../src/server/analytics-persistence/analyticsWorkloadEpoch";
import type {
  AnalyticsBackendEmptinessEvidence,
  AnalyticsRuntimeInventoryEntry,
} from "../src/server/repositories/analyticsBackendDeployment";
import type {
  AnalyticsScoreDeletionQueueDrainEvidence,
  AnalyticsScoreDeletionQueueDrainScope,
} from "../src/server/redis/analyticsScoreDeletionDrain";

const SHA256_HEX = /^[a-f0-9]{64}$/;
const POSITIVE_INTEGER = /^[1-9][0-9]*$/;

const RuntimeInventorySchema = z
  .array(
    z
      .object({
        instanceId: z.string().trim().min(1),
        component: z.enum(["web", "worker"]),
      })
      .strict(),
  )
  .min(2);

type ReadTextFile = (path: string) => Promise<string>;
type OperatorEnvironment = Readonly<Record<string, string | undefined>>;

export type AdoptExistingBackendTransition = (input: {
  readonly expectedBackend: AnalyticsBackend;
  readonly workloadEpochFingerprint: string;
  readonly foundationContractVersion: number;
  readonly expectedInventory: readonly AnalyticsRuntimeInventoryEntry[];
  readonly expectedInventoryDigest: string;
  readonly verifyScoreDeletionQueuesEmpty: (
    scope: AnalyticsScoreDeletionQueueDrainScope,
  ) => Promise<AnalyticsScoreDeletionQueueDrainEvidence>;
  readonly drainAttestationDigest: string;
  readonly denyProbeAttestationDigest: string;
}) => Promise<AnalyticsBackendDeploymentState>;

export type SwitchBackendTransition = (input: {
  readonly expectedBackend: AnalyticsBackend;
  readonly expectedGeneration: bigint;
  readonly expectedWorkloadEpochFingerprint: string;
  readonly targetBackend: AnalyticsBackend;
  readonly targetWorkloadEpochFingerprint: string;
  readonly targetFoundationContractVersion: number;
  readonly expectedQuiescedInventory: readonly AnalyticsRuntimeInventoryEntry[];
  readonly verifyBackendEmptiness: () => Promise<AnalyticsBackendEmptinessEvidence>;
  readonly verifyScoreDeletionQueuesEmpty: (
    scope: AnalyticsScoreDeletionQueueDrainScope,
  ) => Promise<AnalyticsScoreDeletionQueueDrainEvidence>;
  readonly externalDrainAttestationDigest: string;
  readonly denyProbeAttestationDigest: string;
}) => Promise<AnalyticsBackendDeploymentState>;

type CommonDependencies = {
  readonly readTextFile: ReadTextFile;
  readonly fingerprintWorkloadEpoch: (epoch: string) => string;
  readonly digestRuntimeInventory: (
    inventory: readonly AnalyticsRuntimeInventoryEntry[],
  ) => string;
};

export type AdoptExistingBackendOperatorDependencies = CommonDependencies & {
  readonly verifyScoreDeletionQueuesEmpty: (
    scope: AnalyticsScoreDeletionQueueDrainScope,
  ) => Promise<AnalyticsScoreDeletionQueueDrainEvidence>;
  readonly adopt: AdoptExistingBackendTransition;
};

export type SwitchBackendOperatorDependencies = CommonDependencies & {
  readonly probeBackendEmptiness: (input: {
    readonly sourceBackend: AnalyticsBackend;
    readonly targetBackend: AnalyticsBackend;
  }) => Promise<AnalyticsBackendEmptinessEvidence>;
  readonly verifyScoreDeletionQueuesEmpty: (
    scope: AnalyticsScoreDeletionQueueDrainScope,
  ) => Promise<AnalyticsScoreDeletionQueueDrainEvidence>;
  readonly switchBackend: SwitchBackendTransition;
};

function requiredFlag(value: string | undefined, name: string): string {
  if (!value) throw new TypeError(`Missing required ${name}`);
  return value;
}

function parseBackend(
  value: string | undefined,
  name: string,
): AnalyticsBackend {
  const backend = requiredFlag(value, name);
  if (backend !== "clickhouse" && backend !== "doris") {
    throw new TypeError(`${name} must be clickhouse or doris`);
  }
  return backend;
}

function parseDigest(value: string | undefined, name: string): string {
  const digest = requiredFlag(value, name);
  if (!SHA256_HEX.test(digest)) {
    throw new TypeError(`${name} must be a lowercase SHA-256 digest`);
  }
  return digest;
}

function parsePositiveInteger(value: string | undefined, name: string): number {
  const raw = requiredFlag(value, name);
  if (!POSITIVE_INTEGER.test(raw)) {
    throw new TypeError(`${name} must be a positive integer`);
  }
  const parsed = Number(raw);
  if (!Number.isSafeInteger(parsed)) {
    throw new TypeError(`${name} exceeds the safe integer range`);
  }
  return parsed;
}

function parsePositiveBigInt(value: string | undefined, name: string): bigint {
  const raw = requiredFlag(value, name);
  if (!POSITIVE_INTEGER.test(raw)) {
    throw new TypeError(`${name} must be a positive integer`);
  }
  return BigInt(raw);
}

function requireAbsoluteFile(value: string | undefined, name: string): string {
  const path = requiredFlag(value, name);
  if (!isAbsolute(path)) {
    throw new TypeError(`${name} must be an absolute path`);
  }
  return path;
}

async function readRuntimeInventory(input: {
  readonly path: string;
  readonly readTextFile: ReadTextFile;
}): Promise<readonly AnalyticsRuntimeInventoryEntry[]> {
  let parsed: unknown;
  try {
    parsed = JSON.parse(await input.readTextFile(input.path));
  } catch {
    throw new TypeError("Runtime inventory file must contain valid JSON");
  }
  const result = RuntimeInventorySchema.safeParse(parsed);
  if (!result.success) {
    throw new TypeError(
      "Runtime inventory must be a non-empty array of strict web/worker entries",
    );
  }
  const components = new Set(result.data.map((entry) => entry.component));
  if (!components.has("web") || !components.has("worker")) {
    throw new TypeError("Runtime inventory must include web and worker");
  }
  return result.data;
}

async function readWorkloadEpoch(input: {
  readonly env: OperatorEnvironment;
  readonly valueVariable: string;
  readonly fileVariable: string;
  readonly readTextFile: ReadTextFile;
}): Promise<string> {
  const direct = input.env[input.valueVariable];
  const file = input.env[input.fileVariable];
  if ((direct === undefined) === (file === undefined)) {
    throw new TypeError(
      `Set exactly one workload epoch source: ${input.valueVariable} or ${input.fileVariable}`,
    );
  }

  return (await resolveAnalyticsRuntimeWorkloadEpoch({
    value: direct,
    file,
    readTextFile: input.readTextFile,
  }))!;
}

function validateInventoryDigest(input: {
  readonly inventory: readonly AnalyticsRuntimeInventoryEntry[];
  readonly expectedDigest: string;
  readonly digestRuntimeInventory: CommonDependencies["digestRuntimeInventory"];
}): void {
  if (input.digestRuntimeInventory(input.inventory) !== input.expectedDigest) {
    throw new Error(
      "Expected runtime inventory digest does not match inventory",
    );
  }
}

function parseAdoptArguments(argv: readonly string[]) {
  try {
    return parseArgs({
      args: [...argv],
      allowPositionals: false,
      strict: true,
      options: {
        "expected-backend": { type: "string" },
        "foundation-contract-version": { type: "string" },
        "expected-inventory-file": { type: "string" },
        "expected-inventory-digest": { type: "string" },
        "drain-attestation-digest": { type: "string" },
        "deny-probe-attestation-digest": { type: "string" },
      },
    }).values;
  } catch {
    // parseArgs can quote unexpected argv values in its error. Replace it so a
    // mistakenly supplied workload epoch can never be echoed by the caller.
    throw new TypeError("Invalid adopt-existing-backend arguments");
  }
}

function parseSwitchArguments(argv: readonly string[]) {
  try {
    return parseArgs({
      args: [...argv],
      allowPositionals: false,
      strict: true,
      options: {
        "expected-backend": { type: "string" },
        "expected-generation": { type: "string" },
        "target-backend": { type: "string" },
        "target-foundation-contract-version": { type: "string" },
        "expected-inventory-file": { type: "string" },
        "expected-inventory-digest": { type: "string" },
        "expected-source-emptiness-evidence-digest": { type: "string" },
        "expected-target-emptiness-evidence-digest": { type: "string" },
        "drain-attestation-digest": { type: "string" },
        "deny-probe-attestation-digest": { type: "string" },
      },
    }).values;
  } catch {
    throw new TypeError("Invalid switch-backend arguments");
  }
}

export async function executeAdoptExistingBackendOperator(input: {
  readonly argv: readonly string[];
  readonly env: OperatorEnvironment;
  readonly dependencies: AdoptExistingBackendOperatorDependencies;
}): Promise<AnalyticsBackendDeploymentState> {
  const values = parseAdoptArguments(input.argv);
  const expectedBackend = parseBackend(
    values["expected-backend"],
    "--expected-backend",
  );
  const foundationContractVersion = parsePositiveInteger(
    values["foundation-contract-version"],
    "--foundation-contract-version",
  );
  const inventoryPath = requireAbsoluteFile(
    values["expected-inventory-file"],
    "--expected-inventory-file",
  );
  const expectedInventoryDigest = parseDigest(
    values["expected-inventory-digest"],
    "--expected-inventory-digest",
  );
  const drainAttestationDigest = parseDigest(
    values["drain-attestation-digest"],
    "--drain-attestation-digest",
  );
  const denyProbeAttestationDigest = parseDigest(
    values["deny-probe-attestation-digest"],
    "--deny-probe-attestation-digest",
  );
  const [expectedInventory, workloadEpoch] = await Promise.all([
    readRuntimeInventory({
      path: inventoryPath,
      readTextFile: input.dependencies.readTextFile,
    }),
    readWorkloadEpoch({
      env: input.env,
      valueVariable: "LANGFUSE_ANALYTICS_WORKLOAD_EPOCH",
      fileVariable: "LANGFUSE_ANALYTICS_WORKLOAD_EPOCH_FILE",
      readTextFile: input.dependencies.readTextFile,
    }),
  ]);
  validateInventoryDigest({
    inventory: expectedInventory,
    expectedDigest: expectedInventoryDigest,
    digestRuntimeInventory: input.dependencies.digestRuntimeInventory,
  });
  const workloadEpochFingerprint =
    input.dependencies.fingerprintWorkloadEpoch(workloadEpoch);
  parseDigest(workloadEpochFingerprint, "Workload epoch fingerprint");

  return input.dependencies.adopt({
    expectedBackend,
    workloadEpochFingerprint,
    foundationContractVersion,
    expectedInventory,
    expectedInventoryDigest,
    verifyScoreDeletionQueuesEmpty:
      input.dependencies.verifyScoreDeletionQueuesEmpty,
    drainAttestationDigest,
    denyProbeAttestationDigest,
  });
}

export async function executeSwitchBackendOperator(input: {
  readonly argv: readonly string[];
  readonly env: OperatorEnvironment;
  readonly dependencies: SwitchBackendOperatorDependencies;
}): Promise<AnalyticsBackendDeploymentState> {
  const values = parseSwitchArguments(input.argv);
  const expectedBackend = parseBackend(
    values["expected-backend"],
    "--expected-backend",
  );
  const targetBackend = parseBackend(
    values["target-backend"],
    "--target-backend",
  );
  if (expectedBackend === targetBackend) {
    throw new TypeError("Source and target analytics backends must differ");
  }
  const expectedGeneration = parsePositiveBigInt(
    values["expected-generation"],
    "--expected-generation",
  );
  const targetFoundationContractVersion = parsePositiveInteger(
    values["target-foundation-contract-version"],
    "--target-foundation-contract-version",
  );
  const inventoryPath = requireAbsoluteFile(
    values["expected-inventory-file"],
    "--expected-inventory-file",
  );
  const expectedInventoryDigest = parseDigest(
    values["expected-inventory-digest"],
    "--expected-inventory-digest",
  );
  const expectedSourceEmptinessEvidenceDigest = parseDigest(
    values["expected-source-emptiness-evidence-digest"],
    "--expected-source-emptiness-evidence-digest",
  );
  const expectedTargetEmptinessEvidenceDigest = parseDigest(
    values["expected-target-emptiness-evidence-digest"],
    "--expected-target-emptiness-evidence-digest",
  );
  const externalDrainAttestationDigest = parseDigest(
    values["drain-attestation-digest"],
    "--drain-attestation-digest",
  );
  const denyProbeAttestationDigest = parseDigest(
    values["deny-probe-attestation-digest"],
    "--deny-probe-attestation-digest",
  );
  const [expectedQuiescedInventory, expectedEpoch, targetEpoch] =
    await Promise.all([
      readRuntimeInventory({
        path: inventoryPath,
        readTextFile: input.dependencies.readTextFile,
      }),
      readWorkloadEpoch({
        env: input.env,
        valueVariable: "LANGFUSE_ANALYTICS_EXPECTED_WORKLOAD_EPOCH",
        fileVariable: "LANGFUSE_ANALYTICS_EXPECTED_WORKLOAD_EPOCH_FILE",
        readTextFile: input.dependencies.readTextFile,
      }),
      readWorkloadEpoch({
        env: input.env,
        valueVariable: "LANGFUSE_ANALYTICS_TARGET_WORKLOAD_EPOCH",
        fileVariable: "LANGFUSE_ANALYTICS_TARGET_WORKLOAD_EPOCH_FILE",
        readTextFile: input.dependencies.readTextFile,
      }),
    ]);
  validateInventoryDigest({
    inventory: expectedQuiescedInventory,
    expectedDigest: expectedInventoryDigest,
    digestRuntimeInventory: input.dependencies.digestRuntimeInventory,
  });

  const expectedWorkloadEpochFingerprint =
    input.dependencies.fingerprintWorkloadEpoch(expectedEpoch);
  const targetWorkloadEpochFingerprint =
    input.dependencies.fingerprintWorkloadEpoch(targetEpoch);
  parseDigest(
    expectedWorkloadEpochFingerprint,
    "Expected workload epoch fingerprint",
  );
  parseDigest(
    targetWorkloadEpochFingerprint,
    "Target workload epoch fingerprint",
  );
  if (expectedWorkloadEpochFingerprint === targetWorkloadEpochFingerprint) {
    throw new TypeError(
      "Analytics backend switch requires a new workload epoch",
    );
  }

  const verifyBackendEmptiness =
    async (): Promise<AnalyticsBackendEmptinessEvidence> => {
      const evidence = await input.dependencies.probeBackendEmptiness({
        sourceBackend: expectedBackend,
        targetBackend,
      });
      if (
        evidence.source.backend !== expectedBackend ||
        evidence.target.backend !== targetBackend
      ) {
        throw new Error("Analytics backend emptiness probe identity mismatch");
      }
      if (!evidence.source.empty || !evidence.target.empty) {
        throw new Error(
          "Analytics backend switch source and target must both be empty",
        );
      }
      if (
        evidence.source.evidenceDigest !== expectedSourceEmptinessEvidenceDigest
      ) {
        throw new Error("Source emptiness evidence digest does not match");
      }
      if (
        evidence.target.evidenceDigest !== expectedTargetEmptinessEvidenceDigest
      ) {
        throw new Error("Target emptiness evidence digest does not match");
      }
      return evidence;
    };

  return input.dependencies.switchBackend({
    expectedBackend,
    expectedGeneration,
    expectedWorkloadEpochFingerprint,
    targetBackend,
    targetWorkloadEpochFingerprint,
    targetFoundationContractVersion,
    expectedQuiescedInventory,
    verifyBackendEmptiness,
    verifyScoreDeletionQueuesEmpty:
      input.dependencies.verifyScoreDeletionQueuesEmpty,
    externalDrainAttestationDigest,
    denyProbeAttestationDigest,
  });
}

export function formatAnalyticsBackendOperatorSuccess(
  command: "adopt-existing-backend" | "switch-backend",
  state: AnalyticsBackendDeploymentState,
): string {
  return `${JSON.stringify({
    command,
    backend: state.backend.toLowerCase(),
    generation: state.generation.toString(),
  })}\n`;
}

export function formatAnalyticsBackendOperatorFailure(_error: unknown): string {
  return "Analytics backend operator command failed (details redacted)\n";
}
