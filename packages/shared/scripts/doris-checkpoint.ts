import { execFile } from "node:child_process";
import { createHash, randomUUID, sign } from "node:crypto";
import { hostname } from "node:os";
import { readFile } from "node:fs/promises";
import { promisify } from "node:util";

import { prisma } from "../src/db";
import {
  AnalyticsCheckpointCoordinator,
  AnalyticsRuntimeController,
  CURRENT_ANALYTICS_CANONICALIZER_VERSION,
  CURRENT_ANALYTICS_SCHEMA_VERSION,
  abortAnalyticsCheckpoint,
  beginAnalyticsCheckpoint,
  claimAnalyticsCheckpointAnchorReconciliation,
  findAnalyticsCheckpointPendingAnchor,
  fingerprintConfiguredAnalyticsQueueNamespace,
  getAnalyticsCheckpointDrainState,
  getAnalyticsRetentionBarrier,
  recordAnalyticsCheckpointArtifacts,
  renewAnalyticsCheckpointLease,
  resolveAnalyticsRuntimeWorkloadEpoch,
  sealAnalyticsCheckpoint,
  withAnalyticsRuntimeIoAbortSignal,
  withAnalyticsCheckpointIoFence,
  type ExternalCheckpointAnchor,
} from "../src/server";

const execFileAsync = promisify(execFile);

function required(name: string): string {
  const value = process.env[name];
  if (!value) throw new Error(`Missing required ${name}`);
  return value;
}

function optional(name: string): string | undefined {
  return process.env[name];
}

function positiveInteger(name: string, fallback: number): number {
  const raw = process.env[name];
  const value = raw === undefined ? fallback : Number(raw);
  if (!Number.isSafeInteger(value) || value <= 0) {
    throw new Error(`Invalid ${name}`);
  }
  return value;
}

function selectedAnalyticsBackend(): "doris" {
  const backend = optional("LANGFUSE_ANALYTICS_BACKEND") ?? "clickhouse";
  if (backend !== "doris") {
    throw new Error(
      "Doris checkpoint requires LANGFUSE_ANALYTICS_BACKEND=doris",
    );
  }
  return backend;
}

function captureArgv(name: string): readonly [string, ...string[]] {
  const parsed: unknown = JSON.parse(required(name));
  if (
    !Array.isArray(parsed) ||
    parsed.length === 0 ||
    parsed.some((value) => typeof value !== "string" || !value) ||
    !parsed[0]!.startsWith("/")
  ) {
    throw new Error(
      `${name} must be a JSON argv array with an absolute executable`,
    );
  }
  return parsed as [string, ...string[]];
}

async function executeCapture<T>(
  argv: readonly [string, ...string[]],
  timeoutMs: number,
): Promise<T> {
  const { stdout, stderr } = await withAnalyticsRuntimeIoAbortSignal({
    timeoutMs,
    execute: (signal) =>
      execFileAsync(argv[0], argv.slice(1), {
        env: process.env,
        maxBuffer: 10 * 1024 * 1024,
        shell: false,
        signal,
        timeout: timeoutMs,
      }),
  });
  if (stderr.trim()) process.stderr.write(stderr);
  return JSON.parse(stdout) as T;
}

function sha256(value: string): string {
  return createHash("sha256").update(value, "utf8").digest("hex");
}

function assertCapture<T extends { snapshotId?: string; digest?: string }>(
  value: T,
): asserts value is T & { snapshotId: string; digest: string } {
  if (!value.snapshotId || !/^[a-f0-9]{64}$/.test(value.digest ?? "")) {
    throw new Error("Backup capture returned an invalid snapshot ID or digest");
  }
}

async function tokenHeader(): Promise<Record<string, string>> {
  const path = process.env.LANGFUSE_CHECKPOINT_ANCHOR_TOKEN_FILE;
  if (!path) return {};
  const token = (await readFile(path, "utf8")).trim();
  if (!token) throw new Error("Checkpoint anchor token file is empty");
  return { authorization: `Bearer ${token}` };
}

function anchorClient(urlString: string, timeoutMs: number) {
  const url = new URL(urlString);
  if (required("NODE_ENV") === "production" && url.protocol !== "https:") {
    throw new Error("Production checkpoint anchor must use HTTPS");
  }
  return {
    async publishLatest(input: {
      generation: bigint;
      manifestHash: string;
      predecessorHash: string | null;
      idempotencyKey: string;
    }): Promise<{ reference: string }> {
      const response = await withAnalyticsRuntimeIoAbortSignal({
        timeoutMs,
        execute: async (signal) =>
          fetch(url, {
            method: "PUT",
            redirect: "error",
            signal,
            headers: {
              "content-type": "application/json",
              "idempotency-key": input.idempotencyKey,
              ...(await tokenHeader()),
            },
            body: JSON.stringify({
              generation: input.generation.toString(),
              manifestHash: input.manifestHash,
              predecessorHash: input.predecessorHash,
            }),
          }),
      });
      if (!response.ok) {
        throw new Error(
          `Checkpoint anchor publish failed (${response.status})`,
        );
      }
      const result = (await response.json()) as { reference?: string };
      if (!result.reference)
        throw new Error("Checkpoint anchor returned no reference");
      return { reference: result.reference };
    },
    async readLatest(): Promise<ExternalCheckpointAnchor | null> {
      const response = await withAnalyticsRuntimeIoAbortSignal({
        timeoutMs,
        execute: async (signal) =>
          fetch(url, {
            method: "GET",
            redirect: "error",
            signal,
            headers: await tokenHeader(),
          }),
      });
      if (response.status === 404) return null;
      if (!response.ok) {
        throw new Error(`Checkpoint anchor read failed (${response.status})`);
      }
      const result = (await response.json()) as {
        generation?: string;
        manifestHash?: string;
        reference?: string;
      };
      if (
        !result.generation ||
        !/^[a-f0-9]{64}$/.test(result.manifestHash ?? "") ||
        !result.reference
      ) {
        throw new Error("Checkpoint anchor returned an invalid latest record");
      }
      return {
        generation: BigInt(result.generation),
        manifestHash: result.manifestHash!,
        reference: result.reference,
      };
    },
  };
}

async function captureLifecycleWatermarks() {
  const [traceGenerations, projectGenerations, retentionBarrier] =
    await Promise.all([
      prisma.analyticsDeletionTombstone.findMany({
        orderBy: [{ projectId: "asc" }, { traceId: "asc" }],
        select: { projectId: true, traceId: true, generation: true },
      }),
      prisma.analyticsProjectDeletionGeneration.findMany({
        orderBy: { projectId: "asc" },
        select: { projectId: true, generation: true },
      }),
      getAnalyticsRetentionBarrier({ client: prisma }),
    ]);
  return {
    traceDeletionGenerationDigest: sha256(
      JSON.stringify(
        traceGenerations.map((row) => [
          row.projectId,
          row.traceId,
          row.generation.toString(),
        ]),
      ),
    ),
    projectDeletionGenerationDigest: sha256(
      JSON.stringify(
        projectGenerations.map((row) => [
          row.projectId,
          row.generation.toString(),
        ]),
      ),
    ),
    purgeWatermark: retentionBarrier?.toISOString() ?? null,
  };
}

async function main(): Promise<void> {
  const postgresCaptureArgv = captureArgv(
    "LANGFUSE_CHECKPOINT_POSTGRES_CAPTURE_ARGV_JSON",
  );
  const dorisCaptureArgv = captureArgv(
    "LANGFUSE_CHECKPOINT_DORIS_CAPTURE_ARGV_JSON",
  );
  const signingKey = await readFile(
    required("LANGFUSE_CHECKPOINT_SIGNING_PRIVATE_KEY_FILE"),
    "utf8",
  );
  const keyId = required("LANGFUSE_CHECKPOINT_SIGNING_KEY_ID");
  const invocationId = randomUUID();
  const leaseOwner = `${hostname()}-${process.pid}-${invocationId}`;
  const checkpointTimeoutMs = positiveInteger(
    "LANGFUSE_CHECKPOINT_TIMEOUT_MS",
    30 * 60_000,
  );
  const checkpointLeaseMs = positiveInteger(
    "LANGFUSE_CHECKPOINT_LEASE_MS",
    checkpointTimeoutMs + 60_000,
  );
  const runtimeLeaseMs = positiveInteger(
    "LANGFUSE_CHECKPOINT_RUNTIME_LEASE_MS",
    120_000,
  );
  const runtimeHeartbeatMs = positiveInteger(
    "LANGFUSE_CHECKPOINT_RUNTIME_HEARTBEAT_MS",
    30_000,
  );
  if (runtimeHeartbeatMs >= runtimeLeaseMs) {
    throw new Error(
      "LANGFUSE_CHECKPOINT_RUNTIME_HEARTBEAT_MS must be shorter than the runtime lease",
    );
  }
  const ioTransactionTimeoutMs = checkpointTimeoutMs + 5_000;
  if (!Number.isSafeInteger(ioTransactionTimeoutMs)) {
    throw new Error("LANGFUSE_CHECKPOINT_TIMEOUT_MS is too large");
  }
  const workloadEpoch = await resolveAnalyticsRuntimeWorkloadEpoch({
    value: optional("LANGFUSE_ANALYTICS_WORKLOAD_EPOCH"),
    file: optional("LANGFUSE_ANALYTICS_WORKLOAD_EPOCH_FILE"),
  });
  const runtime = new AnalyticsRuntimeController({
    component: "checkpoint",
    instanceId: `checkpoint:${hostname()}:${process.pid}:${invocationId}`,
    backend: selectedAnalyticsBackend(),
    workloadEpoch,
    queueNamespaceFingerprint: fingerprintConfiguredAnalyticsQueueNamespace(),
    buildId: optional("BUILD_ID") ?? "doris-checkpoint",
    foundationContractVersion: 1,
    acceptedSchemaVersion: {
      min: CURRENT_ANALYTICS_SCHEMA_VERSION,
      max: CURRENT_ANALYTICS_SCHEMA_VERSION,
    },
    acceptedCanonicalVersion: {
      min: Number(CURRENT_ANALYTICS_CANONICALIZER_VERSION),
      max: Number(CURRENT_ANALYTICS_CANONICALIZER_VERSION),
    },
    capabilityContracts: [],
    leaseMs: runtimeLeaseMs,
  });
  let result: Awaited<ReturnType<AnalyticsCheckpointCoordinator["run"]>>;
  let runError: unknown;
  try {
    const initialization = await runtime.initialize({
      selectedBackendEmpty: false,
      evidenceDigest: "0".repeat(64),
    });
    if (initialization.mode === "ADOPTION_REQUIRED") {
      throw new Error(
        "Analytics backend deployment adoption is required before checkpointing",
      );
    }
    const admissionContext =
      initialization.mode === "READY"
        ? runtime.getAdmissionContext()
        : undefined;
    if (admissionContext) runtime.startHeartbeat(runtimeHeartbeatMs);
    const coordinator = new AnalyticsCheckpointCoordinator({
      leaseOwner,
      leaseMs: checkpointLeaseMs,
      timeoutMs: checkpointTimeoutMs,
      pollIntervalMs: positiveInteger(
        "LANGFUSE_CHECKPOINT_POLL_INTERVAL_MS",
        1_000,
      ),
      runFencedIo: ({ checkpoint, execute }) =>
        withAnalyticsCheckpointIoFence({
          generation: checkpoint.generation,
          leaseOwner,
          admissionContext,
          transactionTimeoutMs: ioTransactionTimeoutMs,
          execute,
        }),
      repository: {
        findPendingAnchor: () => findAnalyticsCheckpointPendingAnchor({}),
        claimAnchorReconciliation: (input) =>
          claimAnalyticsCheckpointAnchorReconciliation({
            ...input,
            admissionContext,
          }),
        begin: (input) =>
          beginAnalyticsCheckpoint({ ...input, admissionContext }),
        renew: (input) =>
          renewAnalyticsCheckpointLease({ ...input, admissionContext }),
        drainState: (input) =>
          getAnalyticsCheckpointDrainState({ ...input, admissionContext }),
        recordArtifacts: (input) =>
          recordAnalyticsCheckpointArtifacts({ ...input, admissionContext }),
        seal: (input) =>
          sealAnalyticsCheckpoint({ ...input, admissionContext }),
        abort: (input) =>
          abortAnalyticsCheckpoint({ ...input, admissionContext }),
      },
      postgres: {
        capture: async () => {
          const capture = await executeCapture<{
            snapshotId?: string;
            walLsn?: string;
            digest?: string;
          }>(postgresCaptureArgv, checkpointTimeoutMs);
          assertCapture(capture);
          if (!capture.walLsn)
            throw new Error("Postgres capture returned no WAL LSN");
          return {
            snapshotId: capture.snapshotId,
            walLsn: capture.walLsn,
            digest: capture.digest,
          };
        },
      },
      doris: {
        capture: async () => {
          const capture = await executeCapture<{
            snapshotId?: string;
            digest?: string;
            schemaVersions?: Record<string, string>;
          }>(dorisCaptureArgv, checkpointTimeoutMs);
          assertCapture(capture);
          if (!capture.schemaVersions) {
            throw new Error("Doris capture returned no schema versions");
          }
          return {
            snapshotId: capture.snapshotId,
            digest: capture.digest,
            schemaVersions: capture.schemaVersions,
          };
        },
      },
      lifecycle: { capture: captureLifecycleWatermarks },
      signer: {
        keyId,
        sign: async (manifestHash) =>
          sign(null, Buffer.from(manifestHash, "hex"), signingKey).toString(
            "base64",
          ),
      },
      anchor: anchorClient(
        required("LANGFUSE_CHECKPOINT_ANCHOR_URL"),
        checkpointTimeoutMs,
      ),
    });
    result = await coordinator.run();
  } catch (error) {
    runError = error;
  }
  let cleanupError: unknown;
  try {
    runtime.stopHeartbeat();
    if (!(await runtime.quiesce())) {
      throw new Error("Failed to quiesce checkpoint analytics runtime lease");
    }
  } catch (error) {
    cleanupError = error;
  }
  if (runError) throw runError;
  if (cleanupError) throw cleanupError;
  process.stdout.write(
    `${JSON.stringify({
      generation: result!.generation.toString(),
      manifestHash: result!.manifestHash,
      externalAnchorRef: result!.externalAnchorRef,
    })}\n`,
  );
}

main()
  .catch((error) => {
    process.stderr.write(
      `${error instanceof Error ? error.message : "Checkpoint failed"}\n`,
    );
    process.exitCode = 1;
  })
  .finally(async () => prisma.$disconnect());
