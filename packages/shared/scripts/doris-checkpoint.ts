import { execFile } from "node:child_process";
import { createHash, randomUUID, sign } from "node:crypto";
import { hostname } from "node:os";
import { readFile } from "node:fs/promises";
import { promisify } from "node:util";

import { prisma } from "../src/db";
import {
  AnalyticsCheckpointCoordinator,
  abortAnalyticsCheckpoint,
  beginAnalyticsCheckpoint,
  claimAnalyticsCheckpointAnchorReconciliation,
  findAnalyticsCheckpointPendingAnchor,
  getAnalyticsCheckpointDrainState,
  recordAnalyticsCheckpointArtifacts,
  renewAnalyticsCheckpointLease,
  sealAnalyticsCheckpoint,
  type ExternalCheckpointAnchor,
} from "../src/server";

const execFileAsync = promisify(execFile);

function required(name: string): string {
  const value = process.env[name];
  if (!value) throw new Error(`Missing required ${name}`);
  return value;
}

function positiveInteger(name: string, fallback: number): number {
  const raw = process.env[name];
  const value = raw === undefined ? fallback : Number(raw);
  if (!Number.isSafeInteger(value) || value <= 0) {
    throw new Error(`Invalid ${name}`);
  }
  return value;
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
): Promise<T> {
  const { stdout, stderr } = await execFileAsync(argv[0], argv.slice(1), {
    env: process.env,
    maxBuffer: 10 * 1024 * 1024,
    shell: false,
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

function anchorClient(urlString: string) {
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
      const response = await fetch(url, {
        method: "PUT",
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
      const response = await fetch(url, {
        method: "GET",
        headers: await tokenHeader(),
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
  const [traceGenerations, projectGenerations] = await Promise.all([
    prisma.analyticsDeletionTombstone.findMany({
      orderBy: [{ projectId: "asc" }, { traceId: "asc" }],
      select: { projectId: true, traceId: true, generation: true },
    }),
    prisma.analyticsProjectDeletionGeneration.findMany({
      orderBy: { projectId: "asc" },
      select: { projectId: true, generation: true },
    }),
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
    purgeWatermark: null,
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
  const coordinator = new AnalyticsCheckpointCoordinator({
    leaseOwner: `${hostname()}-${process.pid}-${randomUUID()}`,
    leaseMs: positiveInteger("LANGFUSE_CHECKPOINT_LEASE_MS", 60_000),
    timeoutMs: positiveInteger("LANGFUSE_CHECKPOINT_TIMEOUT_MS", 30 * 60_000),
    pollIntervalMs: positiveInteger(
      "LANGFUSE_CHECKPOINT_POLL_INTERVAL_MS",
      1_000,
    ),
    repository: {
      findPendingAnchor: () => findAnalyticsCheckpointPendingAnchor({}),
      claimAnchorReconciliation: (input) =>
        claimAnalyticsCheckpointAnchorReconciliation(input),
      begin: (input) => beginAnalyticsCheckpoint(input),
      renew: (input) => renewAnalyticsCheckpointLease(input),
      drainState: (input) => getAnalyticsCheckpointDrainState(input),
      recordArtifacts: (input) => recordAnalyticsCheckpointArtifacts(input),
      seal: (input) => sealAnalyticsCheckpoint(input),
      abort: (input) => abortAnalyticsCheckpoint(input),
    },
    postgres: {
      capture: async () => {
        const capture = await executeCapture<{
          snapshotId?: string;
          walLsn?: string;
          digest?: string;
        }>(postgresCaptureArgv);
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
        }>(dorisCaptureArgv);
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
    anchor: anchorClient(required("LANGFUSE_CHECKPOINT_ANCHOR_URL")),
  });
  const result = await coordinator.run();
  process.stdout.write(
    `${JSON.stringify({
      generation: result.generation.toString(),
      manifestHash: result.manifestHash,
      externalAnchorRef: result.externalAnchorRef,
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
