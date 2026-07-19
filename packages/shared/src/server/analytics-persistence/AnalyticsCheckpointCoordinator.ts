import { createHash } from "node:crypto";

import type { AnalyticsCheckpointGeneration, Prisma } from "@prisma/client";

type DrainState = {
  readonly nonterminalOperations: number;
  readonly nonterminalLoads: number;
  readonly nonterminalDeletions: number;
  readonly drained: boolean;
};

type CheckpointRepository = {
  readonly findPendingAnchor: () =>
    | Promise<AnalyticsCheckpointGeneration | null>
    | AnalyticsCheckpointGeneration
    | null;
  readonly claimAnchorReconciliation: (input: {
    generation: bigint;
    leaseOwner: string;
    leaseMs: number;
    now: Date;
  }) => Promise<AnalyticsCheckpointGeneration | null>;
  readonly begin: (input: {
    leaseOwner: string;
    leaseMs: number;
    now: Date;
  }) => Promise<AnalyticsCheckpointGeneration>;
  readonly renew: (input: {
    generation: bigint;
    leaseOwner: string;
    leaseMs: number;
    now: Date;
  }) => Promise<boolean>;
  readonly drainState: (input: { generation: bigint }) => Promise<DrainState>;
  readonly recordArtifacts: (input: {
    generation: bigint;
    leaseOwner: string;
    postgresSnapshotId: string;
    postgresWalLsn: string;
    dorisSnapshotId: string;
    artifactDigests: Prisma.InputJsonValue;
    manifest: Prisma.InputJsonValue;
    keyId: string;
    manifestHash: string;
    signature: string;
    now: Date;
  }) => Promise<boolean>;
  readonly seal: (input: {
    generation: bigint;
    leaseOwner: string;
    manifestHash: string;
    externalAnchorRef: string;
    now: Date;
  }) => Promise<boolean>;
  readonly abort: (input: {
    generation: bigint;
    leaseOwner: string;
    reasonCode: string;
    now: Date;
  }) => Promise<boolean>;
};

export type ExternalCheckpointAnchor = {
  readonly generation: bigint;
  readonly manifestHash: string;
  readonly reference: string;
};

export type AnalyticsCheckpointCoordinatorDependencies = {
  readonly leaseOwner: string;
  readonly leaseMs: number;
  readonly timeoutMs: number;
  readonly pollIntervalMs: number;
  readonly repository: CheckpointRepository;
  readonly postgres: {
    capture(): Promise<{
      readonly snapshotId: string;
      readonly walLsn: string;
      readonly digest: string;
    }>;
  };
  readonly doris: {
    capture(): Promise<{
      readonly snapshotId: string;
      readonly digest: string;
      readonly schemaVersions: Readonly<Record<string, string>>;
    }>;
  };
  readonly lifecycle: {
    capture(): Promise<{
      readonly traceDeletionGenerationDigest: string;
      readonly projectDeletionGenerationDigest: string;
      readonly purgeWatermark: string | null;
    }>;
  };
  readonly signer: {
    readonly keyId: string;
    sign(manifestHash: string): Promise<string>;
  };
  readonly anchor: {
    publishLatest(input: {
      readonly generation: bigint;
      readonly manifestHash: string;
      readonly predecessorHash: string | null;
      readonly idempotencyKey: string;
    }): Promise<{ readonly reference: string }>;
    readLatest(): Promise<ExternalCheckpointAnchor | null>;
  };
  readonly now?: () => Date;
  readonly wait?: (milliseconds: number) => Promise<void>;
};

export type AnalyticsCheckpointResult = {
  readonly generation: bigint;
  readonly manifestHash: string;
  readonly externalAnchorRef: string;
};

function stableJson(value: unknown): string {
  if (value === null || typeof value !== "object") return JSON.stringify(value);
  if (Array.isArray(value)) {
    return `[${value.map((item) => stableJson(item)).join(",")}]`;
  }
  const record = value as Record<string, unknown>;
  return `{${Object.keys(record)
    .sort()
    .map((key) => `${JSON.stringify(key)}:${stableJson(record[key])}`)
    .join(",")}}`;
}

function sha256(value: string): string {
  return createHash("sha256").update(value, "utf8").digest("hex");
}

export function canonicalCheckpointManifestHash(manifest: unknown): string {
  return sha256(stableJson(manifest));
}

export async function verifyAnalyticsCheckpointForRestore(input: {
  readonly checkpoint: AnalyticsCheckpointGeneration;
  readonly latestAnchor: ExternalCheckpointAnchor;
  readonly expectedPredecessorHash: string | null;
  readonly actualArtifactDigests: Readonly<Record<string, string>>;
  readonly verifySignature: (input: {
    readonly keyId: string;
    readonly manifestHash: string;
    readonly signature: string;
  }) => Promise<boolean>;
}): Promise<Record<string, unknown>> {
  const { checkpoint } = input;
  if (
    checkpoint.status !== "SEALED" ||
    !checkpoint.sealedAt ||
    !checkpoint.manifest ||
    !checkpoint.manifestHash ||
    !checkpoint.signature ||
    !checkpoint.keyId ||
    !checkpoint.externalAnchorRef
  ) {
    throw new Error("Checkpoint is not sealed and restorable");
  }
  if (
    input.latestAnchor.generation !== checkpoint.generation ||
    input.latestAnchor.manifestHash !== checkpoint.manifestHash ||
    input.latestAnchor.reference !== checkpoint.externalAnchorRef
  ) {
    throw new Error(
      "Checkpoint is below or conflicts with the external anchor",
    );
  }
  if (checkpoint.predecessorHash !== input.expectedPredecessorHash) {
    throw new Error("Checkpoint predecessor chain is invalid");
  }
  const manifestHash = canonicalCheckpointManifestHash(checkpoint.manifest);
  if (manifestHash !== checkpoint.manifestHash) {
    throw new Error("Checkpoint manifest digest is invalid");
  }
  const artifactDigests = checkpoint.artifactDigests;
  if (
    !artifactDigests ||
    stableJson(artifactDigests) !== stableJson(input.actualArtifactDigests)
  ) {
    throw new Error("Checkpoint artifact digests are invalid");
  }
  if (
    !(await input.verifySignature({
      keyId: checkpoint.keyId,
      manifestHash,
      signature: checkpoint.signature,
    }))
  ) {
    throw new Error("Checkpoint signature or verification key is invalid");
  }
  return checkpoint.manifest as Record<string, unknown>;
}

function assertDigest(value: string, name: string): void {
  if (!/^[a-f0-9]{64}$/.test(value)) {
    throw new Error(`Invalid ${name} artifact digest`);
  }
}

function defaultWait(milliseconds: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, milliseconds));
}

export class AnalyticsCheckpointCoordinator {
  private readonly now: () => Date;
  private readonly wait: (milliseconds: number) => Promise<void>;

  constructor(
    private readonly dependencies: AnalyticsCheckpointCoordinatorDependencies,
  ) {
    if (
      !dependencies.leaseOwner ||
      dependencies.leaseMs < 10_000 ||
      dependencies.timeoutMs < dependencies.pollIntervalMs ||
      dependencies.pollIntervalMs < 50
    ) {
      throw new TypeError("Invalid analytics checkpoint coordinator settings");
    }
    this.now = dependencies.now ?? (() => new Date());
    this.wait = dependencies.wait ?? defaultWait;
  }

  async run(): Promise<AnalyticsCheckpointResult> {
    const reconciled = await this.reconcilePendingAnchor();
    if (reconciled) return reconciled;

    const startedAt = this.now();
    const checkpoint = await this.dependencies.repository.begin({
      leaseOwner: this.dependencies.leaseOwner,
      leaseMs: this.dependencies.leaseMs,
      now: startedAt,
    });
    let artifactsRecorded = false;
    try {
      await this.drain(checkpoint, startedAt);
      const [postgres, doris, lifecycle] = await Promise.all([
        this.dependencies.postgres.capture(),
        this.dependencies.doris.capture(),
        this.dependencies.lifecycle.capture(),
      ]);
      assertDigest(postgres.digest, "Postgres");
      assertDigest(doris.digest, "Doris");
      assertDigest(
        lifecycle.traceDeletionGenerationDigest,
        "trace deletion generation",
      );
      assertDigest(
        lifecycle.projectDeletionGenerationDigest,
        "project deletion generation",
      );

      const artifactDigests = {
        postgres: postgres.digest,
        doris: doris.digest,
        traceDeletionGenerations: lifecycle.traceDeletionGenerationDigest,
        projectDeletionGenerations: lifecycle.projectDeletionGenerationDigest,
      };
      const manifest = {
        version: 1,
        keyId: this.dependencies.signer.keyId,
        generation: checkpoint.generation.toString(),
        createdAt: checkpoint.createdAt.toISOString(),
        predecessorHash: checkpoint.predecessorHash,
        highWatermarks: {
          operationAcceptedAt:
            checkpoint.operationHighWatermarkAcceptedAt.toISOString(),
          operationAcceptedAtNanos:
            checkpoint.operationHighWatermarkAcceptedAtNanos.toString(),
          loadCreatedAt: checkpoint.loadHighWatermarkCreatedAt.toISOString(),
          deletionCreatedAt:
            checkpoint.deletionHighWatermarkCreatedAt.toISOString(),
          purge: lifecycle.purgeWatermark,
        },
        schemaVersions: doris.schemaVersions,
        postgres: {
          snapshotId: postgres.snapshotId,
          walLsn: postgres.walLsn,
          digest: postgres.digest,
        },
        doris: {
          snapshotId: doris.snapshotId,
          digest: doris.digest,
        },
        lifecycle: {
          traceDeletionGenerationDigest:
            lifecycle.traceDeletionGenerationDigest,
          projectDeletionGenerationDigest:
            lifecycle.projectDeletionGenerationDigest,
          purgeWatermark: lifecycle.purgeWatermark,
        },
      };
      const manifestHash = canonicalCheckpointManifestHash(manifest);
      const signature = await this.dependencies.signer.sign(manifestHash);
      if (!signature)
        throw new Error("Checkpoint signer returned no signature");
      const recorded = await this.dependencies.repository.recordArtifacts({
        generation: checkpoint.generation,
        leaseOwner: this.dependencies.leaseOwner,
        postgresSnapshotId: postgres.snapshotId,
        postgresWalLsn: postgres.walLsn,
        dorisSnapshotId: doris.snapshotId,
        artifactDigests,
        manifest,
        keyId: this.dependencies.signer.keyId,
        manifestHash,
        signature,
        now: this.now(),
      });
      if (!recorded)
        throw new Error("Analytics checkpoint lost its artifact fence");
      artifactsRecorded = true;

      const externalAnchorRef = await this.publishOrReconcileAnchor({
        checkpoint,
        manifestHash,
      });
      const sealed = await this.dependencies.repository.seal({
        generation: checkpoint.generation,
        leaseOwner: this.dependencies.leaseOwner,
        manifestHash,
        externalAnchorRef,
        now: this.now(),
      });
      if (!sealed) {
        throw new Error("Analytics checkpoint requires anchor reconciliation");
      }
      return {
        generation: checkpoint.generation,
        manifestHash,
        externalAnchorRef,
      };
    } catch (error) {
      if (!artifactsRecorded) {
        await this.dependencies.repository.abort({
          generation: checkpoint.generation,
          leaseOwner: this.dependencies.leaseOwner,
          reasonCode:
            error instanceof Error &&
            error.message === "Analytics checkpoint drain timed out"
              ? "CHECKPOINT_DRAIN_TIMEOUT"
              : "CHECKPOINT_CAPTURE_FAILED",
          now: this.now(),
        });
      }
      throw error;
    }
  }

  private async drain(
    checkpoint: AnalyticsCheckpointGeneration,
    startedAt: Date,
  ): Promise<void> {
    const deadline = startedAt.getTime() + this.dependencies.timeoutMs;
    for (;;) {
      const state = await this.dependencies.repository.drainState({
        generation: checkpoint.generation,
      });
      if (state.drained) return;
      if (this.now().getTime() >= deadline) {
        throw new Error("Analytics checkpoint drain timed out");
      }
      const renewed = await this.dependencies.repository.renew({
        generation: checkpoint.generation,
        leaseOwner: this.dependencies.leaseOwner,
        leaseMs: this.dependencies.leaseMs,
        now: this.now(),
      });
      if (!renewed) throw new Error("Analytics checkpoint lost its lease");
      await this.wait(this.dependencies.pollIntervalMs);
    }
  }

  private async publishOrReconcileAnchor(input: {
    readonly checkpoint: AnalyticsCheckpointGeneration;
    readonly manifestHash: string;
  }): Promise<string> {
    try {
      const published = await this.dependencies.anchor.publishLatest({
        generation: input.checkpoint.generation,
        manifestHash: input.manifestHash,
        predecessorHash: input.checkpoint.predecessorHash,
        idempotencyKey: `analytics-checkpoint-${input.checkpoint.generation}`,
      });
      return published.reference;
    } catch (error) {
      const latest = await this.dependencies.anchor.readLatest();
      if (
        latest?.generation === input.checkpoint.generation &&
        latest.manifestHash === input.manifestHash
      ) {
        return latest.reference;
      }
      if (!latest || latest.generation < input.checkpoint.generation) {
        await this.dependencies.repository.abort({
          generation: input.checkpoint.generation,
          leaseOwner: this.dependencies.leaseOwner,
          reasonCode: "CHECKPOINT_ANCHOR_NOT_PUBLISHED",
          now: this.now(),
        });
      }
      throw error;
    }
  }

  private async reconcilePendingAnchor(): Promise<AnalyticsCheckpointResult | null> {
    const pending = await this.dependencies.repository.findPendingAnchor();
    if (!pending) return null;
    const claimed =
      await this.dependencies.repository.claimAnchorReconciliation({
        generation: pending.generation,
        leaseOwner: this.dependencies.leaseOwner,
        leaseMs: this.dependencies.leaseMs,
        now: this.now(),
      });
    if (!claimed) {
      throw new Error("Analytics checkpoint anchor reconciliation is leased");
    }
    const latest = await this.dependencies.anchor.readLatest();
    if (
      latest?.generation === claimed.generation &&
      latest.manifestHash === claimed.manifestHash
    ) {
      const sealed = await this.dependencies.repository.seal({
        generation: claimed.generation,
        leaseOwner: this.dependencies.leaseOwner,
        manifestHash: claimed.manifestHash!,
        externalAnchorRef: latest.reference,
        now: this.now(),
      });
      if (!sealed)
        throw new Error("Checkpoint anchor reconciliation lost its fence");
      return {
        generation: claimed.generation,
        manifestHash: claimed.manifestHash!,
        externalAnchorRef: latest.reference,
      };
    }
    if (!latest || latest.generation < claimed.generation) {
      await this.dependencies.repository.abort({
        generation: claimed.generation,
        leaseOwner: this.dependencies.leaseOwner,
        reasonCode: "CHECKPOINT_ANCHOR_NOT_PUBLISHED",
        now: this.now(),
      });
      return null;
    }
    throw new Error(
      "External checkpoint anchor conflicts with the pending manifest",
    );
  }
}
