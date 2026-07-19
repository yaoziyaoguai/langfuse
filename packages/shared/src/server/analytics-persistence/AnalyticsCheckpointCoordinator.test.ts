import { describe, expect, it, vi } from "vitest";
import type { AnalyticsCheckpointGeneration } from "@prisma/client";

import {
  AnalyticsCheckpointCoordinator,
  canonicalCheckpointManifestHash,
  verifyAnalyticsCheckpointForRestore,
} from "./AnalyticsCheckpointCoordinator";

function checkpoint() {
  return {
    generation: 7n,
    status: "PREPARING",
    leaseOwner: "checkpoint-worker",
    leaseExpiresAt: new Date("2026-07-18T12:05:00.000Z"),
    operationHighWatermarkAcceptedAt: new Date("2026-07-18T12:00:00.000Z"),
    operationHighWatermarkAcceptedAtNanos: 1_784_376_000_000_000_000n,
    loadHighWatermarkCreatedAt: new Date("2026-07-18T12:00:01.000Z"),
    deletionHighWatermarkCreatedAt: new Date("2026-07-18T12:00:02.000Z"),
    postgresSnapshotId: null,
    postgresWalLsn: null,
    dorisSnapshotId: null,
    artifactDigests: null,
    manifest: null,
    keyId: null,
    predecessorHash: "predecessor-hash",
    manifestHash: null,
    signature: null,
    externalAnchorRef: null,
    sealedAt: null,
    abortedAt: null,
    abortReasonCode: null,
    createdAt: new Date("2026-07-18T12:00:03.000Z"),
    updatedAt: new Date("2026-07-18T12:00:03.000Z"),
  } as const;
}

describe("AnalyticsCheckpointCoordinator", () => {
  it("drains, verifies, signs, anchors, then seals one common checkpoint", async () => {
    const recordArtifacts = vi.fn().mockResolvedValue(true);
    const seal = vi.fn().mockResolvedValue(true);
    const publishLatest = vi.fn().mockResolvedValue({
      reference: "anchor://generation/7",
    });
    const coordinator = new AnalyticsCheckpointCoordinator({
      leaseOwner: "checkpoint-worker",
      leaseMs: 60_000,
      timeoutMs: 30_000,
      pollIntervalMs: 100,
      repository: {
        findPendingAnchor: vi.fn().mockResolvedValue(null),
        claimAnchorReconciliation: vi.fn(),
        begin: vi.fn().mockResolvedValue(checkpoint()),
        renew: vi.fn().mockResolvedValue(true),
        drainState: vi
          .fn()
          .mockResolvedValueOnce({
            nonterminalOperations: 1,
            nonterminalLoads: 1,
            nonterminalDeletions: 0,
            drained: false,
          })
          .mockResolvedValueOnce({
            nonterminalOperations: 0,
            nonterminalLoads: 0,
            nonterminalDeletions: 0,
            drained: true,
          }),
        recordArtifacts,
        seal,
        abort: vi.fn(),
      },
      postgres: {
        capture: vi.fn().mockResolvedValue({
          snapshotId: "pg-backup-7",
          walLsn: "0/16B6C50",
          digest: "a".repeat(64),
        }),
      },
      doris: {
        capture: vi.fn().mockResolvedValue({
          snapshotId: "doris-snapshot-7",
          digest: "b".repeat(64),
          schemaVersions: { doris: "4.0.7", analytics: "0004" },
        }),
      },
      lifecycle: {
        capture: vi.fn().mockResolvedValue({
          traceDeletionGenerationDigest: "c".repeat(64),
          projectDeletionGenerationDigest: "d".repeat(64),
          purgeWatermark: null,
        }),
      },
      signer: {
        keyId: "checkpoint-signing-key-2026-07",
        sign: vi.fn().mockResolvedValue("signed-manifest"),
      },
      anchor: {
        publishLatest,
        readLatest: vi.fn().mockResolvedValue(null),
      },
      wait: vi.fn().mockResolvedValue(undefined),
      now: () => new Date("2026-07-18T12:00:04.000Z"),
    });

    const result = await coordinator.run();

    expect(result).toMatchObject({
      generation: 7n,
      manifestHash: expect.stringMatching(/^[a-f0-9]{64}$/),
      externalAnchorRef: "anchor://generation/7",
    });
    expect(recordArtifacts).toHaveBeenCalledWith(
      expect.objectContaining({
        generation: 7n,
        postgresSnapshotId: "pg-backup-7",
        dorisSnapshotId: "doris-snapshot-7",
        keyId: "checkpoint-signing-key-2026-07",
        signature: "signed-manifest",
      }),
    );
    expect(publishLatest).toHaveBeenCalledWith(
      expect.objectContaining({
        generation: 7n,
        predecessorHash: "predecessor-hash",
      }),
    );
    expect(seal).toHaveBeenCalledWith(
      expect.objectContaining({
        generation: 7n,
        externalAnchorRef: "anchor://generation/7",
      }),
    );
  });

  it("aborts an unanchored checkpoint when the pre-cut drain times out", async () => {
    let currentTime = Date.parse("2026-07-18T12:00:04.000Z");
    const abort = vi.fn().mockResolvedValue(true);
    const coordinator = new AnalyticsCheckpointCoordinator({
      leaseOwner: "checkpoint-worker",
      leaseMs: 60_000,
      timeoutMs: 1_500,
      pollIntervalMs: 1_000,
      repository: {
        findPendingAnchor: vi.fn().mockResolvedValue(null),
        claimAnchorReconciliation: vi.fn(),
        begin: vi.fn().mockResolvedValue(checkpoint()),
        renew: vi.fn().mockResolvedValue(true),
        drainState: vi.fn().mockResolvedValue({
          nonterminalOperations: 1,
          nonterminalLoads: 1,
          nonterminalDeletions: 0,
          drained: false,
        }),
        recordArtifacts: vi.fn(),
        seal: vi.fn(),
        abort,
      },
      postgres: { capture: vi.fn() },
      doris: { capture: vi.fn() },
      lifecycle: { capture: vi.fn() },
      signer: { keyId: "key", sign: vi.fn() },
      anchor: {
        publishLatest: vi.fn(),
        readLatest: vi.fn().mockResolvedValue(null),
      },
      wait: vi.fn().mockImplementation(async (milliseconds: number) => {
        currentTime += milliseconds;
      }),
      now: () => new Date(currentTime),
    });

    await expect(coordinator.run()).rejects.toThrow(
      "Analytics checkpoint drain timed out",
    );
    expect(abort).toHaveBeenCalledWith(
      expect.objectContaining({ reasonCode: "CHECKPOINT_DRAIN_TIMEOUT" }),
    );
  });

  it("rejects tampering and old-valid rollback before restore mutation", async () => {
    const manifest = { generation: "7", artifacts: { postgres: "pg-7" } };
    const manifestHash = canonicalCheckpointManifestHash(manifest);
    const sealed = {
      ...checkpoint(),
      status: "SEALED",
      manifest,
      manifestHash,
      signature: "valid-signature",
      keyId: "key-7",
      artifactDigests: { postgres: "a".repeat(64) },
      externalAnchorRef: "anchor://generation/7",
      sealedAt: new Date("2026-07-18T12:03:00.000Z"),
    } as unknown as AnalyticsCheckpointGeneration;
    const verifySignature = vi.fn().mockResolvedValue(true);

    await expect(
      verifyAnalyticsCheckpointForRestore({
        checkpoint: sealed,
        latestAnchor: {
          generation: 7n,
          manifestHash,
          reference: "anchor://generation/7",
        },
        expectedPredecessorHash: "predecessor-hash",
        actualArtifactDigests: { postgres: "a".repeat(64) },
        verifySignature,
      }),
    ).resolves.toEqual(manifest);

    await expect(
      verifyAnalyticsCheckpointForRestore({
        checkpoint: sealed,
        latestAnchor: {
          generation: 8n,
          manifestHash: "f".repeat(64),
          reference: "anchor://generation/8",
        },
        expectedPredecessorHash: "predecessor-hash",
        actualArtifactDigests: { postgres: "a".repeat(64) },
        verifySignature,
      }),
    ).rejects.toThrow("external anchor");

    await expect(
      verifyAnalyticsCheckpointForRestore({
        checkpoint: { ...sealed, manifest: { generation: "tampered" } },
        latestAnchor: {
          generation: 7n,
          manifestHash,
          reference: "anchor://generation/7",
        },
        expectedPredecessorHash: "predecessor-hash",
        actualArtifactDigests: { postgres: "a".repeat(64) },
        verifySignature,
      }),
    ).rejects.toThrow("manifest digest");
  });
});
