import type {
  AnalyticsBackendDeploymentState,
  PrismaClient,
} from "@prisma/client";
import { describe, expect, it, vi } from "vitest";

import {
  ANALYTICS_DEPLOYMENT_TRANSACTION_OPTIONS,
  switchAnalyticsBackend,
} from "./analyticsBackendDeployment";
import {
  executeAdoptExistingBackendOperator,
  executeSwitchBackendOperator,
  formatAnalyticsBackendOperatorFailure,
  formatAnalyticsBackendOperatorSuccess,
  type AdoptExistingBackendTransition,
  type SwitchBackendTransition,
} from "../../../scripts/analytics-backend-operator";
import type { AnalyticsScoreDeletionQueueDrainScope } from "../redis/analyticsScoreDeletionDrain";
import { createEmptyAnalyticsQueueDrainEvidence } from "../redis/analyticsQueueDrain.test-helper";

const digest = (value: string): string => value.repeat(64);
const queueNamespaceFingerprint = digest("d");
const inventory = [
  { instanceId: "web-1", component: "web" as const },
  { instanceId: "worker-1", component: "worker" as const },
];

const emptyQueueDrainEvidence = (
  scope: AnalyticsScoreDeletionQueueDrainScope,
) =>
  createEmptyAnalyticsQueueDrainEvidence({
    scope,
    queueNamespaceFingerprint,
  });

const adoptArgs = [
  "--expected-backend",
  "doris",
  "--foundation-contract-version",
  "1",
  "--expected-inventory-file",
  "/run/operator/inventory.json",
  "--expected-inventory-digest",
  digest("1"),
  "--drain-attestation-digest",
  digest("2"),
  "--deny-probe-attestation-digest",
  digest("3"),
] as const;

const switchArgs = [
  "--expected-backend",
  "doris",
  "--expected-generation",
  "7",
  "--target-backend",
  "clickhouse",
  "--target-foundation-contract-version",
  "1",
  "--expected-inventory-file",
  "/run/operator/inventory.json",
  "--expected-inventory-digest",
  digest("1"),
  "--expected-source-emptiness-evidence-digest",
  digest("4"),
  "--expected-target-emptiness-evidence-digest",
  digest("5"),
  "--drain-attestation-digest",
  digest("6"),
  "--deny-probe-attestation-digest",
  digest("7"),
] as const;

function marker(
  backend: "CLICKHOUSE" | "DORIS",
  generation: bigint,
): AnalyticsBackendDeploymentState {
  return {
    id: "global",
    backend,
    generation,
    workloadEpochFingerprint: digest("8"),
    queueNamespaceFingerprint,
    foundationContractVersion: 1,
    attestationDigest: digest("9"),
    createdAt: new Date("2026-07-22T00:00:00.000Z"),
    updatedAt: new Date("2026-07-22T00:00:00.000Z"),
  };
}

describe("adopt-existing-backend operator", () => {
  it("reads the workload epoch only from env and passes only its fingerprint", async () => {
    const rawEpoch = "raw-adoption-epoch-must-not-leak";
    const verifyScoreDeletionQueuesEmpty = vi.fn(
      async (scope: AnalyticsScoreDeletionQueueDrainScope) =>
        emptyQueueDrainEvidence(scope),
    );
    const adopt = vi.fn<AdoptExistingBackendTransition>(async (input) => {
      await input.verifyScoreDeletionQueuesEmpty({
        backend: "doris",
        deploymentGeneration: 0n,
        workloadEpochFingerprint: digest("a"),
      });
      return marker("DORIS", 1n);
    });

    const result = await executeAdoptExistingBackendOperator({
      argv: adoptArgs,
      env: { LANGFUSE_ANALYTICS_WORKLOAD_EPOCH: rawEpoch },
      dependencies: {
        readTextFile: vi.fn(async () => JSON.stringify(inventory)),
        fingerprintWorkloadEpoch: vi.fn(() => digest("a")),
        digestRuntimeInventory: vi.fn(() => digest("1")),
        verifyScoreDeletionQueuesEmpty,
        adopt,
      },
    });

    expect(result).toEqual(marker("DORIS", 1n));
    expect(adopt).toHaveBeenCalledWith(
      expect.objectContaining({
        expectedBackend: "doris",
        workloadEpochFingerprint: digest("a"),
        expectedInventory: inventory,
        expectedInventoryDigest: digest("1"),
        verifyScoreDeletionQueuesEmpty,
        drainAttestationDigest: digest("2"),
        denyProbeAttestationDigest: digest("3"),
      }),
    );
    expect(verifyScoreDeletionQueuesEmpty).toHaveBeenCalledWith({
      backend: "doris",
      deploymentGeneration: 0n,
      workloadEpochFingerprint: digest("a"),
    });
    expect(JSON.stringify(adopt.mock.calls)).not.toContain(rawEpoch);
  });

  it("supports a mounted epoch file and rejects ambiguous epoch sources", async () => {
    const readTextFile = vi.fn(async (path: string) =>
      path.endsWith("inventory.json")
        ? JSON.stringify(inventory)
        : "mounted-adoption-epoch\n",
    );
    const adopt = vi.fn(async () => marker("DORIS", 1n));
    const dependencies = {
      readTextFile,
      fingerprintWorkloadEpoch: vi.fn(() => digest("a")),
      digestRuntimeInventory: vi.fn(() => digest("1")),
      verifyScoreDeletionQueuesEmpty: vi.fn(
        async (scope: AnalyticsScoreDeletionQueueDrainScope) =>
          emptyQueueDrainEvidence(scope),
      ),
      adopt,
    };

    await expect(
      executeAdoptExistingBackendOperator({
        argv: adoptArgs,
        env: {
          LANGFUSE_ANALYTICS_WORKLOAD_EPOCH_FILE:
            "/run/secrets/analytics-workload-epoch",
        },
        dependencies,
      }),
    ).resolves.toMatchObject({ backend: "DORIS" });
    expect(dependencies.fingerprintWorkloadEpoch).toHaveBeenCalledWith(
      "mounted-adoption-epoch",
    );

    await expect(
      executeAdoptExistingBackendOperator({
        argv: adoptArgs,
        env: {
          LANGFUSE_ANALYTICS_WORKLOAD_EPOCH: "direct",
          LANGFUSE_ANALYTICS_WORKLOAD_EPOCH_FILE: "/run/secrets/epoch",
        },
        dependencies,
      }),
    ).rejects.toThrow(/exactly one workload epoch source/i);
  });

  it("rejects epoch argv and inventory digest mismatches before transition", async () => {
    const adopt = vi.fn(async () => marker("DORIS", 1n));
    const dependencies = {
      readTextFile: vi.fn(async () => JSON.stringify(inventory)),
      fingerprintWorkloadEpoch: vi.fn(() => digest("a")),
      digestRuntimeInventory: vi.fn(() => digest("0")),
      verifyScoreDeletionQueuesEmpty: vi.fn(
        async (scope: AnalyticsScoreDeletionQueueDrainScope) =>
          emptyQueueDrainEvidence(scope),
      ),
      adopt,
    };

    await expect(
      executeAdoptExistingBackendOperator({
        argv: [...adoptArgs, "--workload-epoch", "argv-secret"],
        env: {},
        dependencies,
      }),
    ).rejects.toThrow("Invalid adopt-existing-backend arguments");
    await expect(
      executeAdoptExistingBackendOperator({
        argv: adoptArgs,
        env: { LANGFUSE_ANALYTICS_WORKLOAD_EPOCH: "epoch" },
        dependencies,
      }),
    ).rejects.toThrow(/inventory digest does not match/i);
    expect(adopt).not.toHaveBeenCalled();
  });
});

describe("switch-backend operator", () => {
  it("probes the explicit source and target before completing the transition", async () => {
    const events: string[] = [];
    const verifyScoreDeletionQueuesEmpty = vi.fn(
      async (scope: AnalyticsScoreDeletionQueueDrainScope) => {
        events.push("queue-probe");
        return emptyQueueDrainEvidence(scope);
      },
    );
    const probeBackendEmptiness = vi.fn(async () => {
      events.push("probe");
      return {
        source: {
          backend: "doris" as const,
          empty: true,
          evidenceDigest: digest("4"),
        },
        target: {
          backend: "clickhouse" as const,
          empty: true,
          evidenceDigest: digest("5"),
        },
      };
    });
    const transition = vi.fn<SwitchBackendTransition>(async (input) => {
      await input.verifyBackendEmptiness();
      await input.verifyScoreDeletionQueuesEmpty({
        backend: "doris",
        deploymentGeneration: 7n,
        workloadEpochFingerprint: digest("b"),
      });
      events.push("transition");
      return marker("CLICKHOUSE", 8n);
    });

    await expect(
      executeSwitchBackendOperator({
        argv: switchArgs,
        env: {
          LANGFUSE_ANALYTICS_EXPECTED_WORKLOAD_EPOCH: "source-epoch",
          LANGFUSE_ANALYTICS_TARGET_WORKLOAD_EPOCH_FILE:
            "/run/secrets/target-epoch",
        },
        dependencies: {
          readTextFile: vi.fn(async (path: string) =>
            path.endsWith("inventory.json")
              ? JSON.stringify(inventory)
              : "target-epoch\n",
          ),
          fingerprintWorkloadEpoch: vi.fn((epoch: string) =>
            epoch === "source-epoch" ? digest("b") : digest("c"),
          ),
          digestRuntimeInventory: vi.fn(() => digest("1")),
          probeBackendEmptiness,
          verifyScoreDeletionQueuesEmpty,
          switchBackend: transition,
        },
      }),
    ).resolves.toMatchObject({ backend: "CLICKHOUSE", generation: 8n });

    expect(events).toEqual(["probe", "queue-probe", "transition"]);
    expect(probeBackendEmptiness).toHaveBeenCalledWith({
      sourceBackend: "doris",
      targetBackend: "clickhouse",
    });
    expect(verifyScoreDeletionQueuesEmpty).toHaveBeenCalledWith({
      backend: "doris",
      deploymentGeneration: 7n,
      workloadEpochFingerprint: digest("b"),
    });
    expect(transition).toHaveBeenCalledWith(
      expect.objectContaining({
        expectedBackend: "doris",
        expectedGeneration: 7n,
        expectedWorkloadEpochFingerprint: digest("b"),
        targetBackend: "clickhouse",
        targetWorkloadEpochFingerprint: digest("c"),
        expectedQuiescedInventory: inventory,
      }),
    );
  });

  it("fails closed on non-empty or unexpected probe evidence", async () => {
    const transitionCompleted = vi.fn();
    const switchBackend = vi.fn<SwitchBackendTransition>(async (input) => {
      await input.verifyBackendEmptiness();
      transitionCompleted();
      return marker("CLICKHOUSE", 8n);
    });
    const baseDependencies = {
      readTextFile: vi.fn(async () => JSON.stringify(inventory)),
      fingerprintWorkloadEpoch: vi.fn((epoch: string) =>
        epoch === "source" ? digest("b") : digest("c"),
      ),
      digestRuntimeInventory: vi.fn(() => digest("1")),
      verifyScoreDeletionQueuesEmpty: vi.fn(
        async (scope: AnalyticsScoreDeletionQueueDrainScope) =>
          emptyQueueDrainEvidence(scope),
      ),
      switchBackend,
    };
    const env = {
      LANGFUSE_ANALYTICS_EXPECTED_WORKLOAD_EPOCH: "source",
      LANGFUSE_ANALYTICS_TARGET_WORKLOAD_EPOCH: "target",
    };

    await expect(
      executeSwitchBackendOperator({
        argv: switchArgs,
        env,
        dependencies: {
          ...baseDependencies,
          probeBackendEmptiness: vi.fn(async () => ({
            source: {
              backend: "doris" as const,
              empty: false,
              evidenceDigest: digest("4"),
            },
            target: {
              backend: "clickhouse" as const,
              empty: true,
              evidenceDigest: digest("5"),
            },
          })),
        },
      }),
    ).rejects.toThrow(/source and target must both be empty/i);
    await expect(
      executeSwitchBackendOperator({
        argv: switchArgs,
        env,
        dependencies: {
          ...baseDependencies,
          probeBackendEmptiness: vi.fn(async () => ({
            source: {
              backend: "doris" as const,
              empty: true,
              evidenceDigest: digest("d"),
            },
            target: {
              backend: "clickhouse" as const,
              empty: true,
              evidenceDigest: digest("5"),
            },
          })),
        },
      }),
    ).rejects.toThrow(/source emptiness evidence digest does not match/i);
    expect(transitionCompleted).not.toHaveBeenCalled();
  });

  it("rejects reuse of the current workload epoch before probing", async () => {
    const probeBackendEmptiness = vi.fn();
    const switchBackend = vi.fn();

    await expect(
      executeSwitchBackendOperator({
        argv: switchArgs,
        env: {
          LANGFUSE_ANALYTICS_EXPECTED_WORKLOAD_EPOCH: "same-epoch",
          LANGFUSE_ANALYTICS_TARGET_WORKLOAD_EPOCH: "same-epoch",
        },
        dependencies: {
          readTextFile: vi.fn(async () => JSON.stringify(inventory)),
          fingerprintWorkloadEpoch: vi.fn(() => digest("e")),
          digestRuntimeInventory: vi.fn(() => digest("1")),
          probeBackendEmptiness,
          verifyScoreDeletionQueuesEmpty: vi.fn(),
          switchBackend,
        },
      }),
    ).rejects.toThrow(/new workload epoch/i);
    expect(probeBackendEmptiness).not.toHaveBeenCalled();
    expect(switchBackend).not.toHaveBeenCalled();
  });
});

describe("analytics backend operator output", () => {
  it("emits only non-secret transition fields and redacts failures", () => {
    const rawEpoch = "never-print-this-epoch";
    const success = formatAnalyticsBackendOperatorSuccess(
      "switch-backend",
      marker("CLICKHOUSE", 8n),
    );
    const failure = formatAnalyticsBackendOperatorFailure(
      new Error(`backend error: ${rawEpoch}`),
    );

    expect(success).toBe(
      '{"command":"switch-backend","backend":"clickhouse","generation":"8"}\n',
    );
    expect(success).not.toContain(rawEpoch);
    expect(failure).toBe(
      "Analytics backend operator command failed (details redacted)\n",
    );
    expect(failure).not.toContain(rawEpoch);
  });
});

describe("analytics backend deployment switch invariant", () => {
  it("allows enough transaction time for bounded backend and queue probes", () => {
    expect(ANALYTICS_DEPLOYMENT_TRANSACTION_OPTIONS).toEqual({
      isolationLevel: "ReadCommitted",
      maxWait: 120_000,
      timeout: 180_000,
    });
  });

  it("rejects the target workload epoch when it matches the current epoch", async () => {
    const transaction = vi.fn(async () => {
      throw new Error("transaction should not be entered");
    });
    const client = { $transaction: transaction } as unknown as PrismaClient;
    const verifyBackendEmptiness = vi.fn();
    const verifyScoreDeletionQueuesEmpty = vi.fn();

    await expect(
      switchAnalyticsBackend({
        client,
        expectedBackend: "doris",
        expectedGeneration: 7n,
        expectedWorkloadEpochFingerprint: digest("a"),
        targetBackend: "clickhouse",
        targetWorkloadEpochFingerprint: digest("a"),
        targetFoundationContractVersion: 1,
        expectedQuiescedInventory: inventory,
        verifyBackendEmptiness,
        verifyScoreDeletionQueuesEmpty,
        externalDrainAttestationDigest: digest("b"),
        denyProbeAttestationDigest: digest("c"),
      }),
    ).rejects.toThrow(/new workload epoch/i);
    expect(transaction).not.toHaveBeenCalled();
    expect(verifyBackendEmptiness).not.toHaveBeenCalled();
    expect(verifyScoreDeletionQueuesEmpty).not.toHaveBeenCalled();
  });
});
