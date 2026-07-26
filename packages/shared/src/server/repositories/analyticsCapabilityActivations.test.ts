import { beforeEach, describe, expect, it, vi } from "vitest";

const mocks = vi.hoisted(() => ({
  acquireDeploymentSharedLock: vi.fn(),
  lockDeploymentState: vi.fn(),
}));

vi.mock("../../db", () => ({ prisma: {} }));
vi.mock("./analyticsBackendDeployment", () => ({
  acquireAnalyticsDeploymentSharedLock: mocks.acquireDeploymentSharedLock,
  lockAnalyticsBackendDeploymentState: mocks.lockDeploymentState,
}));

import { disableAnalyticsCapability } from "./analyticsCapabilityActivations";

function harness() {
  const activation = {
    capability: "CORE_BATCH_EXPORTS",
    status: "DRAINING",
    deploymentGeneration: 1n,
    generation: 2n,
    contractVersion: 1,
    captureEnabled: false,
  };
  const update = vi.fn().mockResolvedValue({
    ...activation,
    status: "DISABLED",
  });
  const transaction = {
    $queryRaw: vi.fn().mockResolvedValue([{ locked: "true" }]),
    analyticsCapabilityActivation: {
      findUniqueOrThrow: vi.fn().mockResolvedValue(activation),
      update,
    },
  };
  const client = {
    $transaction: vi.fn(
      (execute: (value: typeof transaction) => Promise<unknown>) =>
        execute(transaction),
    ),
  };
  return { client, transaction, update };
}

describe("analytics capability disable", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mocks.lockDeploymentState.mockResolvedValue({
      backend: "DORIS",
      generation: 1n,
    });
  });

  it("does not disable while feature-owned durable work is pending", async () => {
    const { client, transaction, update } = harness();
    const verifyDurableDrain = vi
      .fn()
      .mockRejectedValue(new Error("pending export execution"));

    await expect(
      disableAnalyticsCapability({
        client: client as never,
        capability: "coreBatchExports",
        expectedDeploymentGeneration: 1n,
        expectedActivationGeneration: 2n,
        captureRequired: false,
        rescanRequired: false,
        verifyDurableDrain,
        now: new Date("2026-07-21T00:00:00.000Z"),
      }),
    ).rejects.toThrow("pending export execution");
    expect(verifyDurableDrain).toHaveBeenCalledWith(transaction, {
      capability: "coreBatchExports",
      deploymentGeneration: 1n,
      activationGeneration: 2n,
      capabilityContractVersion: 1,
    });
    expect(update).not.toHaveBeenCalled();
  });
});
