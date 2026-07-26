import { beforeEach, describe, expect, it, vi } from "vitest";

import {
  armAnalyticsRuntimeIoLease,
  assertAnalyticsRuntimeIoAllowed,
  fenceAnalyticsRuntimeIo,
  resetAnalyticsRuntimeIoFenceForTests,
  withAnalyticsRuntimeIoAbortSignal,
} from "./analyticsRuntimeIoFence";

describe("withAnalyticsRuntimeIoAbortSignal", () => {
  beforeEach(() => {
    resetAnalyticsRuntimeIoFenceForTests();
  });

  it("fences resumed work synchronously after its local lease deadline", () => {
    armAnalyticsRuntimeIoLease({
      startedAtMonotonicMs: performance.now() - 2_000,
      leaseMs: 1_000,
    });

    expect(() => assertAnalyticsRuntimeIoAllowed()).toThrow(
      expect.objectContaining({
        code: "ANALYTICS_UNAVAILABLE",
        tags: { reasonCode: "RUNTIME_LEASE_FENCED" },
      }),
    );
  });

  it("rejects before starting I/O when the runtime is already fenced", async () => {
    const execute = vi.fn(async () => "unreachable");
    fenceAnalyticsRuntimeIo();

    await expect(
      withAnalyticsRuntimeIoAbortSignal({ timeoutMs: 1_000, execute }),
    ).rejects.toMatchObject({
      code: "ANALYTICS_UNAVAILABLE",
      tags: { reasonCode: "RUNTIME_LEASE_FENCED" },
    });
    expect(execute).not.toHaveBeenCalled();
  });

  it("aborts in-flight I/O immediately when the runtime is fenced", async () => {
    let started: (() => void) | undefined;
    const hasStarted = new Promise<void>((resolve) => {
      started = resolve;
    });
    const operation = withAnalyticsRuntimeIoAbortSignal({
      timeoutMs: 60_000,
      execute: (signal) =>
        new Promise<never>((_resolve, reject) => {
          signal.addEventListener("abort", () => reject(signal.reason), {
            once: true,
          });
          started?.();
        }),
    });
    await hasStarted;

    fenceAnalyticsRuntimeIo();

    await expect(operation).rejects.toMatchObject({
      code: "ANALYTICS_UNAVAILABLE",
      tags: { reasonCode: "RUNTIME_LEASE_FENCED" },
    });
    expect(globalThis.analyticsRuntimeIoFenceListeners?.size).toBe(0);
  });
});
