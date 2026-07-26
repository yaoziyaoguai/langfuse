import { AnalyticsPersistenceError } from "./errors";

declare global {
  var analyticsRuntimeIoFenced: boolean | undefined;
  var analyticsRuntimeIoFenceListeners: Set<() => void> | undefined;
  var analyticsRuntimeIoLeaseDeadlineMonotonicMs: number | undefined;
  var analyticsRuntimeIoLeaseDeadlineTimer:
    | ReturnType<typeof setTimeout>
    | undefined;
}

globalThis.analyticsRuntimeIoFenced ??= false;
globalThis.analyticsRuntimeIoFenceListeners ??= new Set();

function clearAnalyticsRuntimeIoLeaseTimer(): void {
  if (globalThis.analyticsRuntimeIoLeaseDeadlineTimer) {
    clearTimeout(globalThis.analyticsRuntimeIoLeaseDeadlineTimer);
  }
  globalThis.analyticsRuntimeIoLeaseDeadlineTimer = undefined;
}

export function fenceAnalyticsRuntimeIo(): void {
  clearAnalyticsRuntimeIoLeaseTimer();
  globalThis.analyticsRuntimeIoFenced = true;
  const listeners = [...(globalThis.analyticsRuntimeIoFenceListeners ?? [])];
  globalThis.analyticsRuntimeIoFenceListeners?.clear();
  for (const listener of listeners) {
    try {
      listener();
    } catch {
      // Fencing must notify every in-flight transport even if one abort fails.
    }
  }
}

export function isAnalyticsRuntimeIoFenced(): boolean {
  return globalThis.analyticsRuntimeIoFenced === true;
}

/**
 * The database lease is the durable switch fence. This local monotonic deadline
 * is its conservative process-side proof: a paused process cannot issue one
 * more request after the database lease may have expired and a switch may have
 * advanced the deployment generation.
 */
export function armAnalyticsRuntimeIoLease(input: {
  readonly startedAtMonotonicMs: number;
  readonly leaseMs: number;
}): void {
  if (
    !Number.isFinite(input.startedAtMonotonicMs) ||
    !Number.isSafeInteger(input.leaseMs) ||
    input.leaseMs < 1_000
  ) {
    throw new TypeError("Invalid analytics runtime I/O lease");
  }
  if (isAnalyticsRuntimeIoFenced()) {
    assertAnalyticsRuntimeIoAllowed();
  }

  clearAnalyticsRuntimeIoLeaseTimer();
  const deadline = input.startedAtMonotonicMs + input.leaseMs;
  globalThis.analyticsRuntimeIoLeaseDeadlineMonotonicMs = deadline;
  const remainingMs = deadline - performance.now();
  if (remainingMs <= 0) {
    fenceAnalyticsRuntimeIo();
    return;
  }
  const timer = setTimeout(fenceAnalyticsRuntimeIo, remainingMs);
  timer.unref();
  globalThis.analyticsRuntimeIoLeaseDeadlineTimer = timer;
}

export function resetAnalyticsRuntimeIoFenceForTests(): void {
  clearAnalyticsRuntimeIoLeaseTimer();
  globalThis.analyticsRuntimeIoFenced = false;
  globalThis.analyticsRuntimeIoLeaseDeadlineMonotonicMs = undefined;
  globalThis.analyticsRuntimeIoFenceListeners?.clear();
}

export function assertAnalyticsRuntimeIoAllowed(): void {
  const deadline = globalThis.analyticsRuntimeIoLeaseDeadlineMonotonicMs;
  if (
    !isAnalyticsRuntimeIoFenced() &&
    (deadline === undefined || performance.now() < deadline)
  ) {
    return;
  }
  if (!isAnalyticsRuntimeIoFenced()) fenceAnalyticsRuntimeIo();

  throw new AnalyticsPersistenceError("ANALYTICS_UNAVAILABLE", true, {
    tags: { reasonCode: "RUNTIME_LEASE_FENCED" },
  });
}

export function onAnalyticsRuntimeIoFenced(listener: () => void): () => void {
  if (isAnalyticsRuntimeIoFenced()) {
    listener();
    return () => undefined;
  }
  const listeners = (globalThis.analyticsRuntimeIoFenceListeners ??= new Set());
  listeners.add(listener);
  return () => listeners.delete(listener);
}

export async function withAnalyticsRuntimeIoAbortSignal<T>(input: {
  readonly timeoutMs: number;
  readonly signal?: AbortSignal;
  readonly execute: (signal: AbortSignal) => Promise<T>;
}): Promise<T> {
  assertAnalyticsRuntimeIoAllowed();
  const controller = new AbortController();
  const forwardCallerAbort = () => controller.abort(input.signal?.reason);
  if (input.signal?.aborted) forwardCallerAbort();
  else
    input.signal?.addEventListener("abort", forwardCallerAbort, {
      once: true,
    });
  const timeout = setTimeout(
    () => controller.abort(new Error("Analytics I/O timed out")),
    input.timeoutMs,
  );
  timeout.unref();
  const removeFenceListener = onAnalyticsRuntimeIoFenced(() => {
    let reason: unknown;
    try {
      assertAnalyticsRuntimeIoAllowed();
    } catch (error) {
      reason = error;
    }
    controller.abort(reason);
  });

  try {
    const result = await input.execute(controller.signal);
    assertAnalyticsRuntimeIoAllowed();
    return result;
  } finally {
    clearTimeout(timeout);
    removeFenceListener();
    input.signal?.removeEventListener("abort", forwardCallerAbort);
  }
}
