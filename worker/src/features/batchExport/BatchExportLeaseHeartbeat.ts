export async function withBatchExportLeaseHeartbeat<T>(input: {
  readonly intervalMs: number;
  readonly renew: () => Promise<unknown>;
  readonly run: (signal: AbortSignal) => Promise<T>;
}): Promise<T> {
  if (!Number.isSafeInteger(input.intervalMs) || input.intervalMs < 100) {
    throw new TypeError("Invalid batch export lease heartbeat interval");
  }

  const abortController = new AbortController();
  let renewalFailure: unknown;
  let renewal = Promise.resolve();
  const timer = setInterval(() => {
    renewal = renewal.then(async () => {
      if (renewalFailure !== undefined) return;
      try {
        await input.renew();
      } catch (error) {
        renewalFailure = error;
        abortController.abort(error);
      }
    });
  }, input.intervalMs);
  timer.unref?.();

  let result: T | undefined;
  let operationFailure: unknown;
  try {
    result = await input.run(abortController.signal);
  } catch (error) {
    operationFailure = error;
  } finally {
    clearInterval(timer);
    await renewal;
  }

  if (operationFailure !== undefined) throw operationFailure;
  if (renewalFailure !== undefined) throw renewalFailure;
  return result as T;
}
