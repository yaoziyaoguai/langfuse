export async function withAnalyticsIntegrationLeaseHeartbeat<T>(input: {
  readonly intervalMs: number;
  readonly renew: () => Promise<unknown>;
  readonly run: () => Promise<T>;
}): Promise<T> {
  if (!Number.isSafeInteger(input.intervalMs) || input.intervalMs < 100) {
    throw new TypeError(
      "Invalid analytics integration lease heartbeat interval",
    );
  }

  let renewalFailure: unknown;
  let renewal = Promise.resolve();
  const timer = setInterval(() => {
    renewal = renewal.then(async () => {
      if (renewalFailure !== undefined) return;
      try {
        await input.renew();
      } catch (error) {
        renewalFailure = error;
      }
    });
  }, input.intervalMs);
  timer.unref?.();

  let result: T | undefined;
  let operationFailure: unknown;
  try {
    result = await input.run();
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
