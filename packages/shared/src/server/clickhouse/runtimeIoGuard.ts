import type { ClickHouseClient } from "@clickhouse/client";

import {
  assertAnalyticsRuntimeIoAllowed,
  withAnalyticsRuntimeIoAbortSignal,
} from "../analytics-persistence/analyticsRuntimeIoFence";

const ABORTABLE_METHODS = new Set<PropertyKey>([
  "query",
  "command",
  "exec",
  "insert",
  "ping",
]);

export function guardClickHouseClient(
  client: ClickHouseClient,
  requestTimeoutMs: number,
): ClickHouseClient {
  return new Proxy(client, {
    get(target, property) {
      const value = Reflect.get(target, property, target) as unknown;
      if (typeof value !== "function") return value;

      return (...args: unknown[]) => {
        if (property !== "close" && property !== Symbol.asyncDispose) {
          assertAnalyticsRuntimeIoAllowed();
        }
        if (!ABORTABLE_METHODS.has(property)) {
          return Reflect.apply(value, target, args);
        }
        const request =
          typeof args[0] === "object" && args[0] !== null
            ? (args[0] as Record<string, unknown>)
            : {};
        const callerSignal =
          request.abort_signal instanceof AbortSignal
            ? request.abort_signal
            : undefined;
        return withAnalyticsRuntimeIoAbortSignal({
          timeoutMs: requestTimeoutMs,
          signal: callerSignal,
          execute: (signal) =>
            Promise.resolve(
              Reflect.apply(value, target, [
                { ...request, abort_signal: signal },
                ...args.slice(1),
              ]),
            ),
        });
      };
    },
  });
}
