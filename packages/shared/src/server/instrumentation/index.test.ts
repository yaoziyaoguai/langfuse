import { context, trace, type Span } from "@opentelemetry/api";
import { AsyncLocalStorageContextManager } from "@opentelemetry/context-async-hooks";
import { afterAll, beforeAll, describe, expect, it, vi } from "vitest";
import { normalizeAnalyticsQueryTags } from "../analyticsQueryTags";
import { normalizeClickHouseQueryTags } from "../clickhouse/queryTags";
import { contextWithLangfuseProps } from "../headerPropagation";
import { instrumentAsync, instrumentSync } from ".";

describe("instrumentation baggage propagation", () => {
  // Baggage only propagates through context.with once a manager is registered.
  const contextManager = new AsyncLocalStorageContextManager();

  beforeAll(() => {
    contextManager.enable();
    context.setGlobalContextManager(contextManager);
  });

  afterAll(() => {
    context.disable();
  });

  it("instrumentAsync keeps worker surface/route across startNewTrace", async () => {
    const workerContext = contextWithLangfuseProps({
      projectId: "project-1",
      clickhouse: { surface: "worker", route: "langfuse.queue.monitor" },
    });

    const tags = await context.with(workerContext, () =>
      instrumentAsync(
        { name: "process monitor", startNewTrace: true },
        async () => normalizeClickHouseQueryTags(),
      ),
    );

    expect(tags).toMatchObject({
      surface: "worker",
      route: "langfuse.queue.monitor",
      projectId: "project-1",
    });
  });

  it("instrumentSync keeps worker surface/route across startNewTrace", () => {
    const workerContext = contextWithLangfuseProps({
      projectId: "project-1",
      clickhouse: { surface: "worker", route: "langfuse.queue.monitor" },
    });

    const tags = context.with(workerContext, () =>
      instrumentSync({ name: "process monitor", startNewTrace: true }, () =>
        normalizeClickHouseQueryTags(),
      ),
    );

    expect(tags).toMatchObject({
      surface: "worker",
      route: "langfuse.queue.monitor",
      projectId: "project-1",
    });
  });

  it("propagates neutral analytics tags alongside ClickHouse tags", () => {
    const workerContext = contextWithLangfuseProps({
      projectId: "project-1",
      analytics: { surface: "worker", route: "langfuse.queue.analytics" },
      clickhouse: { surface: "worker", route: "langfuse.queue.clickhouse" },
    });

    const tags = context.with(workerContext, () => ({
      analytics: normalizeAnalyticsQueryTags(),
      clickhouse: normalizeClickHouseQueryTags(),
    }));

    expect(tags.analytics).toMatchObject({
      surface: "worker",
      route: "langfuse.queue.analytics",
      projectId: "project-1",
    });
    expect(tags.clickhouse).toMatchObject({
      surface: "worker",
      route: "langfuse.queue.clickhouse",
      projectId: "project-1",
    });
  });

  it("can suppress automatic exception recording for an async span", async () => {
    const recordException = vi.fn();
    const span = {
      end: vi.fn(),
      recordException,
      setAttribute: vi.fn(),
      setAttributes: vi.fn(),
      setStatus: vi.fn(),
    } as unknown as Span;
    const getTracer = vi.spyOn(trace, "getTracer").mockReturnValue({
      startActiveSpan: vi.fn((...args: unknown[]) =>
        (args.at(-1) as (activeSpan: Span) => Promise<unknown>)(span),
      ),
    } as never);
    const secretError = new Error(
      "postgresql://admin:password@db Authorization=Bearer token",
    );

    await expect(
      instrumentAsync(
        { name: "redacted-async", recordException: false },
        async () => {
          throw secretError;
        },
      ),
    ).rejects.toBe(secretError);

    expect(recordException).not.toHaveBeenCalled();
    getTracer.mockRestore();
  });

  it("can suppress automatic exception recording for a sync span", () => {
    const recordException = vi.fn();
    const span = {
      end: vi.fn(),
      recordException,
      setAttribute: vi.fn(),
      setAttributes: vi.fn(),
      setStatus: vi.fn(),
    } as unknown as Span;
    const getTracer = vi.spyOn(trace, "getTracer").mockReturnValue({
      startActiveSpan: vi.fn((...args: unknown[]) =>
        (args.at(-1) as (activeSpan: Span) => unknown)(span),
      ),
    } as never);
    const secretError = new Error("prompt=secret input=private");

    expect(() =>
      instrumentSync({ name: "redacted-sync", recordException: false }, () => {
        throw secretError;
      }),
    ).toThrow(secretError);

    expect(recordException).not.toHaveBeenCalled();
    getTracer.mockRestore();
  });
});
