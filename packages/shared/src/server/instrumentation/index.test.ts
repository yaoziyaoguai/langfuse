import { context } from "@opentelemetry/api";
import { AsyncLocalStorageContextManager } from "@opentelemetry/context-async-hooks";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { normalizeAnalyticsQueryTags } from "../analyticsQueryTags";
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
      analytics: { surface: "worker", route: "langfuse.queue.monitor" },
    });

    const tags = await context.with(workerContext, () =>
      instrumentAsync(
        { name: "process monitor", startNewTrace: true },
        async () => normalizeAnalyticsQueryTags(),
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
      analytics: { surface: "worker", route: "langfuse.queue.monitor" },
    });

    const tags = context.with(workerContext, () =>
      instrumentSync({ name: "process monitor", startNewTrace: true }, () =>
        normalizeAnalyticsQueryTags(),
      ),
    );

    expect(tags).toMatchObject({
      surface: "worker",
      route: "langfuse.queue.monitor",
      projectId: "project-1",
    });
  });
});
