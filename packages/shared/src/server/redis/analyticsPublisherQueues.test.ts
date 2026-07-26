import { EventEmitter } from "node:events";

import { beforeEach, describe, expect, it, vi } from "vitest";

const publisherOptions = vi.hoisted(() => vi.fn());
const createdQueues = vi.hoisted(() => [] as Array<{ name: string }>);

vi.mock("bullmq", async () => {
  const { EventEmitter } = await import("node:events");

  return {
    Queue: class QueueStub extends EventEmitter {
      constructor(name: string) {
        super();
        createdQueues.push({ name });
      }
    },
  };
});

vi.mock("./redis", () => ({
  createAnalyticsQueuePublisherOptionsWithRedis: publisherOptions,
  redisErrorForLogging: (error: unknown) => error,
}));

vi.mock("../logger", () => ({
  logger: { error: vi.fn() },
}));

describe.each([
  [
    "BatchActionQueue",
    async () => (await import("./batchActionQueue.js")).BatchActionQueue,
  ],
  [
    "ScoreDeleteQueue",
    async () => (await import("./scoreDelete.js")).ScoreDeleteQueue,
  ],
] as const)("%s producer lifecycle", (_className, loadQueueClass) => {
  beforeEach(() => {
    publisherOptions.mockReset();
    createdQueues.length = 0;
    vi.resetModules();
  });

  it("recreates the cached producer after its Redis connection ends", async () => {
    const firstConnection = new EventEmitter();
    const secondConnection = new EventEmitter();
    publisherOptions
      .mockReturnValueOnce({ connection: firstConnection })
      .mockReturnValueOnce({ connection: secondConnection });

    const QueueClass = await loadQueueClass();
    const first = QueueClass.getInstance();

    expect(QueueClass.getInstance()).toBe(first);
    expect(createdQueues).toHaveLength(1);

    firstConnection.emit("end");
    const replacement = QueueClass.getInstance();

    expect(replacement).not.toBe(first);
    expect(createdQueues).toHaveLength(2);

    // A delayed duplicate event from the old connection must not evict the
    // producer that replaced it.
    firstConnection.emit("end");
    expect(QueueClass.getInstance()).toBe(replacement);
    expect(createdQueues).toHaveLength(2);
  }, 15_000);
});
