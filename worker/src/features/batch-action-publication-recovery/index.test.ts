import {
  BatchActionStatus,
  BatchEvalSourceTable,
  BatchTableNames,
} from "@langfuse/shared";
import { describe, expect, it, vi } from "vitest";

import {
  BatchActionPublicationRecoveryRunner,
  buildRecoveredBatchActionJob,
  processBatchActionPublicationRecoveryPage,
  publishRecoveredBatchAction,
} from ".";

const validMapping = {
  input: { mode: "full" },
  expectedOutput: { mode: "full" },
  metadata: { mode: "full" },
};

function queuedAction(
  overrides: Partial<{
    id: string;
    actionType: string;
    tableName: string;
    query: unknown;
    config: unknown;
  }> = {},
) {
  return {
    id: overrides.id ?? "batch-action-1",
    projectId: "project-1",
    actionType: overrides.actionType ?? "observation-add-to-dataset",
    tableName: overrides.tableName ?? BatchTableNames.Events,
    status: BatchActionStatus.Queued,
    query: overrides.query ?? {
      filter: [],
      orderBy: { column: "startTime", order: "DESC" },
    },
    config: overrides.config ?? {
      datasetId: "dataset-1",
      datasetName: "Dataset",
      mapping: validMapping,
    },
    createdAt: new Date("2026-07-22T08:00:00.000Z"),
  };
}

describe("batch action publication recovery", () => {
  it("keeps a queued intent after an uncertain add and republishes it on the next scan", async () => {
    const action = queuedAction();
    const publish = vi
      .fn()
      .mockRejectedValueOnce(new Error("Redis connection closed"))
      .mockResolvedValueOnce("published");
    const markFailed = vi.fn();

    await expect(
      processBatchActionPublicationRecoveryPage({
        backend: "doris",
        actions: [action],
        publish,
        markFailed,
      }),
    ).resolves.toMatchObject({ retryableFailures: 1 });
    expect(markFailed).not.toHaveBeenCalled();

    await expect(
      processBatchActionPublicationRecoveryPage({
        backend: "doris",
        actions: [action],
        publish,
        markFailed,
      }),
    ).resolves.toMatchObject({ retryableFailures: 0 });
    expect(publish).toHaveBeenCalledTimes(2);
    expect(publish.mock.calls[0]?.[0]).toEqual(publish.mock.calls[1]?.[0]);
    expect(publish.mock.calls[1]?.[0]).toMatchObject({ id: action.id });
  });

  it("rebuilds the same BullMQ identity and payload on repeated scans", () => {
    const action = queuedAction();

    const first = buildRecoveredBatchActionJob(action);
    const second = buildRecoveredBatchActionJob(action);

    expect(first).toEqual(second);
    expect(first.id).toBe(action.id);
    expect(first.timestamp).toEqual(action.createdAt);
    expect(first.payload).toMatchObject({
      actionId: "observation-add-to-dataset",
      batchActionId: action.id,
      cutoffCreatedAt: action.createdAt,
    });
  });

  it("marks malformed JSON terminally failed with a bounded diagnostic", async () => {
    const action = queuedAction({
      config: {
        datasetId: "dataset-1",
        datasetName: "Dataset",
        mapping: { input: { mode: "invalid" } },
      },
    });
    const publish = vi.fn();
    const markFailed = vi.fn().mockResolvedValue(undefined);

    await expect(
      processBatchActionPublicationRecoveryPage({
        backend: "doris",
        actions: [action],
        publish,
        markFailed,
      }),
    ).resolves.toMatchObject({ terminalFailures: 1 });

    expect(publish).not.toHaveBeenCalled();
    expect(markFailed).toHaveBeenCalledWith(
      action.id,
      action.projectId,
      expect.stringContaining("Invalid durable BatchAction publication intent"),
    );
    expect(String(markFailed.mock.calls[0]?.[2]).length).toBeLessThanOrEqual(
      2_000,
    );
  });

  it("rebuilds batch evaluation for the selected analytics backend", () => {
    const action = queuedAction({
      actionType: "observation-run-batched-evaluation",
      config: { evaluatorIds: ["evaluator-1"] },
    });

    expect(buildRecoveredBatchActionJob(action).payload).toEqual(
      expect.objectContaining({
        actionId: "observation-run-batched-evaluation",
        batchActionId: action.id,
        evaluatorIds: ["evaluator-1"],
        sourceTable: BatchEvalSourceTable.EVENTS,
      }),
    );
  });

  it("preserves experiment evaluation source during publication recovery", () => {
    const action = queuedAction({
      actionType: "observation-run-batched-evaluation",
      config: {
        evaluatorIds: ["evaluator-1"],
        sourceTable: BatchEvalSourceTable.EXPERIMENTS,
      },
    });

    expect(buildRecoveredBatchActionJob(action).payload).toEqual(
      expect.objectContaining({
        actionId: "observation-run-batched-evaluation",
        evaluatorIds: ["evaluator-1"],
        sourceTable: BatchEvalSourceTable.EXPERIMENTS,
      }),
    );
  });

  it("holds the admitted deployment generation through Queue.add", async () => {
    const action = queuedAction();
    const job = buildRecoveredBatchActionJob(action);
    const queue = {
      add: vi.fn().mockResolvedValue({
        getState: vi.fn().mockResolvedValue("waiting"),
      }),
    };
    const transaction = {
      batchAction: {
        findUnique: vi.fn().mockResolvedValue({
          status: BatchActionStatus.Queued,
          actionType: action.actionType,
        }),
      },
    };
    const client = {
      $transaction: vi.fn(async (operation) => operation(transaction)),
    };
    const lockAdmission = vi.fn().mockResolvedValue(undefined);

    await publishRecoveredBatchAction(
      {
        backend: "clickhouse",
        getAdmissionContext: () => ({
          runtimeLeaseId: "worker-runtime-1",
          backend: "clickhouse",
          deploymentGeneration: 7n,
        }),
        job,
        action,
      },
      {
        client: client as never,
        getQueue: () => queue as never,
        lockAdmission,
        lockLegacyAdmission: vi.fn(),
      },
    );

    expect(lockAdmission).toHaveBeenCalledWith(
      expect.objectContaining({
        transaction,
        runtimeLeaseId: "worker-runtime-1",
        expectedBackend: "clickhouse",
        expectedDeploymentGeneration: 7n,
        action: "foundation",
      }),
    );
    expect(queue.add).toHaveBeenCalledWith("batch-action-processing-job", job, {
      jobId: action.id,
    });

    lockAdmission.mockRejectedValueOnce(
      new Error("Analytics backend deployment generation changed"),
    );
    await expect(
      publishRecoveredBatchAction(
        {
          backend: "clickhouse",
          getAdmissionContext: () => ({
            runtimeLeaseId: "worker-runtime-1",
            backend: "clickhouse",
            deploymentGeneration: 7n,
          }),
          job,
          action,
        },
        {
          client: client as never,
          getQueue: () => queue as never,
          lockAdmission,
          lockLegacyAdmission: vi.fn(),
        },
      ),
    ).rejects.toThrow("deployment generation changed");
    expect(queue.add).toHaveBeenCalledTimes(1);
  });

  it("requires the evaluations capability before recovering a Doris batch evaluation", async () => {
    const action = queuedAction({
      actionType: "observation-run-batched-evaluation",
      config: { evaluatorIds: ["evaluator-1"] },
    });
    const job = buildRecoveredBatchActionJob(action);
    const queue = {
      add: vi.fn().mockResolvedValue({
        getState: vi.fn().mockResolvedValue("waiting"),
      }),
    };
    const transaction = {
      batchAction: {
        findUnique: vi.fn().mockResolvedValue({
          status: BatchActionStatus.Queued,
          actionType: action.actionType,
        }),
      },
    };
    const client = {
      $transaction: vi.fn(async (operation) => operation(transaction)),
    };
    const lockAdmission = vi.fn().mockResolvedValue(undefined);

    await publishRecoveredBatchAction(
      {
        backend: "doris",
        getAdmissionContext: () => ({
          runtimeLeaseId: "worker-runtime-1",
          backend: "doris",
          deploymentGeneration: 7n,
        }),
        job,
        action,
      },
      {
        client: client as never,
        getQueue: () => queue as never,
        lockAdmission,
        lockLegacyAdmission: vi.fn(),
      },
    );

    expect(lockAdmission).toHaveBeenCalledWith({
      transaction,
      runtimeLeaseId: "worker-runtime-1",
      expectedBackend: "doris",
      expectedDeploymentGeneration: 7n,
      capability: "evaluations",
      action: "externalProducer",
    });
    expect(queue.add).toHaveBeenCalledWith("batch-action-processing-job", job, {
      jobId: action.id,
    });
  });

  it.each(["failed", "completed"] as const)(
    "atomically republishes a retained %s job",
    async (terminalState) => {
      const action = queuedAction();
      const job = buildRecoveredBatchActionJob(action);
      const retainedJob = {
        getState: vi.fn().mockResolvedValue(terminalState),
        retry: vi.fn().mockResolvedValue(undefined),
        remove: vi.fn().mockResolvedValue(undefined),
      };
      const queue = {
        add: vi.fn().mockResolvedValue(retainedJob),
      };
      const updateMany = vi.fn().mockResolvedValue({ count: 1 });
      const transaction = {
        batchAction: {
          findUnique: vi.fn().mockResolvedValue({
            status: BatchActionStatus.Queued,
            actionType: action.actionType,
          }),
          updateMany,
        },
      };
      const client = {
        $transaction: vi.fn(async (operation) => operation(transaction)),
      };

      await expect(
        publishRecoveredBatchAction(
          {
            backend: "clickhouse",
            getAdmissionContext: () => null,
            job,
            action,
          },
          {
            client: client as never,
            getQueue: () => queue as never,
            lockAdmission: vi.fn(),
            lockLegacyAdmission: vi.fn().mockResolvedValue(undefined),
          },
        ),
      ).resolves.toBe("published");

      expect(retainedJob.retry).toHaveBeenCalledWith(terminalState);
      expect(updateMany).not.toHaveBeenCalled();
      expect(retainedJob.remove).not.toHaveBeenCalled();
    },
  );

  it("never terminalizes or removes a delivery when a DLQ retry wins after the terminal-state read", async () => {
    const action = queuedAction();
    const job = buildRecoveredBatchActionJob(action);
    let queueState = "failed";
    const retainedJob = {
      getState: vi.fn(async () => {
        const observedState = queueState;
        // 模拟 DlqRetryService 在 getState 的 Redis 命令返回后、publisher
        // 执行下一条命令前完成原子 retry。
        queueState = "waiting";
        return observedState;
      }),
      retry: vi.fn(async (expectedState: string) => {
        if (queueState !== expectedState) {
          throw new Error(`Job is not in the ${expectedState} state`);
        }
        queueState = "waiting";
      }),
      remove: vi.fn(async () => {
        queueState = "removed";
      }),
    };
    const queue = {
      add: vi.fn().mockResolvedValue(retainedJob),
    };
    const updateMany = vi.fn().mockResolvedValue({ count: 1 });
    const transaction = {
      batchAction: {
        findUnique: vi.fn().mockResolvedValue({
          status: BatchActionStatus.Queued,
          actionType: action.actionType,
        }),
        updateMany,
      },
    };
    const client = {
      $transaction: vi.fn(async (operation) => operation(transaction)),
    };

    await expect(
      publishRecoveredBatchAction(
        {
          backend: "clickhouse",
          getAdmissionContext: () => null,
          job,
          action,
        },
        {
          client: client as never,
          getQueue: () => queue as never,
          lockAdmission: vi.fn(),
          lockLegacyAdmission: vi.fn().mockResolvedValue(undefined),
        },
      ),
    ).rejects.toThrow("not in the failed state");

    expect(queueState).toBe("waiting");
    expect(updateMany).not.toHaveBeenCalled();
    expect(retainedJob.remove).not.toHaveBeenCalled();
  });

  it("uses the legacy adoption fence and rejects a mismatched runtime topology", async () => {
    const action = queuedAction();
    const job = buildRecoveredBatchActionJob(action);
    const queue = {
      add: vi.fn().mockResolvedValue({
        getState: vi.fn().mockResolvedValue("waiting"),
      }),
    };
    const transaction = {
      batchAction: {
        findUnique: vi.fn().mockResolvedValue({
          status: BatchActionStatus.Queued,
          actionType: action.actionType,
        }),
      },
    };
    const client = {
      $transaction: vi.fn(async (operation) => operation(transaction)),
    };
    const lockLegacyAdmission = vi.fn().mockResolvedValue(undefined);
    const dependencies = {
      client: client as never,
      getQueue: () => queue as never,
      lockAdmission: vi.fn(),
      lockLegacyAdmission,
    };

    await publishRecoveredBatchAction(
      {
        backend: "clickhouse",
        getAdmissionContext: () => null,
        job,
        action,
      },
      dependencies,
    );
    expect(lockLegacyAdmission).toHaveBeenCalledWith(transaction);
    expect(queue.add).toHaveBeenCalledOnce();

    await expect(
      publishRecoveredBatchAction(
        {
          backend: "clickhouse",
          getAdmissionContext: () => ({
            runtimeLeaseId: "doris-runtime",
            backend: "doris",
            deploymentGeneration: 8n,
          }),
          job,
          action,
        },
        dependencies,
      ),
    ).rejects.toThrow("runtime backend changed");
    expect(client.$transaction).toHaveBeenCalledOnce();
    expect(queue.add).toHaveBeenCalledOnce();
  });

  it("scans a bounded cursor page and advances fairly across queued rows", async () => {
    const first = queuedAction({ id: "batch-action-1" });
    const second = {
      ...queuedAction({ id: "batch-action-2" }),
      createdAt: new Date("2026-07-22T08:00:01.000Z"),
    };
    const findQueuedActions = vi
      .fn()
      .mockResolvedValueOnce([first])
      .mockResolvedValueOnce([second]);
    const publish = vi.fn().mockResolvedValue("published");
    const runner = new BatchActionPublicationRecoveryRunner({
      backend: "doris",
      intervalMs: 1_000,
      batchSize: 1,
      lockTtlSeconds: 60,
      getAdmissionContext: () => null,
      dependencies: {
        findQueuedActions,
        publish,
        runExclusive: async (operation) => operation(),
      },
    });

    await expect(runner.processBatch()).resolves.toBe(0);
    await expect(runner.processBatch()).resolves.toBe(0);

    expect(findQueuedActions).toHaveBeenNthCalledWith(1, {
      after: null,
      limit: 1,
    });
    expect(findQueuedActions).toHaveBeenNthCalledWith(2, {
      after: { id: first.id, createdAt: first.createdAt },
      limit: 1,
    });
    expect(publish.mock.calls.map(([, action]) => action.id)).toEqual([
      first.id,
      second.id,
    ]);
  });

  it("stopAndDrain waits for the current recovery scan and prevents another", async () => {
    let releaseScan!: () => void;
    let markStarted!: () => void;
    const started = new Promise<void>((resolve) => {
      markStarted = resolve;
    });
    const blocked = new Promise<void>((resolve) => {
      releaseScan = resolve;
    });
    const findQueuedActions = vi.fn(async () => {
      markStarted();
      await blocked;
      return [];
    });
    const runner = new BatchActionPublicationRecoveryRunner({
      backend: "clickhouse",
      intervalMs: 1_000,
      batchSize: 10,
      lockTtlSeconds: 60,
      getAdmissionContext: () => null,
      dependencies: {
        findQueuedActions,
        runExclusive: async (operation) => operation(),
      },
    });

    runner.start();
    await started;
    let drained = false;
    const drain = runner.stopAndDrain().then(() => {
      drained = true;
    });
    await Promise.resolve();
    expect(drained).toBe(false);

    releaseScan();
    await drain;
    expect(findQueuedActions).toHaveBeenCalledOnce();
  });
});
