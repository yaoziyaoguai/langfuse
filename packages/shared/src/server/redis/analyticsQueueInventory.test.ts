import { describe, expect, it } from "vitest";

import { QueueName } from "../queues";
import {
  COMMUNITY_ANALYTICS_NON_SHARDED_QUEUE_FAMILIES,
  COMMUNITY_ANALYTICS_QUEUE_FAMILIES,
  COMMUNITY_ANALYTICS_SHARDED_QUEUE_FAMILIES,
  expandCommunityAnalyticsQueueInventory,
  type AnalyticsQueueShardCounts,
} from "./analyticsQueueInventory";

const oneShardEach = Object.fromEntries(
  COMMUNITY_ANALYTICS_SHARDED_QUEUE_FAMILIES.map((name) => [name, 1]),
) as AnalyticsQueueShardCounts;

describe("community analytics queue inventory", () => {
  it("keeps the fixed Community queue family contract complete", () => {
    expect(COMMUNITY_ANALYTICS_SHARDED_QUEUE_FAMILIES).toEqual([
      QueueName.IngestionQueue,
      QueueName.IngestionSecondaryQueue,
      QueueName.OtelIngestionQueue,
      QueueName.OtelIngestionSecondaryQueue,
      QueueName.TraceUpsert,
      QueueName.EvaluationExecution,
      QueueName.EvaluationExecutionSecondaryQueue,
      QueueName.LLMAsJudgeExecution,
      QueueName.CodeEvalExecution,
    ]);
    expect(COMMUNITY_ANALYTICS_NON_SHARDED_QUEUE_FAMILIES).toEqual([
      QueueName.TraceDelete,
      QueueName.ProjectDelete,
      QueueName.DatasetRunItemUpsert,
      QueueName.BatchExport,
      QueueName.AnalyticsIngestionQueue,
      QueueName.AnalyticsEvaluationDispatch,
      QueueName.ExperimentCreate,
      QueueName.PostHogIntegrationQueue,
      QueueName.PostHogIntegrationProcessingQueue,
      QueueName.MixpanelIntegrationQueue,
      QueueName.MixpanelIntegrationProcessingQueue,
      QueueName.BlobStorageIntegrationQueue,
      QueueName.BlobStorageIntegrationProcessingQueue,
      QueueName.BatchActionQueue,
      QueueName.CreateEvalQueue,
      QueueName.ScoreDelete,
      QueueName.DatasetDelete,
      QueueName.MonitorQueue,
      QueueName.EventPropagationQueue,
      QueueName.DeadLetterRetryQueue,
    ]);
    expect(COMMUNITY_ANALYTICS_QUEUE_FAMILIES).toHaveLength(29);
    expect(new Set(COMMUNITY_ANALYTICS_QUEUE_FAMILIES)).toHaveProperty(
      "size",
      29,
    );
  });

  it("expands every configured shard to its physical BullMQ name", () => {
    const inventory = expandCommunityAnalyticsQueueInventory({
      ...oneShardEach,
      [QueueName.IngestionQueue]: 3,
      [QueueName.EvaluationExecutionSecondaryQueue]: 2,
    });

    expect(
      inventory.filter((entry) => entry.family === QueueName.IngestionQueue),
    ).toEqual([
      {
        family: QueueName.IngestionQueue,
        name: QueueName.IngestionQueue,
        shardIndex: 0,
      },
      {
        family: QueueName.IngestionQueue,
        name: `${QueueName.IngestionQueue}-1`,
        shardIndex: 1,
      },
      {
        family: QueueName.IngestionQueue,
        name: `${QueueName.IngestionQueue}-2`,
        shardIndex: 2,
      },
    ]);
    expect(
      inventory.filter(
        (entry) => entry.family === QueueName.EvaluationExecutionSecondaryQueue,
      ),
    ).toHaveLength(2);
    expect(
      inventory.find((entry) => entry.family === QueueName.ScoreDelete),
    ).toEqual({
      family: QueueName.ScoreDelete,
      name: QueueName.ScoreDelete,
      shardIndex: null,
    });
    expect(inventory).toHaveLength(32);
    expect(new Set(inventory.map((entry) => entry.name))).toHaveProperty(
      "size",
      32,
    );
  });

  it.each([0, -1, 1.5, Number.NaN, Number.POSITIVE_INFINITY])(
    "fails closed for invalid shard count %s",
    (shardCount) => {
      expect(() =>
        expandCommunityAnalyticsQueueInventory({
          ...oneShardEach,
          [QueueName.TraceUpsert]: shardCount,
        }),
      ).toThrow(/shard count/i);
    },
  );
});
