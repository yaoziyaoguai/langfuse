import { env } from "../../env";
import { QueueName } from "../queues";

export const COMMUNITY_ANALYTICS_SHARDED_QUEUE_FAMILIES = [
  QueueName.IngestionQueue,
  QueueName.IngestionSecondaryQueue,
  QueueName.OtelIngestionQueue,
  QueueName.OtelIngestionSecondaryQueue,
  QueueName.TraceUpsert,
  QueueName.EvaluationExecution,
  QueueName.EvaluationExecutionSecondaryQueue,
  QueueName.LLMAsJudgeExecution,
  QueueName.CodeEvalExecution,
] as const satisfies readonly QueueName[];

export const COMMUNITY_ANALYTICS_NON_SHARDED_QUEUE_FAMILIES = [
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
] as const satisfies readonly QueueName[];

export const COMMUNITY_ANALYTICS_QUEUE_FAMILIES = [
  ...COMMUNITY_ANALYTICS_SHARDED_QUEUE_FAMILIES,
  ...COMMUNITY_ANALYTICS_NON_SHARDED_QUEUE_FAMILIES,
] as const;

export type CommunityAnalyticsShardedQueueFamily =
  (typeof COMMUNITY_ANALYTICS_SHARDED_QUEUE_FAMILIES)[number];

export type CommunityAnalyticsQueueFamily =
  (typeof COMMUNITY_ANALYTICS_QUEUE_FAMILIES)[number];

export type AnalyticsQueueShardCounts = Readonly<
  Record<CommunityAnalyticsShardedQueueFamily, number>
>;

export type AnalyticsQueueShardCountConfig = {
  readonly LANGFUSE_INGESTION_QUEUE_SHARD_COUNT?: unknown;
  readonly LANGFUSE_INGESTION_SECONDARY_QUEUE_SHARD_COUNT?: unknown;
  readonly LANGFUSE_OTEL_INGESTION_QUEUE_SHARD_COUNT?: unknown;
  readonly LANGFUSE_OTEL_INGESTION_SECONDARY_QUEUE_SHARD_COUNT?: unknown;
  readonly LANGFUSE_TRACE_UPSERT_QUEUE_SHARD_COUNT?: unknown;
  readonly LANGFUSE_EVAL_EXECUTION_QUEUE_SHARD_COUNT?: unknown;
  readonly LANGFUSE_EVAL_EXECUTION_SECONDARY_QUEUE_SHARD_COUNT?: unknown;
  readonly LANGFUSE_LLM_AS_JUDGE_EXECUTION_QUEUE_SHARD_COUNT?: unknown;
  readonly LANGFUSE_CODE_EVAL_EXECUTION_QUEUE_SHARD_COUNT?: unknown;
};

export type CommunityAnalyticsQueueInventoryEntry = {
  readonly family: CommunityAnalyticsQueueFamily;
  readonly name: string;
  readonly shardIndex: number | null;
};

function configuredShardCount(value: unknown): number {
  return value === undefined || value === null || value === ""
    ? 1
    : Number(value);
}

export function communityAnalyticsQueueShardCountsFromConfig(
  config: AnalyticsQueueShardCountConfig,
): AnalyticsQueueShardCounts {
  return {
    [QueueName.IngestionQueue]: configuredShardCount(
      config.LANGFUSE_INGESTION_QUEUE_SHARD_COUNT,
    ),
    [QueueName.IngestionSecondaryQueue]: configuredShardCount(
      config.LANGFUSE_INGESTION_SECONDARY_QUEUE_SHARD_COUNT,
    ),
    [QueueName.OtelIngestionQueue]: configuredShardCount(
      config.LANGFUSE_OTEL_INGESTION_QUEUE_SHARD_COUNT,
    ),
    [QueueName.OtelIngestionSecondaryQueue]: configuredShardCount(
      config.LANGFUSE_OTEL_INGESTION_SECONDARY_QUEUE_SHARD_COUNT,
    ),
    [QueueName.TraceUpsert]: configuredShardCount(
      config.LANGFUSE_TRACE_UPSERT_QUEUE_SHARD_COUNT,
    ),
    [QueueName.EvaluationExecution]: configuredShardCount(
      config.LANGFUSE_EVAL_EXECUTION_QUEUE_SHARD_COUNT,
    ),
    [QueueName.EvaluationExecutionSecondaryQueue]: configuredShardCount(
      config.LANGFUSE_EVAL_EXECUTION_SECONDARY_QUEUE_SHARD_COUNT,
    ),
    [QueueName.LLMAsJudgeExecution]: configuredShardCount(
      config.LANGFUSE_LLM_AS_JUDGE_EXECUTION_QUEUE_SHARD_COUNT,
    ),
    [QueueName.CodeEvalExecution]: configuredShardCount(
      config.LANGFUSE_CODE_EVAL_EXECUTION_QUEUE_SHARD_COUNT,
    ),
  };
}

export function configuredCommunityAnalyticsQueueShardCounts(): AnalyticsQueueShardCounts {
  return communityAnalyticsQueueShardCountsFromConfig(env);
}

export function expandCommunityAnalyticsQueueInventory(
  shardCounts: AnalyticsQueueShardCounts,
): readonly CommunityAnalyticsQueueInventoryEntry[] {
  const inventory: CommunityAnalyticsQueueInventoryEntry[] = [];

  for (const family of COMMUNITY_ANALYTICS_SHARDED_QUEUE_FAMILIES) {
    const shardCount = shardCounts[family];
    if (!Number.isSafeInteger(shardCount) || shardCount < 1) {
      throw new TypeError(`Invalid ${family} shard count`);
    }
    for (let shardIndex = 0; shardIndex < shardCount; shardIndex += 1) {
      inventory.push({
        family,
        name: `${family}${shardIndex > 0 ? `-${shardIndex}` : ""}`,
        shardIndex,
      });
    }
  }

  for (const family of COMMUNITY_ANALYTICS_NON_SHARDED_QUEUE_FAMILIES) {
    inventory.push({ family, name: family, shardIndex: null });
  }

  return inventory;
}

export function configuredCommunityAnalyticsQueueInventory(): readonly CommunityAnalyticsQueueInventoryEntry[] {
  return expandCommunityAnalyticsQueueInventory(
    configuredCommunityAnalyticsQueueShardCounts(),
  );
}
