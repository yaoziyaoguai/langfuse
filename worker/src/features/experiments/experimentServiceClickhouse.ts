import {
  asRecord,
  convertEventRecordToObservationForEval,
  DatasetItemDomain,
  Prisma,
} from "@langfuse/shared";
import {
  ChatMessage,
  convertDateToClickhouseDateTime,
  createLLMOutput,
  createLLMToolSet,
  createUnknownSdkIngestionAttribution,
  createDatasetItemFilterState,
  DatasetRunItemUpsertQueue,
  eventTypes,
  generateLLMText,
  getDatasetItems,
  getExistingDatasetRunItemDatasetItemIds,
  IngestionEventType,
  isManagedExperimentExecutionJob,
  LangfuseInternalTraceEnvironment,
  logger,
  mapLegacyLLMCompletionParams,
  processEventBatch,
  QueueJobs,
  redis,
  TraceSinkParams,
  type AnalyticsRuntimeAdmissionContext,
  type ExperimentCreateEventType,
} from "@langfuse/shared/src/server";
import { v4, v5 } from "uuid";
import {
  parseDatasetItemInput,
  replaceVariablesInPrompt,
  validateAndSetupExperiment,
  type PromptExperimentConfig,
} from "./utils";
import {
  validateDatasetItem,
  normalizeDatasetItemInput,
} from "@langfuse/shared";
import { randomUUID } from "crypto";
import { createW3CTraceId } from "../utils";
import { scheduleExperimentObservationEvals } from "./scheduleExperimentEvals";
import { createInternalEventsWriter } from "../internal-tracing/createInternalEventsWriter";
import { getWorkerAnalyticsAdmissionContext } from "../../analyticsRuntime";

async function processItem(
  projectId: string,
  datasetItem: DatasetItemDomain & { input: Prisma.JsonObject },
  config: PromptExperimentConfig,
  execution: ExperimentExecutionOptions,
): Promise<{ success: boolean }> {
  // Use unified trace ID to avoid creating duplicate traces between PostgreSQL and ClickHouse
  const newTraceId = createW3CTraceId(`${config.runId}-${datasetItem.id}`);
  const runItemId = execution.managedDoris
    ? v5(`${config.runId}:${datasetItem.id}`, v5.URL)
    : v4();
  const timestamp = new Date().toISOString();

  const event = {
    id: runItemId,
    type: eventTypes.DATASET_RUN_ITEM_CREATE,
    timestamp,
    body: {
      id: runItemId,
      traceId: newTraceId,
      observationId: null,
      error: null,
      createdAt: timestamp,
      datasetId: datasetItem.datasetId,
      runId: config.runId,
      datasetItemId: datasetItem.id,
      datasetVersion: datasetItem.validFrom.toISOString(),
    },
  };

  const auth = {
    validKey: true as const,
    scope: {
      projectId: config.projectId,
      accessLevel: "project" as const,
    },
  };

  const ingestionResult = await processEventBatch([event], auth, {
    isLangfuseInternal: true,
    analyticsAdmissionContext: execution.analyticsAdmissionContext,
    enableDorisDatasetRunIngestion: execution.managedDoris,
    attribution: createUnknownSdkIngestionAttribution({ authCheck: auth }),
  });

  if (ingestionResult.errors.length > 0) {
    const error = ingestionResult.errors[0];
    logger.error(
      `Failed to create run item for dataset item ${datasetItem.id}`,
      error,
    );
    if (execution.managedDoris) {
      throw new Error(
        `Doris dataset-run ingestion rejected item ${datasetItem.id}`,
      );
    }
  }

  /********************
   * LLM MODEL CALL *
   ********************/

  const llmResult = await processLLMCall(
    runItemId,
    newTraceId,
    datasetItem,
    config,
    execution.analyticsAdmissionContext,
  );

  if (!llmResult.success) return { success: false };

  /********************
   * ASYNC RUN ITEM EVAL *
   ********************/

  if (!execution.managedDoris && redis) {
    const queue = DatasetRunItemUpsertQueue.getInstance();
    if (queue) {
      await queue.add(QueueJobs.DatasetRunItemUpsert, {
        payload: {
          projectId,
          datasetItemId: datasetItem.id,
          datasetItemValidFrom: datasetItem.validFrom,
          traceId: newTraceId,
        },
        id: randomUUID(),
        timestamp: new Date(),
        name: QueueJobs.DatasetRunItemUpsert as const,
      });
    }
  }

  return { success: true };
}

async function processLLMCall(
  runItemId: string,
  traceId: string,
  datasetItem: DatasetItemDomain & { input: Prisma.JsonObject },
  config: PromptExperimentConfig,
  analyticsAdmissionContext: AnalyticsRuntimeAdmissionContext | null,
): Promise<{ success: boolean }> {
  let messages: ChatMessage[] = [];
  // Extract and replace variables in prompt
  try {
    messages = replaceVariablesInPrompt(
      config.validatedPrompt,
      datasetItem.input,
      config.allVariables,
      config.placeholderNames,
    );
  } catch (error) {
    logger.error(
      `Failed to replace variables in prompt for dataset item ${datasetItem.id}`,
      error,
    );
    return { success: false };
  }
  const traceSinkParams: TraceSinkParams = {
    environment: LangfuseInternalTraceEnvironment.PromptExperiments,
    traceName: `dataset-run-item-${runItemId.slice(0, 5)}`,
    traceId,
    targetProjectId: config.projectId, // ingest to user project
    metadata: {
      dataset_id: datasetItem.datasetId,
      dataset_item_id: datasetItem.id,
      structured_output_schema: config.structuredOutputSchema,
      experiment_name: config.experimentName,
      experiment_run_name: config.experimentRunName,
    },
    prompt: config.prompt,
    eventsWriter: createInternalEventsWriter({
      analyticsAdmissionContext,
      experimentContext: {
        id: config.runId,
        name: config.datasetRun.name,
        metadata: asRecord(config.datasetRun.metadata),
        description: config.datasetRun.description,
        datasetId: datasetItem.datasetId,
        itemId: datasetItem.id,
        itemVersion: convertDateToClickhouseDateTime(datasetItem.validFrom),
        itemExpectedOutput: datasetItem.expectedOutput,
        itemMetadata: asRecord(datasetItem.metadata),
      },
      onRootEventRecordReady: async (rootEventRecord) => {
        await scheduleExperimentObservationEvals({
          observation: convertEventRecordToObservationForEval(rootEventRecord),
        });
      },
    }),
  };

  const llmParams = mapLegacyLLMCompletionParams({
    connection: config.validatedApiKey,
    messages,
    modelParams: {
      provider: config.provider,
      model: config.model,
      adapter: config.validatedApiKey.adapter,
      ...config.model_params,
    },
  });

  await generateLLMText({
    ...llmParams,
    maxRetries: 1,
    // Setup rejects the unsupported tools + structured-output combination.
    ...(config.structuredOutputSchema
      ? { output: createLLMOutput(config.structuredOutputSchema) }
      : config.tools.length > 0
        ? { tools: createLLMToolSet(config.tools) }
        : {}),
    trace: traceSinkParams,
  }).catch(() => undefined); // catch errors and do not retry

  return { success: true };
}

async function getItemsToProcess(
  projectId: string,
  datasetId: string,
  runId: string,
  config: PromptExperimentConfig,
) {
  // Fetch all dataset items at the specified version (if provided)
  const datasetItems = await getDatasetItems({
    projectId,
    filterState: createDatasetItemFilterState({
      datasetIds: [datasetId],
      status: "ACTIVE",
    }),
    version: config.datasetVersion,
    includeIO: true,
  });

  // Filter and validate dataset items
  const validatedDatasetItems = datasetItems
    .filter(({ input }) => validateDatasetItem(input, config.allVariables))
    .map((datasetItem) => {
      // Normalize string inputs to object format for single-variable prompts
      const normalizedInput = normalizeDatasetItemInput(
        datasetItem.input,
        config.allVariables,
      );

      return {
        ...datasetItem,
        status: datasetItem.status ?? "ACTIVE",
        input: parseDatasetItemInput(normalizedInput, config.allVariables),
      };
    });

  if (!validatedDatasetItems.length) {
    logger.info(
      `No Dataset ${datasetId} item input matches expected prompt variable format`,
    );
    return [];
  }

  // Batch deduplication - get existing run items' dataset item ids
  const existingDatasetItemIds = await getExistingDatasetRunItemDatasetItemIds({
    projectId,
    datasetRunId: runId,
    datasetId,
  });

  // Filter out existing items
  const itemsToProcess = validatedDatasetItems.filter(
    (item) => !existingDatasetItemIds.has(item.id),
  );

  logger.info(
    `Found ${validatedDatasetItems.length} valid items, ${existingDatasetItemIds.size} already exist, ${itemsToProcess.length} to process`,
  );

  return itemsToProcess;
}

type ExperimentExecutionOptions = {
  readonly managedDoris: boolean;
  readonly analyticsAdmissionContext: AnalyticsRuntimeAdmissionContext | null;
  readonly onItemProcessed?: () => Promise<void>;
};

export const createExperimentJob = async ({
  event,
  onItemProcessed,
}: {
  event: ExperimentCreateEventType;
  onItemProcessed?: () => Promise<void>;
}) => {
  const managedDoris = isManagedExperimentExecutionJob(event);
  const analyticsAdmissionContext = managedDoris
    ? getWorkerAnalyticsAdmissionContext()
    : null;
  if (managedDoris && !analyticsAdmissionContext) {
    throw new Error("Doris experiment worker runtime is not admitted");
  }
  const execution: ExperimentExecutionOptions = {
    managedDoris,
    analyticsAdmissionContext,
    onItemProcessed,
  };
  const startTime = Date.now();
  logger.info("Processing experiment create job", {
    ...event,
    analyticsBackend: managedDoris ? "doris" : "clickhouse",
  });

  const { datasetId, projectId, runId } = event;

  /********************
   * INPUT VALIDATION *
   ********************/

  let experimentConfig: PromptExperimentConfig;
  try {
    experimentConfig = await validateAndSetupExperiment(event);
  } catch (error) {
    logger.error("Failed to validate and setup experiment", error);
    const errorMessage =
      error instanceof Error ? error.message : "Unknown error";
    // Create all dataset run items with the configuration error
    await createAllDatasetRunItemsWithConfigError(
      projectId,
      datasetId,
      runId,
      errorMessage,
      execution,
    );
    return { success: true };
  }

  /********************
   * FETCH AND VALIDATE ALL DATASET ITEMS *
   ********************/

  const itemsToProcess = await getItemsToProcess(
    projectId,
    datasetId,
    runId,
    experimentConfig,
  );

  if (itemsToProcess.length === 0) {
    logger.info(`No new items to process for experiment ${runId}`);
    return { success: true };
  }

  /********************
   * PROCESS VALID ITEMS *
   ********************/

  logger.info(`Processing ${itemsToProcess.length} items`);

  for (let i = 0; i < itemsToProcess.length; i++) {
    const item = itemsToProcess[i];
    logger.info(
      `Processing item ${i + 1}/${itemsToProcess.length} (${item.id})`,
    );

    try {
      await processItem(projectId, item, experimentConfig, execution);
    } catch (error) {
      logger.error(`Item ${i + 1} failed completely`, error);
      if (managedDoris) throw error;
    } finally {
      await execution.onItemProcessed?.();
    }
  }

  const duration = Date.now() - startTime;
  logger.info(
    `Experiment ${runId} completed in ${duration}ms. Processed: ${itemsToProcess.length}`,
  );

  return { success: true };
};

export const createExperimentJobClickhouse = createExperimentJob;

// In error cases (config errors), we always create traces in ClickHouse execution path since PostgreSQL execution
// simply updates dataset run metadata and has never created error-level traces. This is new behavior we have introduced.
// We accept this inconsistency in writes until the DRI migration had been completed.
async function createAllDatasetRunItemsWithConfigError(
  projectId: string,
  datasetId: string,
  runId: string,
  errorMessage: string,
  execution: ExperimentExecutionOptions,
) {
  // Fetch all dataset items
  const datasetItems = await getDatasetItems({
    projectId,
    filterState: createDatasetItemFilterState({
      datasetIds: [datasetId],
      status: "ACTIVE",
    }),
    includeIO: true,
  });

  // Check for existing run items' dataset item ids to avoid duplicates
  const existingRunItemDatasetItemIds =
    await getExistingDatasetRunItemDatasetItemIds({
      projectId,
      datasetRunId: runId,
      datasetId,
    });

  // Create run items with config error for all non-existing items
  const newItems = datasetItems.filter(
    (item) => !existingRunItemDatasetItemIds.has(item.id),
  );

  const events: IngestionEventType[] = newItems.flatMap((datasetItem) => {
    const traceId = execution.managedDoris
      ? createW3CTraceId(`${runId}-${datasetItem.id}`)
      : v4();
    const runItemId = execution.managedDoris
      ? v5(`${runId}:${datasetItem.id}`, v5.URL)
      : v4();
    const generationId = execution.managedDoris
      ? v5(`${runId}:${datasetItem.id}:config-error`, v5.URL)
      : v4();
    const timestamp = new Date().toISOString();

    let stringInput = "";
    try {
      stringInput = JSON.stringify(datasetItem.input);
    } catch {
      logger.info(
        `Failed to stringify input for dataset item ${datasetItem.id}`,
      );
    }

    return [
      // dataset run item
      {
        id: runItemId,
        type: eventTypes.DATASET_RUN_ITEM_CREATE,
        timestamp,
        body: {
          id: runItemId,
          traceId,
          observationId: null,
          error: `Experiment configuration error: ${errorMessage}`,
          createdAt: timestamp,
          datasetId: datasetItem.datasetId,
          runId: runId,
          datasetItemId: datasetItem.id,
          datasetVersion: datasetItem.validFrom.toISOString(),
        },
      },
      // trace
      {
        id: traceId,
        type: eventTypes.TRACE_CREATE,
        timestamp,
        body: {
          id: traceId,
          environment: LangfuseInternalTraceEnvironment.PromptExperiments,
          name: `dataset-run-item-${runItemId.slice(0, 5)}`,
          input: stringInput,
        },
      },
      // generation
      {
        id: generationId,
        type: eventTypes.GENERATION_CREATE,
        timestamp,
        body: {
          id: generationId,
          environment: LangfuseInternalTraceEnvironment.PromptExperiments,
          traceId,
          input: stringInput,
          level: "ERROR" as const,
          statusMessage: `Experiment configuration error: ${errorMessage}`,
        },
      },
    ];
  });

  if (events.length > 0) {
    logger.info(
      `Creating ${events.length / 3} dataset run items with config error`,
    );

    const auth = {
      validKey: true as const,
      scope: {
        projectId,
        accessLevel: "project" as const,
      },
    };

    const result = await processEventBatch(events, auth, {
      isLangfuseInternal: true,
      analyticsAdmissionContext: execution.analyticsAdmissionContext,
      enableDorisDatasetRunIngestion: execution.managedDoris,
      attribution: createUnknownSdkIngestionAttribution({ authCheck: auth }),
    });
    if (execution.managedDoris && result.errors.length > 0) {
      throw new Error("Doris rejected experiment configuration error events");
    }
  }
}
