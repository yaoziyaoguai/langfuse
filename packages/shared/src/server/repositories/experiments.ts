import type { OrderByState } from "../../interfaces/orderBy";
import type { FilterState } from "../../types";
import { InvalidRequestError } from "../../errors";

export type ExperimentEventsDataReturnType = {
  experiment_id: string;
  experiment_name: string;
  experiment_description: string | null;
  experiment_dataset_id: string;
  start_time: string;
  item_count: number;
  error_count: number;
  prompts: Array<[string, number | null]>;
  experiment_metadata: Record<string, string>;
};

export type ExperimentMetricsReturnType = {
  experiment_id: string;
  total_cost: number | null;
  latency_avg: number | null;
};

export type FetchExperimentsFromEventsProps = {
  select: "count" | "rows";
  projectId: string;
  filter: FilterState;
  orderBy?: OrderByState;
  limit?: number;
  page?: number;
};

export type ExperimentItemEventsDataReturnType = {
  item_id: string;
  experiment_id: string;
  level: string;
  start_time: string;
  total_cost: number | null;
  latency_ms: number | null;
  observation_id: string;
  trace_id: string;
};

export type ExperimentItemData = {
  experimentId: string;
  level: string;
  startTime: Date;
  totalCost: number | null;
  latencyMs: number | null;
  observationId: string;
  traceId: string;
};

export type GroupedExperimentItem = {
  itemId: string;
  experiments: ExperimentItemData[];
};

export type ExperimentItemMetricsReturnType = {
  experiment_item_id: string;
  trace_id: string;
  total_cost: number | null;
  latency_milliseconds: number | null;
};

type ExperimentItemInput = {
  projectId: string;
  compExperimentIds: string[];
  filterByExperiment: { experimentId: string; filters: FilterState }[];
  baseExperimentId?: string;
  config?: { requireBaselinePresence?: boolean };
};

export type ScoreColumnDefinition = {
  name: string;
  dataType: "NUMERIC" | "BOOLEAN" | "CATEGORICAL";
  source: string;
};

export type ExperimentOutputData = {
  experimentId: string;
  output: string | null;
};

export type ExperimentItemBatchIO = {
  itemId: string;
  input: string | null;
  expectedOutput: string | null;
  outputs: ExperimentOutputData[];
};

function unavailable(): never {
  throw new InvalidRequestError("Experiments are unavailable in Doris R1A");
}

export const getExperimentsCountFromEvents = async (_props: {
  projectId: string;
  filter: FilterState;
  orderBy?: OrderByState;
  limit?: number;
  page?: number;
}): Promise<number> => unavailable();

export const getExperimentsFromEvents = async (_props: {
  projectId: string;
  filter: FilterState;
  orderBy?: OrderByState;
  limit?: number;
  page?: number;
}): Promise<
  {
    id: string;
    name: string;
    description: string | null;
    datasetId: string;
    itemCount: number;
    errorCount: number;
    prompts: Array<[string, number | null]>;
    metadata: Record<string, string>;
    startTime: Date;
  }[]
> => unavailable();

export const getExperimentMetricsFromEvents = async (_props: {
  projectId: string;
  experimentIds: string[];
}): Promise<
  { id: string; totalCost: number | null; latencyAvg: number | null }[]
> => unavailable();

export const getExperimentItemsCountFromEvents = async (
  _props: ExperimentItemInput,
): Promise<number> => unavailable();

type ScoreOptions = {
  obs_scores_avg: string[];
  obs_score_categories: Array<{ label: string; values: string[] }>;
  obs_score_columns: ScoreColumnDefinition[];
  experiment_scores_avg: string[];
  experiment_score_categories: Array<{ label: string; values: string[] }>;
  experiment_score_columns: ScoreColumnDefinition[];
};

export const getExperimentItemsFilterOptions = async (_props: {
  projectId: string;
  experimentIds: string[];
}): Promise<{
  obs_scores_avg: string[];
  obs_score_categories: Array<{ label: string; values: string[] }>;
  obs_score_booleans: string[];
  obs_score_columns: ScoreColumnDefinition[];
  trace_scores_avg: string[];
  trace_score_categories: Array<{ label: string; values: string[] }>;
  trace_score_booleans: string[];
  trace_score_columns: ScoreColumnDefinition[];
}> => unavailable();

export const getExperimentScoreOptions = async (_props: {
  projectId: string;
  experimentIds: string[];
}): Promise<ScoreOptions> => unavailable();

export const getExperimentItemsFromEvents = async (
  _props: ExperimentItemInput & { limit?: number; offset?: number },
): Promise<GroupedExperimentItem[]> => unavailable();

export const getExperimentItemsBatchIO = async (_props: {
  projectId: string;
  itemIds: string[];
  baseExperimentId?: string;
  compExperimentIds: string[];
}): Promise<ExperimentItemBatchIO[]> => unavailable();

export const getExperimentNamesFromEvents = async (_props: {
  projectId: string;
}): Promise<{ experimentName: string; experimentId: string }[]> =>
  unavailable();
