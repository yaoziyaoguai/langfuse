import type Decimal from "decimal.js";

import type { DatasetRunItemDomain } from "../../domain/dataset-run-items";
import type { ScoreAggregate } from "../../features/scores";
import type { OrderByState } from "../../interfaces/orderBy";
import type { FilterState } from "../../types";
import { InvalidRequestError } from "../../errors";

type DatasetRunItemsTableQuery = {
  projectId: string;
  filter: FilterState;
  datasetId?: string;
  orderBy?: OrderByState | OrderByState[];
  limit?: number;
  offset?: number;
};

type DatasetRunItemsByDatasetIdQuery = Omit<
  DatasetRunItemsTableQuery,
  "datasetId"
> & { datasetId: string };

type DatasetRunsMetricsTableQuery = {
  projectId: string;
  datasetId: string;
  filter: FilterState;
  runIds?: string[];
  orderBy?: OrderByState;
  limit?: number;
  offset?: number;
};

type BaseRunDataQuery = {
  projectId: string;
  datasetId: string;
  runIds: string[];
  filterByRun: { runId: string; filters: FilterState }[];
};

export type DatasetRunsMetrics = {
  id: string;
  name: string;
  projectId: string;
  datasetId: string;
  countRunItems: number;
  avgTotalCost: Decimal;
  totalCost: Decimal;
  avgLatency: number;
  aggScoresAvg: Array<[string, number]>;
  aggScoreCategories: string[];
  aggScoreBooleans: string[];
};

type DatasetRunsRows = {
  id: string;
  name: string;
  projectId: string;
  createdAt: Date;
  datasetId: string;
  description: string;
  metadata: string;
};

export type EnrichedDatasetRunItem = {
  id: string;
  createdAt: Date;
  datasetItemId: string;
  datasetItemVersion: Date | undefined;
  datasetRunId: string;
  datasetRunName: string;
  observation:
    | { id: string; latency: number; calculatedTotalCost: Decimal }
    | undefined;
  trace: { id: string; duration: number; totalCost: number };
  scores: ScoreAggregate;
};

function unavailable(): never {
  throw new InvalidRequestError(
    "Dataset run analytics are unavailable in Doris R1A",
  );
}

export const getDatasetRunsTableMetrics = async (
  _opts: DatasetRunsMetricsTableQuery,
): Promise<DatasetRunsMetrics[]> => unavailable();

export const getDatasetRunsTableRowsCh = async (
  _opts: DatasetRunsMetricsTableQuery,
): Promise<DatasetRunsRows[]> => unavailable();

export const getDatasetRunsTableCountCh = async (
  _opts: DatasetRunsMetricsTableQuery,
): Promise<number> => unavailable();

export const getDatasetRunItemsCh = async (
  _opts: DatasetRunItemsTableQuery,
): Promise<DatasetRunItemDomain[]> => unavailable();

export const getDatasetRunItemsByDatasetId = async (
  _opts: DatasetRunItemsByDatasetIdQuery,
): Promise<DatasetRunItemDomain[]> => unavailable();

export const getDatasetItemsWithRunDataCount = async (
  _opts: BaseRunDataQuery,
): Promise<number> => unavailable();

export const getDatasetItemIdsWithRunData = async (
  _opts: BaseRunDataQuery & { limit?: number; offset?: number },
): Promise<string[]> => unavailable();

export const getDatasetRunItemsWithoutIOByItemIds = async (_opts: {
  projectId: string;
  datasetId: string;
  runIds: string[];
  datasetItemIds: string[];
}): Promise<DatasetRunItemDomain<false>[]> => unavailable();

export const getDatasetItemIdsByTraceIdCh = async (_opts: {
  projectId: string;
  traceId: string;
  filter: FilterState;
}): Promise<
  { id: string; datasetId: string; observationId: string | null }[]
> => unavailable();

export const getDatasetRunItemsCountCh = async (
  _opts: DatasetRunItemsTableQuery,
): Promise<number> => unavailable();

export const getDatasetRunItemsCountByDatasetIdCh = async (
  _opts: DatasetRunItemsByDatasetIdQuery,
): Promise<number> => unavailable();

export const hasAnyDatasetRunItem = async (_projectId: string) => false;
export const deleteDatasetRunItemsByProjectId = async (_projectId: string) =>
  true;
export const deleteDatasetRunItemsByDatasetId = async (_input: {
  projectId: string;
  datasetId: string;
}) => undefined;
export const deleteDatasetRunItemsByDatasetRunIds = async (_input: {
  projectId: string;
  datasetRunIds: string[];
  datasetId: string;
}) => undefined;
export const getDatasetRunItemCountsByProjectInCreationInterval =
  async (_input: {
    start: Date;
    end: Date;
  }): Promise<{ projectId: string; count: number }[]> => [];
