import { type ColumnDefinition, type MultiValueOption } from "..";
import { formatColumnOptions } from "./typeHelpers";
import { datasetRunsTableUiColumnDefinitions } from "./mapDatasetRunsTable";

/**
 * Type representing all possible dataset run filter column IDs
 */
type ExtractLiterals<T> = T extends readonly { uiTableName: infer U }[]
  ? U
  : never;

type DatasetRunFilterColumnLiterals = ExtractLiterals<
  typeof datasetRunsTableUiColumnDefinitions
>;

/**
 * Columns that can be filtered using basic PostgreSQL dataset run data
 * (don't require analytics dataset-run aggregates)
 */
const ANALYTICS_FILTER_COLUMNS: DatasetRunFilterColumnLiterals[] = [
  "Scores (categorical)",
  "Scores (numeric)",
  "Scores (boolean)",
];
const ANALYTICS_FILTER_COLUMNS_SET = new Set(ANALYTICS_FILTER_COLUMNS);

/**
 * Returns true if the dataset run filter column requires DRI analytics metrics.
 *
 * This function determines data source requirements by checking if the column
 * needs aggregated dataset-run-item metrics, which are an R1B capability.
 *
 * @param column - The dataset run filter column ID to check
 * @returns true if analytics metrics are required, false if PostgreSQL is sufficient
 */
export function isAnalyticsFilterColumn(column: string): boolean {
  return ANALYTICS_FILTER_COLUMNS_SET.has(column);
}

export const datasetRunsTableCols: ColumnDefinition[] = [
  {
    name: "Scores (numeric)",
    id: "agg_scores_avg",
    type: "numberObject",
    internal: "agg_scores_avg",
    nullable: true,
  },
  {
    name: "Scores (categorical)",
    id: "agg_score_categories",
    type: "categoryOptions",
    internal: "agg_score_categories",
    options: [], // to be filled in at runtime
    nullable: true,
  },
  {
    name: "Scores (boolean)",
    id: "agg_score_booleans",
    type: "booleanObject",
    internal: "agg_score_booleans",
    nullable: true,
  },
];

export type DatasetRunsOptions = {
  agg_scores_avg?: Array<string>;
  agg_score_categories?: Array<MultiValueOption>;
  agg_score_booleans?: Array<string>;
};

export function datasetRunsTableColsWithOptions(
  options?: DatasetRunsOptions,
  cols: ColumnDefinition[] = datasetRunsTableCols,
): ColumnDefinition[] {
  return cols.map((col) => {
    if (col.id === "agg_scores_avg") {
      return formatColumnOptions(col, options?.agg_scores_avg ?? []);
    }
    if (col.id === "agg_score_categories") {
      return formatColumnOptions(col, options?.agg_score_categories ?? []);
    }
    if (col.id === "agg_score_booleans") {
      return formatColumnOptions(col, options?.agg_score_booleans ?? []);
    }
    return col;
  });
}
