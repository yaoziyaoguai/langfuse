import type { OrderByState } from "../../interfaces/orderBy";
import type { FilterState } from "../../types";
import {
  getSessionsTableCountFromEvents,
  getSessionsTableFromEvents,
  getSessionsWithMetricsFromEvents,
} from "./sessions-ui-table-events-service";

export type SessionDataReturnType = {
  session_id: string;
  max_timestamp: string;
  min_timestamp: string;
  trace_ids: string[];
  user_ids: string[];
  trace_count: number;
  trace_tags: string[];
  trace_environment?: string;
  scores_avg?: Array<Array<[string, number]>>;
  score_categories?: Array<Array<string>>;
  score_booleans?: Array<Array<string>>;
};

export type SessionWithMetricsReturnType = SessionDataReturnType & {
  total_observations: number;
  duration: number;
  session_usage_details: Record<string, number>;
  session_cost_details: Record<string, number>;
  session_input_cost: string;
  session_output_cost: string;
  session_total_cost: string;
  session_input_usage: string;
  session_output_usage: string;
  session_total_usage: string;
};

type SessionTableProps = {
  projectId: string;
  filter: FilterState;
  orderBy?: OrderByState;
  limit?: number;
  page?: number;
};

export const getSessionsTableCount = (props: SessionTableProps) =>
  getSessionsTableCountFromEvents(props);

export const getSessionsTable = async (props: SessionTableProps) => {
  const rows = await getSessionsTableFromEvents(props);
  return rows.map(({ environment, ...row }) => ({
    ...row,
    trace_environment: environment,
  }));
};

export const getSessionsWithMetrics = async (props: SessionTableProps) => {
  const rows = await getSessionsWithMetricsFromEvents(props);
  return rows.map(({ environment, ...row }) => ({
    ...row,
    trace_environment: environment,
  }));
};
