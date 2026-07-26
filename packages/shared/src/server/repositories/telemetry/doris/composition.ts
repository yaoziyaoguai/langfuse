import type { DorisQueryExecutor } from "../../../doris/client";
import { DorisDatasetRunItemsRepository } from "./datasetRunItems";
import { DorisExperimentsRepository } from "./experiments";
import { DorisObservationsRepository } from "./observations";
import { DorisScoresRepository } from "./scores";
import { DorisSessionsRepository } from "./sessions";
import { DorisTracesRepository } from "./traces";
import { DorisUsersRepository } from "./users";

export type DorisTelemetryRepositories = ReturnType<
  typeof createDorisTelemetryRepositories
>;

export function createDorisTelemetryRepositories(executor: DorisQueryExecutor) {
  const query = executor.query.bind(executor);
  const streamQuery = executor.streamQuery
    ? executor.streamQuery.bind(executor)
    : async function* <T extends object>(
        sql: string,
        params?: readonly unknown[],
      ): AsyncIterable<T> {
        for (const row of await executor.query<T>(sql, params)) yield row;
      };
  return {
    datasetRunItems: new DorisDatasetRunItemsRepository({
      query,
      streamQuery,
    }),
    experiments: new DorisExperimentsRepository({ query, streamQuery }),
    observations: new DorisObservationsRepository({ query, streamQuery }),
    scores: new DorisScoresRepository({ query, streamQuery }),
    traces: new DorisTracesRepository({ query, streamQuery }),
    sessions: new DorisSessionsRepository({ query, streamQuery }),
    users: new DorisUsersRepository({ query }),
  };
}
