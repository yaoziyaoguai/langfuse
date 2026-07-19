import type { DorisQueryExecutor } from "../../../doris/client";
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
  return {
    observations: new DorisObservationsRepository({ query }),
    scores: new DorisScoresRepository({ query }),
    traces: new DorisTracesRepository({ query }),
    sessions: new DorisSessionsRepository({ query }),
    users: new DorisUsersRepository({ query }),
  };
}
