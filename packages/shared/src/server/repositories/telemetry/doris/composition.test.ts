import { describe, expect, it, vi } from "vitest";

import { createDorisTelemetryRepositories } from "./composition";
import { DorisDatasetRunItemsRepository } from "./datasetRunItems";
import { DorisExperimentsRepository } from "./experiments";
import { DorisObservationsRepository } from "./observations";
import { DorisSessionsRepository } from "./sessions";
import { DorisTracesRepository } from "./traces";
import { DorisUsersRepository } from "./users";

describe("Doris telemetry repository composition", () => {
  it("constructs every R1A entity repository over one query executor", () => {
    const query = vi.fn();

    const repositories = createDorisTelemetryRepositories({ query });

    expect(repositories.datasetRunItems).toBeInstanceOf(
      DorisDatasetRunItemsRepository,
    );
    expect(repositories.experiments).toBeInstanceOf(DorisExperimentsRepository);
    expect(repositories.observations).toBeInstanceOf(
      DorisObservationsRepository,
    );
    expect(repositories.traces).toBeInstanceOf(DorisTracesRepository);
    expect(repositories.sessions).toBeInstanceOf(DorisSessionsRepository);
    expect(repositories.users).toBeInstanceOf(DorisUsersRepository);
  });
});
