import { InvalidRequestError } from "@langfuse/shared";

import type {
  GetExperimentItemsV1QueryType,
  GetExperimentsV1QueryType,
} from "@/src/features/public-api/types/experiments";

function unavailable(): never {
  throw new InvalidRequestError("Experiments are unavailable in Doris R1A");
}

export async function listExperimentsForPublicApi(_input: {
  projectId: string;
  query: GetExperimentsV1QueryType;
}) {
  return unavailable();
}

export async function listExperimentItemsForPublicApi(_input: {
  projectId: string;
  query: GetExperimentItemsV1QueryType;
}) {
  return unavailable();
}
