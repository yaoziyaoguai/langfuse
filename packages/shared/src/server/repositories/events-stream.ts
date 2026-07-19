import { Readable } from "stream";
import type { TracingSearchType } from "../../interfaces/search";
import type { FilterCondition } from "../../types";
import { InvalidRequestError } from "../../errors";

export const getEventsStreamForEval = async (_props: {
  projectId: string;
  cutoffCreatedAt?: Date;
  filter: FilterCondition[] | null;
  searchQuery?: string;
  searchType?: TracingSearchType[];
  rowLimit: number;
}): Promise<Readable> => {
  throw new InvalidRequestError(
    "Evaluation streaming is unavailable in Doris R1A",
  );
};
