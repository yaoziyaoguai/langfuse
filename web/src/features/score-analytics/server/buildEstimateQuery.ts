import { estimateDorisScoreComparison } from "./dorisScoreAnalytics";

export async function buildEstimateQuery(params: {
  projectId: string;
  score1Name: string;
  score1Source: string;
  score1DataType: string;
  score2Name: string;
  score2Source: string;
  score2DataType: string;
  fromTimestamp: Date;
  toTimestamp: Date;
  objectType: "all" | "trace" | "session" | "observation" | "dataset_run";
}) {
  const counts = await estimateDorisScoreComparison({
    projectId: params.projectId,
    score1: {
      name: params.score1Name,
      source: params.score1Source,
      dataType: params.score1DataType,
    },
    score2: {
      name: params.score2Name,
      source: params.score2Source,
      dataType: params.score2DataType,
    },
    fromTimestamp: params.fromTimestamp,
    toTimestamp: params.toTimestamp,
    objectType: params.objectType,
  });
  return {
    score1Count: counts.score1Count,
    score2Count: counts.score2Count,
    estimatedMatchedCount: counts.matchedCount,
  };
}
