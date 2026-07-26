import { createHash } from "node:crypto";
import type { CodeEvalScoreWithName } from "./codeEvalDispatcherTypes";

const EVAL_SCORE_ID_NAMESPACE = Buffer.from(
  "52b93de01d6c4fb39f65e5173184b1cb",
  "hex",
);

function createUuidV5(name: string): string {
  // shared 的 CommonJS 产物会把 uuid 转成 require("uuid")，与 ESM-only
  // uuid 包不兼容；这里直接实现 RFC 9562 UUIDv5，保持既有 ID 完全不变。
  const bytes = createHash("sha1")
    .update(EVAL_SCORE_ID_NAMESPACE)
    .update(name, "utf8")
    .digest()
    .subarray(0, 16);

  bytes[6] = (bytes[6] & 0x0f) | 0x50;
  bytes[8] = (bytes[8] & 0x3f) | 0x80;

  const hex = bytes.toString("hex");
  return [
    hex.slice(0, 8),
    hex.slice(8, 12),
    hex.slice(12, 16),
    hex.slice(16, 20),
    hex.slice(20),
  ].join("-");
}

export function createDeterministicEvalScoreId(params: {
  jobExecutionId: string;
  scoreName: string;
  occurrenceIndex: number;
}): string {
  return createUuidV5(
    JSON.stringify([
      "eval-score",
      params.jobExecutionId,
      params.scoreName,
      params.occurrenceIndex,
    ]),
  );
}

export function buildDeterministicEvalScoreIds(params: {
  scores: CodeEvalScoreWithName[];
  jobExecutionId: string;
}): string[] {
  const occurrenceByScoreName = new Map<string, number>();

  return params.scores.map((score) => {
    const occurrenceIndex = occurrenceByScoreName.get(score.name) ?? 0;
    occurrenceByScoreName.set(score.name, occurrenceIndex + 1);

    return createDeterministicEvalScoreId({
      jobExecutionId: params.jobExecutionId,
      scoreName: score.name,
      occurrenceIndex,
    });
  });
}
