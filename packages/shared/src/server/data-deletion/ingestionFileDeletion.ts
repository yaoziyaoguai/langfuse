import { InvalidRequestError } from "../../errors";

type BlobCleanupPipelineOptions = {
  s3ChunkSize?: number;
  s3Concurrency?: number;
  tombstoneFlushSize?: number;
};

const unavailable = (): never => {
  throw new InvalidRequestError(
    "Per-project ingestion object retention is unavailable in Doris R1A; raw and canonical prefixes use the deployment lifecycle policy",
  );
};

/** @deprecated Manual score cleanup is handled by the Doris materialized deletion worker. */
export const deleteIngestionEventsFromS3AndDorisForScores = async (
  _input: {
    projectId: string;
    scoreIds: string[];
  } & BlobCleanupPipelineOptions,
): Promise<void> => unavailable();

/** @deprecated Manual trace cleanup is handled by the Doris deletion lifecycle. */
export const removeIngestionEventsFromS3AndDeleteDorisRefsForTraces = async (
  _input: {
    projectId: string;
    traceIds: string[];
    includeEventsTable?: boolean;
  } & BlobCleanupPipelineOptions,
): Promise<void> => unavailable();

/** Compatibility stub: global/per-project retention is not part of Doris R1A. */
export const removeIngestionEventsFromS3AndDeleteDorisRefsForProject = async (
  _projectId: string,
  _cutOffDate: Date | undefined,
  _options?: BlobCleanupPipelineOptions,
): Promise<void> => unavailable();

// Enterprise compatibility only. R1A does not register the retention worker.
export const removeIngestionEventsFromS3AndDeleteClickhouseRefsForProject =
  removeIngestionEventsFromS3AndDeleteDorisRefsForProject;
