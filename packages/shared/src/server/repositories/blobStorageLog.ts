import { InvalidRequestError } from "../../errors";
import { getDorisQueryExecutor } from "./telemetry/doris/runtime";
import type { BlobStorageFileRefRecordReadType } from "./definitions";

const PAGE_SIZE = 1_000;

const projection = `
  SELECT
    file_id AS id,
    project_id,
    entity_type,
    entity_id,
    event_id,
    bucket_name,
    bucket_path,
    CAST(created_at AS STRING) AS created_at,
    CAST(updated_at AS STRING) AS updated_at,
    CAST(updated_at AS STRING) AS event_ts,
    0 AS is_deleted
  FROM blob_storage_file_log`;

async function* streamRows(
  where: string,
  params: readonly unknown[],
): AsyncGenerator<BlobStorageFileRefRecordReadType> {
  let offset = 0;
  while (true) {
    const rows =
      await getDorisQueryExecutor().query<BlobStorageFileRefRecordReadType>(
        `${projection} WHERE ${where}
       ORDER BY project_id, file_date, entity_type, entity_id, file_id
       LIMIT ? OFFSET ?`,
        [...params, PAGE_SIZE, offset],
      );
    for (const row of rows) yield row;
    if (rows.length < PAGE_SIZE) return;
    offset += rows.length;
  }
}

export const getBlobStorageByProjectAndEntityId = async (
  projectId: string,
  entityType: string,
  entityId: string,
): Promise<BlobStorageFileRefRecordReadType[]> =>
  getDorisQueryExecutor().query<BlobStorageFileRefRecordReadType>(
    `${projection}
     WHERE project_id = ? AND LOWER(entity_type) = LOWER(?) AND entity_id = ?`,
    [projectId, entityType, entityId],
  ) as Promise<BlobStorageFileRefRecordReadType[]>;

export const getBlobStorageByProjectId = (
  projectId: string,
): AsyncGenerator<BlobStorageFileRefRecordReadType> =>
  streamRows("project_id = ?", [projectId]);

export const getBlobStorageByProjectIdBeforeDate = (
  projectId: string,
  beforeDate: Date,
): AsyncGenerator<BlobStorageFileRefRecordReadType> =>
  streamRows("project_id = ? AND created_at <= ?", [projectId, beforeDate]);

export const getBlobStorageByProjectIdAndEntityIds = (
  projectId: string,
  entityType: "observation" | "trace" | "score",
  entityIds: string[],
): AsyncGenerator<BlobStorageFileRefRecordReadType> =>
  entityIds.length === 0
    ? streamRows("1 = 0", [])
    : streamRows(
        "project_id = ? AND LOWER(entity_type) = LOWER(?) AND entity_id IN (?)",
        [projectId, entityType, entityIds],
      );

export const getBlobStorageByProjectIdAndTraceIds = (
  projectId: string,
  traceIds: string[],
  _opts?: { includeEventsTable?: boolean },
): AsyncGenerator<BlobStorageFileRefRecordReadType> =>
  traceIds.length === 0
    ? streamRows("1 = 0", [])
    : streamRows(
        `project_id = ? AND (
          (LOWER(entity_type) = 'trace' AND entity_id IN (?)) OR
          (LOWER(entity_type) IN ('event', 'observation') AND entity_id IN (
            SELECT span_id FROM events_current
            WHERE project_id = ? AND trace_id IN (?)
          )) OR
          (LOWER(entity_type) = 'score' AND entity_id IN (
            SELECT score_id FROM scores_current
            WHERE project_id = ? AND trace_id IN (?)
          ))
        )`,
        [projectId, traceIds, projectId, traceIds, projectId, traceIds],
      );

const legacyMigrationUnavailable = (): never => {
  throw new InvalidRequestError(
    "Legacy event-log migration is unavailable in Doris R1A",
  );
};

export const insertIntoS3RefsTableFromEventLog = async (
  _limit: number,
  _offset: number,
): Promise<void> => legacyMigrationUnavailable();

export const getLastEventLogPrimaryKey = async (): Promise<undefined> =>
  legacyMigrationUnavailable();

export const findS3RefsByPrimaryKey = async (primaryKey: {
  project_id: string;
  entity_type: string;
  entity_id: string;
  bucket_path: string;
}): Promise<BlobStorageFileRefRecordReadType[]> =>
  getDorisQueryExecutor().query<BlobStorageFileRefRecordReadType>(
    `${projection}
     WHERE project_id = ? AND LOWER(entity_type) = LOWER(?)
       AND entity_id = ? AND bucket_path = ?`,
    [
      primaryKey.project_id,
      primaryKey.entity_type,
      primaryKey.entity_id,
      primaryKey.bucket_path,
    ],
  ) as Promise<BlobStorageFileRefRecordReadType[]>;
