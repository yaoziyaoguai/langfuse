import type { AnalyticsEntityHead } from "@prisma/client";
import {
  getDorisQueryExecutor,
  getS3EventStorageClient,
  toFileReferenceIdentity,
} from "@langfuse/shared/src/server";

import { env } from "../../env";
import { getDorisAnalyticsLifecycleRuntime } from "../../services/dorisAnalyticsLifecycle";

const FILE_LOOKUP_BATCH_SIZE = 250;

type DorisFileReferenceRow = {
  readonly bucket_path: string | null;
};

type DorisProjectRetentionDeletionDependencies = {
  readonly query?: <T extends object>(
    sql: string,
    params?: readonly unknown[],
  ) => Promise<T[]>;
  readonly bucketName?: string;
  readonly deleteFiles?: (paths: string[]) => Promise<void>;
  readonly deleteHeads?: (
    operationId: string,
    heads: readonly AnalyticsEntityHead[],
  ) => Promise<void>;
};

export async function deleteDorisProjectRetentionHeads(
  operationId: string,
  heads: readonly AnalyticsEntityHead[],
  context: {
    readonly cutoffDate: Date;
    readonly projectId?: string;
  },
  dependencies: DorisProjectRetentionDeletionDependencies = {},
): Promise<void> {
  if (!operationId) {
    throw new TypeError("Invalid Doris project retention operation");
  }
  if (Number.isNaN(context.cutoffDate.getTime())) {
    throw new TypeError("Invalid Doris project retention cutoff");
  }
  if (!context.projectId) {
    throw new TypeError("Invalid Doris project retention scope");
  }
  for (const head of heads) {
    if (head.projectId !== context.projectId) {
      throw new Error("Doris project retention scope mismatch");
    }
    if (head.partitionDate >= context.cutoffDate) {
      throw new Error("Doris project retention received a current head");
    }
  }
  const projectId = context.projectId;
  const bucketName =
    dependencies.bucketName ?? env.LANGFUSE_S3_EVENT_UPLOAD_BUCKET;
  const fileHeads = heads.filter(
    (head) => head.entityType === "FILE_REFERENCE",
  );
  if (fileHeads.length > 0) {
    const executor = dependencies.query ? null : getDorisQueryExecutor();
    const query = dependencies.query ?? executor!.query.bind(executor);
    const bucketPaths = new Set<string>();
    for (
      let offset = 0;
      offset < fileHeads.length;
      offset += FILE_LOOKUP_BATCH_SIZE
    ) {
      const selected = fileHeads.slice(offset, offset + FILE_LOOKUP_BATCH_SIZE);
      const conditions: string[] = [];
      const params: unknown[] = [projectId, bucketName];
      for (const head of selected) {
        const identity = toFileReferenceIdentity(head.entityKey);
        if (identity.projectId !== projectId) {
          throw new Error("Doris file-reference head project mismatch");
        }
        conditions.push(
          "(file_date = ? AND entity_type = ? AND entity_id = ? AND file_id = ?)",
        );
        params.push(
          head.partitionDate.toISOString().slice(0, 10),
          identity.entityType,
          identity.entityId,
          identity.fileId,
        );
      }
      const rows = await query<DorisFileReferenceRow>(
        `
          SELECT bucket_path
          FROM blob_storage_file_log
          WHERE project_id = ?
            AND bucket_name = ?
            AND (${conditions.join(" OR ")})
        `,
        params,
      );
      for (const row of rows) {
        if (row.bucket_path) bucketPaths.add(row.bucket_path);
      }
    }
    if (bucketPaths.size > 0) {
      const retainedBucketPaths = new Set<string>();
      const paths = [...bucketPaths];
      for (
        let offset = 0;
        offset < paths.length;
        offset += FILE_LOOKUP_BATCH_SIZE
      ) {
        const selected = paths.slice(offset, offset + FILE_LOOKUP_BATCH_SIZE);
        const rows = await query<DorisFileReferenceRow>(
          `
            SELECT DISTINCT bucket_path
            FROM blob_storage_file_log
            WHERE project_id = ?
              AND bucket_name = ?
              AND bucket_path IN (${selected.map(() => "?").join(", ")})
              AND file_date >= ?
          `,
          [
            projectId,
            bucketName,
            ...selected,
            context.cutoffDate.toISOString().slice(0, 10),
          ],
        );
        for (const row of rows) {
          if (row.bucket_path) retainedBucketPaths.add(row.bucket_path);
        }
      }
      const expiredBucketPaths = paths.filter(
        (path) => !retainedBucketPaths.has(path),
      );
      const deleteFiles =
        dependencies.deleteFiles ??
        ((paths: string[]) =>
          getS3EventStorageClient(bucketName).deleteFiles(paths));
      if (expiredBucketPaths.length > 0) {
        await deleteFiles(expiredBucketPaths);
      }
    }
  }

  await (
    dependencies.deleteHeads ??
    ((id, selected) =>
      getDorisAnalyticsLifecycleRuntime().materializedDeletion.deleteHeads(
        id,
        selected,
      ))
  )(operationId, heads);
}
