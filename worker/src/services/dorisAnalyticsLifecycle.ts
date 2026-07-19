import { createHash } from "node:crypto";

import type { AnalyticsEntityHead } from "@prisma/client";
import {
  DorisAnalyticsLifecycleStore,
  DorisStreamLoadClient,
  getDeletionProgressForProject,
  getDorisQueryExecutor,
  parseDorisStreamLoadConfig,
  resolveDorisNodeEnv,
  toEventIdentity,
  toFileReferenceIdentity,
  toScoreIdentity,
  type DorisStreamLoadRequest,
} from "@langfuse/shared/src/server";

import { env } from "../env";

const TERMINAL_VERSION = "9223372036854775807";
const DELETE_BATCH_SIZE = 5_000;

type LifecycleTransport = Pick<DorisStreamLoadClient, "load" | "reconcile">;

function optionalString(value: number | string | undefined) {
  return value === undefined ? undefined : String(value);
}

function lifecycleTransport(): LifecycleTransport {
  const config = parseDorisStreamLoadConfig(
    {
      DORIS_QUERY_USER: env.DORIS_QUERY_USER,
      DORIS_STREAM_LOAD_FE_URL: env.DORIS_STREAM_LOAD_FE_URL,
      DORIS_STREAM_LOAD_USER: env.DORIS_STREAM_LOAD_USER,
      DORIS_STREAM_LOAD_PASSWORD: env.DORIS_STREAM_LOAD_PASSWORD,
      DORIS_STREAM_LOAD_DATABASE: env.DORIS_STREAM_LOAD_DATABASE,
      DORIS_STREAM_LOAD_FE_IP_ALLOWLIST: env.DORIS_STREAM_LOAD_FE_IP_ALLOWLIST,
      DORIS_STREAM_LOAD_BE_ALLOWLIST: env.DORIS_STREAM_LOAD_BE_ALLOWLIST,
      DORIS_STREAM_LOAD_BE_IP_ALLOWLIST: env.DORIS_STREAM_LOAD_BE_IP_ALLOWLIST,
      DORIS_STREAM_LOAD_TLS_CA_PATH: env.DORIS_STREAM_LOAD_TLS_CA_PATH,
      DORIS_STREAM_LOAD_REQUEST_TIMEOUT_MS: optionalString(
        env.DORIS_STREAM_LOAD_REQUEST_TIMEOUT_MS,
      ),
      DORIS_STREAM_LOAD_MAX_BODY_BYTES: optionalString(
        env.DORIS_STREAM_LOAD_MAX_BODY_BYTES,
      ),
    },
    resolveDorisNodeEnv(env.NODE_ENV, env.DORIS_LOCAL_DEV_MODE),
  );
  return new DorisStreamLoadClient(config);
}

type DeleteBatch = Pick<
  DorisStreamLoadRequest,
  "table" | "columns" | "ndjsonBody"
> & { readonly rowCount: number };

function date(value: Date): string {
  return value.toISOString().slice(0, 10);
}

function batches(heads: readonly AnalyticsEntityHead[]): DeleteBatch[] {
  const rows = new Map<
    string,
    { columns: readonly string[]; rows: Record<string, string>[] }
  >([
    [
      "events_current",
      {
        columns: [
          "project_id",
          "partition_date",
          "trace_id",
          "span_id",
          "version_token",
          "type",
          "environment",
          "is_app_root",
          "bookmarked",
          "public",
          "start_time",
          "created_at",
          "updated_at",
          "source",
          "ingestion_sdk_name",
          "ingestion_sdk_version",
        ],
        rows: [],
      },
    ],
    [
      "scores_current",
      {
        columns: [
          "project_id",
          "score_date",
          "score_id",
          "version_token",
          "name",
          "source",
          "data_type",
          "environment",
          "timestamp",
          "created_at",
          "updated_at",
        ],
        rows: [],
      },
    ],
    [
      "blob_storage_file_log",
      {
        columns: [
          "project_id",
          "file_date",
          "entity_type",
          "entity_id",
          "file_id",
          "version_token",
          "created_at",
          "updated_at",
        ],
        rows: [],
      },
    ],
  ]);

  const orderedHeads = [...heads].sort((left, right) => {
    const leftKey = `${left.entityType}\0${left.partitionDate.toISOString()}\0${left.entityKey}`;
    const rightKey = `${right.entityType}\0${right.partitionDate.toISOString()}\0${right.entityKey}`;
    return leftKey < rightKey ? -1 : leftKey > rightKey ? 1 : 0;
  });

  for (const head of orderedHeads) {
    if (head.entityType === "EVENT") {
      const identity = toEventIdentity(head.entityKey);
      if (identity.projectId !== head.projectId) {
        throw new Error("Analytics entity head project mismatch");
      }
      rows.get("events_current")!.rows.push({
        project_id: identity.projectId,
        partition_date: date(head.partitionDate),
        trace_id: identity.traceId,
        span_id: identity.spanId,
        version_token: TERMINAL_VERSION,
        type: "EVENT",
        environment: "default",
        is_app_root: "false",
        bookmarked: "false",
        public: "false",
        start_time: `${date(head.partitionDate)} 00:00:00.000000`,
        created_at: `${date(head.partitionDate)} 00:00:00.000000`,
        updated_at: `${date(head.partitionDate)} 00:00:00.000000`,
        source: "deletion",
        ingestion_sdk_name: "unknown",
        ingestion_sdk_version: "unknown",
      });
    } else if (head.entityType === "SCORE") {
      const identity = toScoreIdentity(head.entityKey);
      if (identity.projectId !== head.projectId) {
        throw new Error("Analytics entity head project mismatch");
      }
      rows.get("scores_current")!.rows.push({
        project_id: identity.projectId,
        score_date: date(head.partitionDate),
        score_id: identity.scoreId,
        version_token: TERMINAL_VERSION,
        name: "__deleted__",
        source: "deletion",
        data_type: "NUMERIC",
        environment: "default",
        timestamp: `${date(head.partitionDate)} 00:00:00.000000`,
        created_at: `${date(head.partitionDate)} 00:00:00.000000`,
        updated_at: `${date(head.partitionDate)} 00:00:00.000000`,
      });
    } else {
      const identity = toFileReferenceIdentity(head.entityKey);
      if (identity.projectId !== head.projectId) {
        throw new Error("Analytics entity head project mismatch");
      }
      rows.get("blob_storage_file_log")!.rows.push({
        project_id: identity.projectId,
        file_date: date(head.partitionDate),
        entity_type: identity.entityType,
        entity_id: identity.entityId,
        file_id: identity.fileId,
        version_token: TERMINAL_VERSION,
        created_at: `${date(head.partitionDate)} 00:00:00.000000`,
        updated_at: `${date(head.partitionDate)} 00:00:00.000000`,
      });
    }
  }

  return [...rows.entries()].flatMap(([table, group]) => {
    const output: DeleteBatch[] = [];
    for (
      let offset = 0;
      offset < group.rows.length;
      offset += DELETE_BATCH_SIZE
    ) {
      const selected = group.rows.slice(offset, offset + DELETE_BATCH_SIZE);
      output.push({
        table,
        columns: group.columns,
        rowCount: selected.length,
        ndjsonBody: `${selected.map((row) => JSON.stringify(row)).join("\n")}\n`,
      });
    }
    return output;
  });
}

function deleteLabel(operationId: string, batch: DeleteBatch): string {
  const digest = createHash("sha256")
    .update(`${operationId}\0${batch.table}\0${batch.ndjsonBody}`, "utf8")
    .digest("hex")
    .slice(0, 40);
  return `lf_materialized_delete_${digest}`;
}

export class DorisMaterializedDeletionWriter {
  constructor(private readonly transport: LifecycleTransport) {}

  async deleteHeads(
    operationId: string,
    heads: readonly AnalyticsEntityHead[],
  ): Promise<void> {
    for (const batch of batches(heads)) {
      const label = deleteLabel(operationId, batch);
      const result = await this.transport.load({
        table: batch.table,
        columns: batch.columns,
        ndjsonBody: batch.ndjsonBody,
        label,
        mergeType: "DELETE",
      });
      const reconciled = result.requiresReconciliation
        ? await this.transport.reconcile({ label })
        : null;
      const visible = reconciled
        ? reconciled.visible
        : result.committed && !result.requiresReconciliation;
      if (
        !visible ||
        (!reconciled &&
          (result.numberFilteredRows > 0 ||
            result.numberTotalRows !== batch.rowCount))
      ) {
        throw new Error(
          `Doris materialized deletion is not visible for ${batch.table} (status=${result.status}, visible=${visible}, total=${result.numberTotalRows}, filtered=${result.numberFilteredRows}, expected=${batch.rowCount})`,
        );
      }
    }
  }
}

export type DorisAnalyticsLifecycleRuntime = {
  readonly store: DorisAnalyticsLifecycleStore;
  readonly materializedDeletion: DorisMaterializedDeletionWriter;
};

let runtime: DorisAnalyticsLifecycleRuntime | undefined;

export function getDorisAnalyticsLifecycleRuntime(): DorisAnalyticsLifecycleRuntime {
  if (runtime) return runtime;
  const streamLoad = lifecycleTransport();
  const executor = getDorisQueryExecutor();
  runtime = {
    store: new DorisAnalyticsLifecycleStore({
      streamLoad,
      query: executor.query.bind(executor),
      getDeletionProgress: getDeletionProgressForProject,
    }),
    materializedDeletion: new DorisMaterializedDeletionWriter(streamLoad),
  };
  return runtime;
}
