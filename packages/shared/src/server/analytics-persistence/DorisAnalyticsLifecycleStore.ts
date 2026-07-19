import { createHash } from "node:crypto";

import type { DorisQueryExecutor } from "../doris/client";
import type {
  DorisStreamLoadReconciliation,
  DorisStreamLoadRequest,
  DorisStreamLoadResult,
} from "../doris/streamLoadClient";
import type {
  AnalyticsDeletionProgress,
  AnalyticsLifecycleStore,
  ProjectTombstoneRequest,
  ProjectTombstoneResult,
  TraceTombstoneRequest,
  TraceTombstoneResult,
} from "./AnalyticsLifecycleStore";

type LifecycleStreamLoad = {
  load(request: DorisStreamLoadRequest): Promise<DorisStreamLoadResult>;
  reconcile(input: {
    readonly label: string;
  }): Promise<DorisStreamLoadReconciliation>;
};

type LifecycleDependencies = {
  readonly streamLoad: LifecycleStreamLoad;
  readonly query: DorisQueryExecutor["query"];
  readonly getDeletionProgress: (input: {
    readonly operationId: string;
    readonly projectId: string;
  }) => Promise<AnalyticsDeletionProgress | null>;
};

function label(
  scope: "trace" | "project",
  request: {
    readonly operationId: string;
    readonly projectId: string;
    readonly generation: bigint;
  },
): string {
  const digest = createHash("sha256")
    .update(
      `${scope}\0${request.operationId}\0${request.projectId}\0${request.generation}`,
      "utf8",
    )
    .digest("hex")
    .slice(0, 40);
  return `lf_${scope}_delete_${digest}`;
}

function dorisDateTime(value: Date): string {
  if (Number.isNaN(value.getTime()))
    throw new TypeError("Invalid deletion time");
  return `${value.toISOString().slice(0, 23).replace("T", " ")}000`;
}

async function loadIsVisible(
  streamLoad: LifecycleStreamLoad,
  request: DorisStreamLoadRequest,
): Promise<boolean> {
  const result = await streamLoad.load(request);
  if (result.committed && !result.requiresReconciliation) return true;
  if (!result.requiresReconciliation) return false;
  return (await streamLoad.reconcile({ label: request.label })).visible;
}

/** Worker-only lifecycle adapter. Web runtimes never receive the load identity. */
export class DorisAnalyticsLifecycleStore implements AnalyticsLifecycleStore {
  constructor(private readonly dependencies: LifecycleDependencies) {}

  async publishTraceTombstone(
    request: TraceTombstoneRequest,
  ): Promise<TraceTombstoneResult> {
    const loadLabel = label("trace", request);
    const loaded = await loadIsVisible(this.dependencies.streamLoad, {
      table: "trace_tombstones",
      label: loadLabel,
      columns: ["project_id", "trace_id", "deletion_generation", "created_at"],
      ndjsonBody: `${JSON.stringify({
        project_id: request.projectId,
        trace_id: request.traceId,
        deletion_generation: request.generation.toString(),
        created_at: dorisDateTime(request.createdAt),
      })}\n`,
    });
    const visible = loaded && (await this.traceBarrierVisible(request));
    return {
      projectId: request.projectId,
      traceId: request.traceId,
      generation: request.generation,
      visible,
      barrierLabel: loadLabel,
    };
  }

  async publishProjectTombstone(
    request: ProjectTombstoneRequest,
  ): Promise<ProjectTombstoneResult> {
    const loadLabel = label("project", request);
    const loaded = await loadIsVisible(this.dependencies.streamLoad, {
      table: "project_tombstones",
      label: loadLabel,
      columns: ["project_id", "deletion_generation", "created_at"],
      ndjsonBody: `${JSON.stringify({
        project_id: request.projectId,
        deletion_generation: request.generation.toString(),
        created_at: dorisDateTime(request.createdAt),
      })}\n`,
    });
    const visible = loaded && (await this.projectBarrierVisible(request));
    return {
      projectId: request.projectId,
      generation: request.generation,
      visible,
      barrierLabel: loadLabel,
    };
  }

  getDeletionProgress(input: {
    readonly operationId: string;
    readonly projectId: string;
  }): Promise<AnalyticsDeletionProgress | null> {
    return this.dependencies.getDeletionProgress(input);
  }

  private async traceBarrierVisible(
    request: TraceTombstoneRequest,
  ): Promise<boolean> {
    const rows = await this.dependencies.query<{ deletion_generation: string }>(
      `SELECT deletion_generation
       FROM trace_tombstones
       WHERE project_id = ?
         AND trace_id = ?
         AND deletion_generation >= ?
       LIMIT 1`,
      [request.projectId, request.traceId, request.generation.toString()],
    );
    return rows.length === 1;
  }

  private async projectBarrierVisible(
    request: ProjectTombstoneRequest,
  ): Promise<boolean> {
    const rows = await this.dependencies.query<{ deletion_generation: string }>(
      `SELECT deletion_generation
       FROM project_tombstones
       WHERE project_id = ?
         AND deletion_generation >= ?
       LIMIT 1`,
      [request.projectId, request.generation.toString()],
    );
    return rows.length === 1;
  }
}
