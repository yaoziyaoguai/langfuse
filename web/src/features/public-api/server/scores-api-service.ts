import { randomUUID } from "crypto";

import {
  convertScoreToPublicApi,
  type ScoreQueryType,
} from "@/src/features/public-api/server/scores";
import { auditLog } from "@/src/features/audit-logs/auditLog";
import { env } from "@/src/env.mjs";
import {
  ForbiddenError,
  InternalServerError,
  LISTABLE_SCORE_TYPES,
  type ScoreSourceType,
  type PostScoresBodyV1,
} from "@langfuse/shared";
import {
  eventTypes,
  QueueJobs,
  ScoreDeleteQueue,
  type AuthHeaderValidVerificationResultIngestion,
  type IngestionAttribution,
  getDorisTelemetryRepositories,
  readDorisScoresForPublicApi,
  acceptAnalyticsIngestion,
  CURRENT_ANALYTICS_CANONICALIZER_VERSION,
  CURRENT_ANALYTICS_SCHEMA_VERSION,
  getS3EventStorageClient,
} from "@langfuse/shared/src/server";
import type { z } from "zod";

type ScoreIngestionResult = {
  successes: Array<{ id: string; status: number }>;
  errors: Array<{ status: number; error?: string; message?: string }>;
};

export class ScoresApiService {
  private readonly dorisReads = new WeakMap<
    object,
    ReturnType<typeof readDorisScoresForPublicApi>
  >();

  constructor(private readonly apiVersion: "v1" | "v2") {}

  private readDorisScores(props: ScoreQueryType) {
    const existing = this.dorisReads.get(props);
    if (existing) return existing;
    const read = readDorisScoresForPublicApi(props, this.apiVersion);
    this.dorisReads.set(props, read);
    return read;
  }

  async createScore({
    body,
    auth,
    auditScope,
    scoreId = body.id ?? randomUUID(),
    attribution,
  }: {
    body: z.infer<typeof PostScoresBodyV1>;
    auth: AuthHeaderValidVerificationResultIngestion;
    auditScope?: { projectId: string; orgId: string; apiKeyId: string };
    scoreId?: string;
    attribution: IngestionAttribution;
  }) {
    if (!auth.scope.projectId) {
      throw new ForbiddenError("Project-scoped API key required");
    }

    const existingScore = auditScope
      ? await this.getScoreById({
          projectId: auditScope.projectId,
          scoreId,
        })
      : undefined;

    const event = {
      id: randomUUID(),
      type: eventTypes.SCORE_CREATE,
      timestamp: new Date().toISOString(),
      body: { ...body, id: scoreId },
    };
    const result = await acceptAnalyticsIngestion({
      projectId: auth.scope.projectId,
      envelope: {
        formatVersion: 1,
        source: "score",
        payload: [event],
        attribution,
      },
      canonicalizerVersion: CURRENT_ANALYTICS_CANONICALIZER_VERSION,
      schemaVersion: CURRENT_ANALYTICS_SCHEMA_VERSION,
      storageService: getS3EventStorageClient(
        env.LANGFUSE_S3_EVENT_UPLOAD_BUCKET,
      ),
      rawPrefix: env.LANGFUSE_S3_EVENT_UPLOAD_PREFIX,
    }).then(
      (): ScoreIngestionResult => ({
        successes: [{ id: event.id, status: 201 }],
        errors: [],
      }),
    );

    if (
      auditScope &&
      result.errors.length === 0 &&
      result.successes.length === 1
    ) {
      await auditLog({
        action: existingScore ? "update" : "create",
        resourceType: "score",
        resourceId: scoreId,
        projectId: auditScope.projectId,
        orgId: auditScope.orgId,
        apiKeyId: auditScope.apiKeyId,
        before: existingScore,
        after: { ...body, id: scoreId },
      });
    }

    return { id: scoreId, result };
  }

  async deleteScore({
    projectId,
    orgId,
    apiKeyId,
    scoreId,
  }: {
    projectId: string;
    orgId: string;
    apiKeyId: string;
    scoreId: string;
  }) {
    const scoreDeleteQueue = ScoreDeleteQueue.getInstance();
    if (!scoreDeleteQueue) {
      throw new InternalServerError("ScoreDeleteQueue not initialized");
    }

    await auditLog({
      action: "delete",
      resourceType: "score",
      resourceId: scoreId,
      projectId,
      orgId,
      apiKeyId,
    });

    await scoreDeleteQueue.add(QueueJobs.ScoreDelete, {
      timestamp: new Date(),
      id: randomUUID(),
      payload: {
        projectId,
        scoreIds: [scoreId],
      },
      name: QueueJobs.ScoreDelete,
    });

    return { message: "Score deletion queued successfully" };
  }

  /**
   * Get a specific score by ID
   * v1: Returns listable scores (NUMERIC, BOOLEAN, CATEGORICAL, TEXT) - excludes CORRECTION
   * v2: Returns all score types including CORRECTION and TEXT
   */
  async getScoreById({
    projectId,
    scoreId,
    source,
  }: {
    projectId: string;
    scoreId: string;
    source?: ScoreSourceType;
  }) {
    const score = await getDorisTelemetryRepositories().scores.get({
      projectId,
      scoreId,
    });
    if (
      !score ||
      (source && score.source !== source) ||
      (this.apiVersion === "v1" &&
        (!LISTABLE_SCORE_TYPES.some(
          (dataType) => dataType === score.dataType,
        ) ||
          !score.traceId ||
          score.sessionId))
    ) {
      return undefined;
    }
    return convertScoreToPublicApi(score);
  }

  /**
   * Get list of scores with version-aware filtering
   * v1: Returns listable scores (NUMERIC, BOOLEAN, CATEGORICAL, TEXT) - excludes CORRECTION
   * v2: Returns all score types including CORRECTION and TEXT
   */
  async generateScoresForPublicApi(props: ScoreQueryType) {
    const { items } = await this.readDorisScores(props);
    // Apply API-shape transformation (moves longStringValue→stringValue for
    // CORRECTION, strips longStringValue for others). Must happen here because
    // convertScoreToPublicApi is a web-layer concern that the shared repository
    // function deliberately does not call.
    return items.map(({ trace, ...score }) => ({
      ...convertScoreToPublicApi(score),
      trace,
    }));
  }

  /**
   * Get count of scores with version-aware filtering
   * v1: Only counts listable scores (NUMERIC, BOOLEAN, CATEGORICAL, TEXT) - excludes CORRECTION
   * v2: Counts all score types including CORRECTION and TEXT
   */
  async getScoresCountForPublicApi(props: ScoreQueryType) {
    return (await this.readDorisScores(props)).count;
  }
}
