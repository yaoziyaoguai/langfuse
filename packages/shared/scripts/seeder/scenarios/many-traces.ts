import { prisma } from "../../../src/db";
import {
  createObservation,
  createTrace,
  createTraceScore,
  type EventRecordInsertType,
  type ScoreRecordInsertType,
} from "../../../src/server";
import {
  seedEventFixtures,
  seedScoreFixtures,
} from "../utils/analytics-writer";
import { observationToEvent } from "./event-mirror";
import { utcDayStartMs } from "./rng";
import {
  ScenarioContext,
  ScenarioDefinition,
  SeedError,
  SeedSummary,
} from "./types";
import { countRows, escapeLike, tracesListLink } from "./verify";

const SESSION_POOL_SIZE = 100;
const TRACE_BATCH_SIZE = 250;

const run = async (
  ctx: ScenarioContext,
  params: Record<string, string | number | boolean>,
): Promise<SeedSummary> => {
  const startedAt = Date.now();
  const count = Number(params["count"]);
  const days = Number(params["days"]);
  const observationsPerTrace = Number(params["observations-per-trace"]);
  const scoresPerTrace = Number(params["scores-per-trace"]);
  const richPayloads = params["rich-payloads"] === true;

  if (!Number.isInteger(count) || count < 1) {
    throw new SeedError(`--count must be a positive integer, got ${count}`);
  }
  if (!Number.isInteger(observationsPerTrace) || observationsPerTrace < 1) {
    throw new SeedError(
      `--observations-per-trace must be >= 1 in the Doris events-only model, got ${observationsPerTrace}`,
    );
  }
  if (!Number.isInteger(scoresPerTrace) || scoresPerTrace < 0) {
    throw new SeedError(
      `--scores-per-trace must be >= 0, got ${scoresPerTrace}`,
    );
  }
  if (!Number.isFinite(days) || days < 0) {
    throw new SeedError(`--days must be >= 0, got ${days}`);
  }

  const counts: Record<string, number> = {
    sessions: SESSION_POOL_SIZE,
    traces: count,
    observations: count * observationsPerTrace,
    scores: count * scoresPerTrace,
  };
  const links = [tracesListLink(ctx)];
  if (ctx.dryRun) {
    return {
      scenario: "many-traces",
      target: "doris",
      params,
      projectId: ctx.projectId,
      environment: ctx.environment,
      traceIds: [],
      sessionIds: Array.from(
        { length: 5 },
        (_, index) => `${ctx.idPrefix}-session_${index}`,
      ),
      counts,
      verified: {},
      links,
      dryRun: true,
      durationMs: Date.now() - startedAt,
    };
  }

  await prisma.traceSession.createMany({
    data: Array.from({ length: SESSION_POOL_SIZE }, (_, index) => ({
      id: `${ctx.idPrefix}-session_${index}`,
      projectId: ctx.projectId,
      environment: ctx.environment,
    })),
    skipDuplicates: true,
  });

  const anchor = utcDayStartMs();
  const spreadMs = days * 24 * 60 * 60 * 1_000;
  const suffix = ctx.projectId.slice(-8);
  ctx.log(
    `writing ${count} traces, ${counts.observations} observations and ${counts.scores} scores through the analytics ingestion pipeline`,
  );

  for (let offset = 0; offset < count; offset += TRACE_BATCH_SIZE) {
    const end = Math.min(count, offset + TRACE_BATCH_SIZE);
    const events: EventRecordInsertType[] = [];
    const scores: ScoreRecordInsertType[] = [];
    for (let index = offset; index < end; index += 1) {
      const traceId = `${ctx.idPrefix}-trace-bulk-${index}-${suffix}`;
      const timestamp =
        anchor - Math.floor((index * spreadMs) / Math.max(count, 1));
      const trace = createTrace({
        id: traceId,
        project_id: ctx.projectId,
        environment: ctx.environment,
        session_id:
          index % 10 < 3
            ? `${ctx.idPrefix}-session_${index % SESSION_POOL_SIZE}`
            : null,
        timestamp,
        name: `bulk-trace-${index % 10}`,
        user_id: index % 3 === 0 ? `${ctx.idPrefix}-user-${index % 100}` : null,
        tags: ["seed", "many-traces"],
        metadata: { scenario: "many-traces", index: String(index) },
        input: richPayloads
          ? JSON.stringify({
              question: `Explain fixture ${index}`,
              nested: { index },
            })
          : JSON.stringify({ index }),
        output: richPayloads
          ? `Generated fixture answer for trace ${index}`
          : "ok",
        public: false,
        bookmarked: index % 20 === 0,
        created_at: timestamp,
        updated_at: timestamp,
        event_ts: timestamp,
      });

      for (
        let observationIndex = 0;
        observationIndex < observationsPerTrace;
        observationIndex += 1
      ) {
        const observation = createObservation({
          id: `${ctx.idPrefix}-obs-bulk-${index}-${observationIndex}-${suffix}`,
          trace_id: traceId,
          project_id: ctx.projectId,
          environment: ctx.environment,
          parent_observation_id:
            observationIndex === 0
              ? null
              : `${ctx.idPrefix}-obs-bulk-${index}-0-${suffix}`,
          type: observationIndex % 3 === 0 ? "GENERATION" : "SPAN",
          name: `bulk-observation-${observationIndex}`,
          start_time: timestamp + observationIndex * 10,
          end_time: timestamp + observationIndex * 10 + 5,
          completion_start_time:
            observationIndex % 3 === 0
              ? timestamp + observationIndex * 10 + 2
              : null,
          input: trace.input,
          output: trace.output,
          metadata: { scenario: "many-traces" },
          created_at: timestamp,
          updated_at: timestamp,
          event_ts: timestamp + observationIndex * 10 + 5,
        });
        events.push(observationToEvent(observation, trace));
      }

      for (let scoreIndex = 0; scoreIndex < scoresPerTrace; scoreIndex += 1) {
        scores.push(
          createTraceScore({
            id: `${ctx.idPrefix}-score-bulk-${index}-${scoreIndex}-${suffix}`,
            project_id: ctx.projectId,
            trace_id: traceId,
            observation_id: null,
            environment: ctx.environment,
            name: `bulk-score-${scoreIndex % 5}`,
            value: ((index + scoreIndex) % 100) / 100,
            data_type: "NUMERIC",
            source: "EVAL",
            comment: null,
            metadata: {},
            timestamp,
            created_at: timestamp,
            updated_at: timestamp,
            event_ts: timestamp,
          }),
        );
      }
    }
    await seedEventFixtures(events);
    await seedScoreFixtures(scores);
  }

  const idSuffix = escapeLike(suffix);
  const verified: Record<string, number> = {
    traces: await countRows(
      "traces",
      `project_id = {projectId: String} AND id LIKE {prefix: String}`,
      {
        projectId: ctx.projectId,
        prefix: `${escapeLike(ctx.idPrefix)}-trace-bulk-%-${idSuffix}`,
      },
      "uniqExact(id)",
    ),
    observations: await countRows(
      "observations",
      `project_id = {projectId: String} AND id LIKE {prefix: String}`,
      {
        projectId: ctx.projectId,
        prefix: `${escapeLike(ctx.idPrefix)}-obs-bulk-%-${idSuffix}`,
      },
      "uniqExact(id)",
    ),
    scores: await countRows(
      "scores",
      `project_id = {projectId: String} AND id LIKE {prefix: String}`,
      {
        projectId: ctx.projectId,
        prefix: `${escapeLike(ctx.idPrefix)}-score-bulk-%-${idSuffix}`,
      },
      "uniqExact(id)",
    ),
  };

  for (const [entity, expected] of Object.entries({
    traces: count,
    observations: counts.observations,
    scores: counts.scores,
  })) {
    if (verified[entity] !== expected) {
      throw new SeedError(
        `Readback mismatch for ${entity}: expected ${expected}, found ${verified[entity]}`,
      );
    }
  }

  return {
    scenario: "many-traces",
    target: "doris",
    params,
    projectId: ctx.projectId,
    environment: ctx.environment,
    traceIds: [],
    sessionIds: Array.from(
      { length: 5 },
      (_, index) => `${ctx.idPrefix}-session_${index}`,
    ),
    counts,
    verified,
    links,
    dryRun: false,
    durationMs: Date.now() - startedAt,
  };
};

export const manyTracesScenario: ScenarioDefinition = {
  name: "many-traces",
  description: "Large deterministic Doris trace corpus for list performance",
  supportsV4: true,
  flags: [
    {
      flag: "count",
      type: "number",
      default: 10_000,
      description: "Trace count",
    },
    { flag: "days", type: "number", default: 7, description: "UTC day spread" },
    {
      flag: "observations-per-trace",
      type: "number",
      default: 3,
      description: "Events per trace (minimum 1)",
    },
    {
      flag: "scores-per-trace",
      type: "number",
      default: 1,
      description: "Trace-level scores per trace",
    },
    {
      flag: "rich-payloads",
      type: "boolean",
      default: false,
      description: "Use larger nested input/output fixtures",
    },
  ],
  run,
};
