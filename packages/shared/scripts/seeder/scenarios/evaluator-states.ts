import { createHash } from "node:crypto";

import { JobExecutionStatus, Prisma } from "@prisma/client";

import { prisma } from "../../../src/db";
import {
  DEFAULT_SEED_PROJECT_ID,
  DEFAULT_SEED_PUBLIC_KEY,
  DEFAULT_SEED_SECRET_KEY,
} from "../defaults";
import { analyticsSmokeScenario } from "./analytics-smoke";
import {
  ANALYTICS_SMOKE_FIXTURE_DATE_FLAG,
  SeedError,
  type ScenarioContext,
  type ScenarioDefinition,
  type SeedSummary,
} from "./types";

const WAIT_TIMEOUT_MS = 90_000;

const stableHexId = (input: string, length: number): string =>
  createHash("sha256").update(input).digest("hex").slice(0, length);

const asRecord = (value: unknown): Record<string, unknown> | null =>
  value && typeof value === "object" && !Array.isArray(value)
    ? (value as Record<string, unknown>)
    : null;

const fixtureStart = (value: unknown): Date => {
  if (typeof value !== "string" || !/^\d{4}-\d{2}-\d{2}$/.test(value)) {
    throw new SeedError(
      `--fixture-date expects YYYY-MM-DD, got "${String(value)}"`,
      "pass a valid UTC calendar date, e.g. --fixture-date 2026-07-17",
    );
  }
  const timestamp = new Date(`${value}T12:00:00.000Z`);
  if (
    !Number.isFinite(timestamp.getTime()) ||
    timestamp.toISOString() !== `${value}T12:00:00.000Z`
  ) {
    throw new SeedError(
      `--fixture-date expects YYYY-MM-DD, got "${value}"`,
      "pass a valid UTC calendar date, e.g. --fixture-date 2026-07-17",
    );
  }
  return timestamp;
};

const authorization = (): string => {
  const values = process.env as Record<string, string | undefined>;
  return `Basic ${Buffer.from(
    `${DEFAULT_SEED_PUBLIC_KEY}:${values["SEED_SECRET_KEY"] ?? DEFAULT_SEED_SECRET_KEY}`,
  ).toString("base64")}`;
};

const requestJson = async (
  url: string,
  input: {
    readonly method?: "GET" | "POST";
    readonly authorization: string;
    readonly body?: unknown;
  },
): Promise<unknown> => {
  let response: Response;
  try {
    response = await fetch(url, {
      method: input.method ?? "GET",
      cache: "no-store",
      redirect: "error",
      signal: AbortSignal.timeout(30_000),
      headers: {
        Accept: "application/json",
        Authorization: input.authorization,
        "Cache-Control": "no-cache",
        ...(input.body === undefined
          ? {}
          : { "Content-Type": "application/json" }),
      },
      ...(input.body === undefined ? {} : { body: JSON.stringify(input.body) }),
    });
  } catch {
    throw new SeedError(
      "public evaluator-state API request failed",
      "confirm NEXTAUTH_URL points to the running web app and retry",
    );
  }
  const body = await response.json().catch(() => null);
  if (!response.ok) {
    throw new SeedError(
      `public evaluator-state API returned HTTP ${response.status}`,
      "confirm the selected analytics backend is ready and the seed API key owns the default project",
    );
  }
  return body;
};

const sleep = (ms: number): Promise<void> =>
  new Promise((resolve) => setTimeout(resolve, ms));

const waitForScores = async (input: {
  readonly baseUrl: string;
  readonly authorization: string;
  readonly traceId: string;
  readonly scoreIds: readonly string[];
}): Promise<void> => {
  const deadline = Date.now() + WAIT_TIMEOUT_MS;
  let attempt = 0;
  while (Date.now() < deadline) {
    attempt += 1;
    const body = await requestJson(
      `${input.baseUrl}/api/public/v2/scores?traceId=${encodeURIComponent(input.traceId)}&fields=score&page=1&limit=100&readbackAttempt=${attempt}`,
      { authorization: input.authorization },
    );
    const data = asRecord(body)?.["data"];
    const visibleIds = new Set(
      Array.isArray(data)
        ? data
            .map((item) => asRecord(item)?.["id"])
            .filter((id): id is string => typeof id === "string")
        : [],
    );
    if (input.scoreIds.every((id) => visibleIds.has(id))) return;
    await sleep(250);
  }
  throw new SeedError(
    "evaluator-state score readback did not converge",
    "confirm the analytics writer and score read repository are healthy",
  );
};

const run = async (
  ctx: ScenarioContext,
  params: Record<string, string | number | boolean>,
): Promise<SeedSummary> => {
  const startedAt = Date.now();
  if (ctx.projectId !== DEFAULT_SEED_PROJECT_ID) {
    throw new SeedError(
      `evaluator-states only supports the default seed project ${DEFAULT_SEED_PROJECT_ID}`,
      "omit --project or use the default seed project",
    );
  }
  const start = fixtureStart(params["fixture-date"]);
  const suffix = stableHexId(`${ctx.projectId}:${ctx.idPrefix}`, 12);
  const traceId = stableHexId(`${ctx.projectId}:${ctx.idPrefix}:trace`, 32);
  const observationId = stableHexId(
    `${ctx.projectId}:${ctx.idPrefix}:span`,
    16,
  );
  const templateId = `seed-evaluator-template-${suffix}`;
  const historyConfigId = `seed-evaluator-history-${suffix}`;
  const emptyConfigId = `seed-evaluator-empty-${suffix}`;
  const successfulScoreId = `seed-evaluator-success-score-${suffix}`;
  const recoveredScoreId = `seed-evaluator-recovered-score-${suffix}`;
  const scoreIds = [successfulScoreId, recoveredScoreId] as const;
  const jobIds = {
    success: `seed-evaluator-job-success-${suffix}`,
    recovered: `seed-evaluator-job-recovered-${suffix}`,
    error: `seed-evaluator-job-error-${suffix}`,
    retrying: `seed-evaluator-job-retrying-${suffix}`,
    running: `seed-evaluator-job-running-${suffix}`,
    queued: `seed-evaluator-job-queued-${suffix}`,
  } as const;
  const counts = {
    traces: 1,
    observations: 1,
    evaluatorTemplates: 1,
    evaluationRules: 2,
    jobExecutions: 6,
    scores: 2,
  };

  const smoke = await analyticsSmokeScenario.run(ctx, params);
  const summary = (input: {
    readonly verified: Record<string, number>;
    readonly links: string[];
  }): SeedSummary => ({
    scenario: "evaluator-states",
    target: ctx.backend,
    params,
    projectId: ctx.projectId,
    environment: ctx.environment,
    traceIds: [traceId],
    sessionIds: [],
    counts,
    verified: input.verified,
    links: input.links,
    dryRun: ctx.dryRun,
    durationMs: Date.now() - startedAt,
    evidenceContractVersion: 1,
    fixtureHash: stableHexId(
      JSON.stringify({
        traceId,
        observationId,
        templateId,
        historyConfigId,
        emptyConfigId,
        scoreIds,
        jobIds,
      }),
      64,
    ),
    semanticHash: smoke.semanticHash ?? null,
    operationId: smoke.operationId ?? null,
  });
  if (ctx.dryRun) return summary({ verified: {}, links: [] });

  const auth = authorization();
  for (const [index, score] of scoreIds.entries()) {
    const body = await requestJson(`${ctx.baseUrl}/api/public/scores`, {
      method: "POST",
      authorization: auth,
      body: {
        id: score,
        name: "seed-evaluator-state",
        value: index === 0 ? 0.91 : 0.97,
        dataType: "NUMERIC",
        traceId,
        observationId,
        environment: ctx.environment,
        comment:
          index === 0
            ? "Seeded successful execution"
            : "Seeded recovery after a delayed retry",
        metadata: {
          scenario: "evaluator-states",
          state: index === 0 ? "success" : "recovered",
        },
      },
    });
    if (asRecord(body)?.["id"] !== score) {
      throw new SeedError(
        "evaluator-state score creation returned an unexpected id",
        "inspect the public score API response and retry",
      );
    }
  }
  await waitForScores({
    baseUrl: ctx.baseUrl,
    authorization: auth,
    traceId,
    scoreIds,
  });

  const at = (minutes: number): Date =>
    new Date(start.getTime() + minutes * 60_000);
  await prisma.$transaction(async (tx) => {
    await tx.evalTemplate.upsert({
      where: {
        projectId_name_version: {
          projectId: ctx.projectId,
          name: `Seed evaluator states ${suffix}`,
          version: 1,
        },
      },
      create: {
        id: templateId,
        projectId: ctx.projectId,
        name: `Seed evaluator states ${suffix}`,
        version: 1,
        type: "LLM_AS_JUDGE",
        prompt:
          "Score the observation input {{input}} and output {{output}} for the local evaluator-state fixture.",
        provider: null,
        model: null,
        modelParams: Prisma.DbNull,
        vars: ["input", "output"],
        outputDefinition: {
          score: "A numeric quality score between zero and one.",
          reasoning: "A concise explanation of the score.",
        },
        sourceCode: null,
        sourceCodeLanguage: null,
      },
      update: {
        prompt:
          "Score the observation input {{input}} and output {{output}} for the local evaluator-state fixture.",
        vars: ["input", "output"],
        outputDefinition: {
          score: "A numeric quality score between zero and one.",
          reasoning: "A concise explanation of the score.",
        },
      },
      select: { id: true },
    });

    const configData = (input: {
      readonly id: string;
      readonly scoreName: string;
    }) => ({
      id: input.id,
      projectId: ctx.projectId,
      jobType: "EVAL" as const,
      status: "INACTIVE" as const,
      evalTemplateId: templateId,
      scoreName: input.scoreName,
      filter: [],
      targetObject: "event",
      variableMapping: [
        {
          templateVariable: "input",
          langfuseObject: "generation",
          selectedColumnId: "input",
        },
        {
          templateVariable: "output",
          langfuseObject: "generation",
          selectedColumnId: "output",
        },
      ],
      sampling: new Prisma.Decimal(1),
      delay: 0,
      timeScope: ["NEW"],
      blockedAt: null,
      blockReason: null,
      blockMessage: null,
    });
    for (const config of [
      configData({
        id: historyConfigId,
        scoreName: "seed-evaluator-state",
      }),
      configData({
        id: emptyConfigId,
        scoreName: "seed-evaluator-empty",
      }),
    ]) {
      await tx.jobConfiguration.upsert({
        where: { id: config.id },
        create: config,
        update: config,
        select: { id: true },
      });
    }

    const executionData = [
      {
        id: jobIds.success,
        status: JobExecutionStatus.COMPLETED,
        createdAt: at(1),
        startTime: at(1),
        endTime: at(1.5),
        error: null,
        jobOutputScoreId: successfulScoreId,
      },
      {
        id: jobIds.recovered,
        status: JobExecutionStatus.COMPLETED,
        createdAt: at(2),
        startTime: at(2),
        endTime: at(2.5),
        error: null,
        jobOutputScoreId: recoveredScoreId,
      },
      {
        id: jobIds.error,
        status: JobExecutionStatus.ERROR,
        createdAt: at(3),
        startTime: at(3),
        endTime: at(3.25),
        error: "The evaluation model request failed.",
        jobOutputScoreId: null,
      },
      {
        id: jobIds.retrying,
        status: JobExecutionStatus.DELAYED,
        createdAt: at(4),
        startTime: at(4),
        endTime: null,
        error: null,
        jobOutputScoreId: null,
      },
      {
        id: jobIds.running,
        status: JobExecutionStatus.PENDING,
        createdAt: at(5),
        startTime: at(5),
        endTime: null,
        error: null,
        jobOutputScoreId: null,
      },
      {
        id: jobIds.queued,
        status: JobExecutionStatus.PENDING,
        createdAt: at(6),
        startTime: null,
        endTime: null,
        error: null,
        jobOutputScoreId: null,
      },
    ] as const;
    for (const execution of executionData) {
      const data = {
        id: execution.id,
        createdAt: execution.createdAt,
        projectId: ctx.projectId,
        jobConfigurationId: historyConfigId,
        jobTemplateId: templateId,
        status: execution.status,
        startTime: execution.startTime,
        endTime: execution.endTime,
        error: execution.error,
        jobInputTraceId: traceId,
        jobInputTraceTimestamp: start,
        jobInputObservationId: observationId,
        jobOutputScoreId: execution.jobOutputScoreId,
        executionTraceId: traceId,
      };
      await tx.jobExecution.upsert({
        where: { id: execution.id },
        create: data,
        update: data,
      });
    }
  });

  const [templateCount, configCount, executionCount] = await Promise.all([
    prisma.evalTemplate.count({
      where: { id: templateId, projectId: ctx.projectId },
    }),
    prisma.jobConfiguration.count({
      where: {
        id: { in: [historyConfigId, emptyConfigId] },
        projectId: ctx.projectId,
      },
    }),
    prisma.jobExecution.count({
      where: {
        id: { in: Object.values(jobIds) },
        projectId: ctx.projectId,
      },
    }),
  ]);
  if (
    templateCount !== 1 ||
    configCount !== counts.evaluationRules ||
    executionCount !== counts.jobExecutions
  ) {
    throw new SeedError(
      "evaluator-state Postgres readback did not match the fixture",
      "confirm the local Postgres schema is current and retry",
    );
  }

  return summary({
    verified: {
      traces: smoke.verified["traces"] ?? 0,
      observations: smoke.verified["observations"] ?? 0,
      evaluatorTemplates: templateCount,
      evaluationRules: configCount,
      jobExecutions: executionCount,
      scores: scoreIds.length,
    },
    links: [
      `${ctx.baseUrl}/project/${ctx.projectId}/evals`,
      `${ctx.baseUrl}/project/${ctx.projectId}/evals/${encodeURIComponent(historyConfigId)}`,
      `${ctx.baseUrl}/project/${ctx.projectId}/evals/${encodeURIComponent(emptyConfigId)}`,
      ...smoke.links,
    ],
  });
};

export const evaluatorStatesScenario: ScenarioDefinition = {
  name: "evaluator-states",
  description:
    "Backend-neutral evaluator list/detail fixture with empty, queued, running, success, terminal error, retrying, and recovered states. Analytics writes use public APIs; only evaluator control-plane rows are seeded in Postgres.",
  flags: [ANALYTICS_SMOKE_FIXTURE_DATE_FLAG],
  supportsV4: false,
  run,
};
