import { createHash } from "node:crypto";

import {
  DEFAULT_SEED_PROJECT_ID,
  DEFAULT_SEED_PUBLIC_KEY,
  DEFAULT_SEED_SECRET_KEY,
} from "../defaults";
import {
  ANALYTICS_SMOKE_FIXTURE_DATE_FLAG,
  SeedError,
  type ScenarioContext,
  type ScenarioDefinition,
  type SeedSummary,
} from "./types";

const WAIT_TIMEOUT_MS = 90_000;
const TERMINAL_OPERATION_STATUSES = new Set([
  "PARTIAL_FAILED",
  "QUARANTINED",
  "UNRECOVERABLE",
  "CANCELLED_BY_DELETION",
  "COMPLETED_WITH_CANCELLATIONS",
]);

const sha256 = (value: unknown): string =>
  createHash("sha256").update(JSON.stringify(value)).digest("hex");

const stableHexId = (input: string, length: number): string =>
  createHash("sha256").update(input).digest("hex").slice(0, length);

const fixtureStartMs = (value: unknown): number => {
  if (typeof value !== "string" || !/^\d{4}-\d{2}-\d{2}$/.test(value)) {
    throw new SeedError(
      `--fixture-date expects YYYY-MM-DD, got "${String(value)}"`,
      "pass a valid UTC calendar date, e.g. --fixture-date 2026-07-17",
    );
  }
  const expectedIso = `${value}T12:00:00.000Z`;
  const parsed = Date.parse(expectedIso);
  if (
    !Number.isFinite(parsed) ||
    new Date(parsed).toISOString() !== expectedIso
  ) {
    throw new SeedError(
      `--fixture-date expects YYYY-MM-DD, got "${value}"`,
      "pass a valid UTC calendar date, e.g. --fixture-date 2026-07-17",
    );
  }
  return parsed;
};

const asRecord = (value: unknown): Record<string, unknown> | null =>
  value && typeof value === "object" && !Array.isArray(value)
    ? (value as Record<string, unknown>)
    : null;

const basicAuth = (publicKey: string, secretKey: string): string =>
  `Basic ${Buffer.from(`${publicKey}:${secretKey}`).toString("base64")}`;

const authorization = (): string => {
  const values = process.env as Record<string, string | undefined>;
  return basicAuth(
    DEFAULT_SEED_PUBLIC_KEY,
    values["SEED_SECRET_KEY"] ?? DEFAULT_SEED_SECRET_KEY,
  );
};

const sleep = (ms: number): Promise<void> =>
  new Promise((resolve) => setTimeout(resolve, ms));

const request = async (
  url: string,
  init: RequestInit,
  timeoutMs = 30_000,
): Promise<Response> => {
  try {
    return await fetch(url, {
      ...init,
      redirect: "error",
      signal: AbortSignal.timeout(timeoutMs),
    });
  } catch (error) {
    throw new SeedError(
      `public API request failed: ${error instanceof Error ? error.message : "unknown network error"}`,
      "confirm NEXTAUTH_URL points to the running web app and retry",
    );
  }
};

const jsonRequest = async (
  url: string,
  input: {
    readonly method?: "GET" | "POST";
    readonly authorization: string;
    readonly body?: unknown;
    readonly allowNotFound?: boolean;
  },
): Promise<{ readonly response: Response; readonly body: unknown }> => {
  const response = await request(url, {
    method: input.method ?? "GET",
    cache: "no-store",
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
  const body = await response.json().catch(() => null);
  if (!response.ok && !(input.allowNotFound && response.status === 404)) {
    throw new SeedError(
      `public foundation API returned HTTP ${response.status}`,
      "confirm dataset-run ingestion is active for Doris and the seed API key owns the default project",
    );
  }
  return { response, body };
};

const waitForDorisOperation = async (
  baseUrl: string,
  operationId: string,
  auth: string,
): Promise<void> => {
  const deadline = Date.now() + WAIT_TIMEOUT_MS;
  while (Date.now() < deadline) {
    const { response, body } = await jsonRequest(
      `${baseUrl}/api/public/ingestion-operations/${encodeURIComponent(operationId)}`,
      { authorization: auth, allowNotFound: true },
    );
    if (response.status === 404) {
      await sleep(250);
      continue;
    }
    const status = asRecord(body)?.["status"];
    if (status === "VISIBLE") return;
    if (typeof status === "string" && TERMINAL_OPERATION_STATUSES.has(status)) {
      throw new SeedError(
        `analytics ingestion operation ended in ${status}`,
        "inspect web/worker logs before replaying the same foundation seed",
      );
    }
    await sleep(250);
  }
  throw new SeedError(
    `timed out waiting for analytics ingestion operation ${operationId}`,
    "start the worker and confirm the analytics ingestion queue is healthy",
  );
};

const otlpFixture = (input: {
  readonly traceId: string;
  readonly spanId: string;
  readonly datasetId: string;
  readonly datasetItemId: string;
  readonly experimentId: string;
  readonly experimentName: string;
  readonly environment: string;
  readonly startMs: number;
}) => ({
  resourceSpans: [
    {
      resource: {
        attributes: [
          { key: "service.name", value: { stringValue: "langfuse-seeder" } },
        ],
      },
      scopeSpans: [
        {
          scope: { name: "langfuse-seeder", version: "1" },
          spans: [
            {
              traceId: input.traceId,
              spanId: input.spanId,
              name: "experiment-foundation",
              kind: 1,
              startTimeUnixNano: String(BigInt(input.startMs) * 1_000_000n),
              endTimeUnixNano: String(BigInt(input.startMs + 250) * 1_000_000n),
              attributes: [
                {
                  key: "langfuse.environment",
                  value: { stringValue: input.environment },
                },
                {
                  key: "langfuse.trace.name",
                  value: { stringValue: "experiment-foundation" },
                },
                {
                  key: "langfuse.experiment.id",
                  value: { stringValue: input.experimentId },
                },
                {
                  key: "langfuse.experiment.name",
                  value: { stringValue: input.experimentName },
                },
                {
                  key: "langfuse.experiment.dataset.id",
                  value: { stringValue: input.datasetId },
                },
                {
                  key: "langfuse.experiment.item.id",
                  value: { stringValue: input.datasetItemId },
                },
              ],
              status: {},
            },
          ],
        },
      ],
    },
  ],
});

const waitForFoundationReadback = async (input: {
  readonly baseUrl: string;
  readonly authorization: string;
  readonly traceId: string;
  readonly spanId: string;
  readonly datasetId: string;
  readonly runName: string;
  readonly runId: string;
  readonly runItemId: string;
  readonly scoreId: string;
}): Promise<void> => {
  const deadline = Date.now() + WAIT_TIMEOUT_MS;
  let attempt = 0;
  while (Date.now() < deadline) {
    attempt += 1;
    const [traceResult, runItemsResult, scoresResult] = await Promise.all([
      jsonRequest(
        `${input.baseUrl}/api/public/traces/${encodeURIComponent(input.traceId)}?fields=observations&readbackAttempt=${attempt}`,
        { authorization: input.authorization, allowNotFound: true },
      ),
      jsonRequest(
        `${input.baseUrl}/api/public/dataset-run-items?datasetId=${encodeURIComponent(input.datasetId)}&runName=${encodeURIComponent(input.runName)}&page=1&limit=10&readbackAttempt=${attempt}`,
        { authorization: input.authorization, allowNotFound: true },
      ),
      jsonRequest(
        `${input.baseUrl}/api/public/v2/scores?datasetRunId=${encodeURIComponent(input.runId)}&fields=score&page=1&limit=10&readbackAttempt=${attempt}`,
        { authorization: input.authorization, allowNotFound: true },
      ),
    ]);
    if (
      traceResult.response.status === 404 ||
      runItemsResult.response.status === 404 ||
      scoresResult.response.status === 404
    ) {
      await sleep(250);
      continue;
    }
    const trace = asRecord(traceResult.body);
    const observations = Array.isArray(trace?.["observations"])
      ? trace["observations"]
      : [];
    const runItems = asRecord(runItemsResult.body)?.["data"];
    const scores = asRecord(scoresResult.body)?.["data"];
    if (
      trace?.["id"] === input.traceId &&
      observations.some((item) => asRecord(item)?.["id"] === input.spanId) &&
      Array.isArray(runItems) &&
      runItems.some(
        (item) =>
          asRecord(item)?.["id"] === input.runItemId &&
          asRecord(item)?.["datasetRunId"] === input.runId,
      ) &&
      Array.isArray(scores) &&
      scores.some((item) => asRecord(item)?.["id"] === input.scoreId)
    ) {
      return;
    }
    await sleep(250);
  }
  throw new SeedError(
    "experiment foundation readback did not converge",
    "confirm dataset-run ingestion, analytics writer, and public read repositories are healthy",
  );
};

const run = async (
  ctx: ScenarioContext,
  params: Record<string, string | number | boolean>,
): Promise<SeedSummary> => {
  const startedAt = Date.now();
  if (ctx.projectId !== DEFAULT_SEED_PROJECT_ID) {
    throw new SeedError(
      `experiment-foundation only supports the default seed project ${DEFAULT_SEED_PROJECT_ID}`,
      "omit --project or use the default seed project; its fixed local API key protects project-scoped readback",
    );
  }
  const startMs = fixtureStartMs(params["fixture-date"]);
  const suffix = stableHexId(`${ctx.projectId}:${ctx.idPrefix}`, 12);
  const datasetName = `experiment-foundation-${suffix}`;
  const runName = `foundation-run-${suffix}`;
  const experimentId = `foundation-experiment-${suffix}`;
  const datasetItemId = `foundation-item-${suffix}`;
  const traceId = stableHexId(`${ctx.projectId}:${ctx.idPrefix}:trace`, 32);
  const spanId = stableHexId(`${ctx.projectId}:${ctx.idPrefix}:span`, 16);
  const scoreId = `foundation-score-${suffix}`;
  const counts = {
    datasets: 1,
    datasetItems: 1,
    traces: 1,
    observations: 1,
    datasetRunItems: 1,
    scores: 1,
  };
  const fixtureHash = sha256({
    datasetName,
    runName,
    experimentId,
    datasetItemId,
    traceId,
    spanId,
    scoreId,
    environment: ctx.environment,
    startTime: new Date(startMs).toISOString(),
  });
  const summary = (input: {
    readonly verified: Record<string, number>;
    readonly semanticHash: string | null;
    readonly operationId: string | null;
    readonly links: string[];
  }): SeedSummary => ({
    scenario: "experiment-foundation",
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
    fixtureHash,
    semanticHash: input.semanticHash,
    operationId: input.operationId,
  });

  if (ctx.dryRun) {
    return summary({
      verified: {},
      semanticHash: null,
      operationId: null,
      links: [],
    });
  }

  const auth = authorization();
  const datasetResult = await jsonRequest(
    `${ctx.baseUrl}/api/public/v2/datasets`,
    {
      method: "POST",
      authorization: auth,
      body: {
        name: datasetName,
        description: "Backend-neutral experiment foundation seed",
        metadata: { scenario: "experiment-foundation", seed: ctx.seed },
      },
    },
  );
  const datasetId = asRecord(datasetResult.body)?.["id"];
  if (typeof datasetId !== "string") {
    throw new SeedError(
      "dataset creation returned no id",
      "inspect the public dataset API response and retry",
    );
  }
  const datasetItemResult = await jsonRequest(
    `${ctx.baseUrl}/api/public/dataset-items`,
    {
      method: "POST",
      authorization: auth,
      body: {
        id: datasetItemId,
        datasetName,
        input: { question: "What is 2 + 2?" },
        expectedOutput: { answer: "4" },
        metadata: { scenario: "experiment-foundation" },
      },
    },
  );
  if (asRecord(datasetItemResult.body)?.["id"] !== datasetItemId) {
    throw new SeedError(
      "dataset item creation returned an unexpected id",
      "inspect the public dataset item API response and retry",
    );
  }

  const traceResponse = await request(
    `${ctx.baseUrl}/api/public/otel/v1/traces`,
    {
      method: "POST",
      headers: {
        Accept: "application/json",
        Authorization: auth,
        "Content-Type": "application/json",
        "x-langfuse-ingestion-version": "4",
        "x-langfuse-sdk-name": "langfuse-seeder",
        "x-langfuse-sdk-version": "1",
      },
      body: JSON.stringify(
        otlpFixture({
          traceId,
          spanId,
          datasetId,
          datasetItemId,
          experimentId,
          experimentName: runName,
          environment: ctx.environment,
          startMs,
        }),
      ),
    },
  );
  if (!traceResponse.ok) {
    throw new SeedError(
      `public OTLP ingestion returned HTTP ${traceResponse.status}`,
      "confirm web readiness and the selected analytics backend",
    );
  }
  const operationId = traceResponse.headers.get(
    "x-langfuse-ingestion-operation-id",
  );
  if (ctx.backend === "doris" && !operationId) {
    throw new SeedError(
      "Doris experiment foundation ingestion returned no operation ID",
      "restart web with LANGFUSE_ANALYTICS_BACKEND=doris",
    );
  }
  if (ctx.backend === "clickhouse" && operationId) {
    throw new SeedError(
      "ClickHouse experiment foundation returned a Doris operation ID",
      "restart web with LANGFUSE_ANALYTICS_BACKEND=clickhouse",
    );
  }
  if (operationId) {
    await waitForDorisOperation(ctx.baseUrl, operationId, auth);
  }

  const existingRunItemsResult = await jsonRequest(
    `${ctx.baseUrl}/api/public/dataset-run-items?datasetId=${encodeURIComponent(datasetId)}&runName=${encodeURIComponent(runName)}&page=1&limit=100`,
    { authorization: auth, allowNotFound: true },
  );
  const existingRunItems = asRecord(existingRunItemsResult.body)?.["data"];
  const existingRunItem = Array.isArray(existingRunItems)
    ? existingRunItems.find((item) => {
        const record = asRecord(item);
        return (
          record?.["datasetItemId"] === datasetItemId &&
          record?.["traceId"] === traceId
        );
      })
    : undefined;
  const runItem =
    asRecord(existingRunItem) ??
    asRecord(
      (
        await jsonRequest(`${ctx.baseUrl}/api/public/dataset-run-items`, {
          method: "POST",
          authorization: auth,
          body: {
            runName,
            runDescription: "Backend-neutral foundation run",
            metadata: { scenario: "experiment-foundation", experimentId },
            datasetItemId,
            traceId,
            createdAt: new Date(startMs).toISOString(),
          },
        })
      ).body,
    );
  const runItemId = runItem?.["id"];
  const runId = runItem?.["datasetRunId"];
  if (typeof runItemId !== "string" || typeof runId !== "string") {
    throw new SeedError(
      "dataset run item creation returned no run identity",
      "inspect the public dataset-run API response and retry",
    );
  }

  const existingScoresResult = await jsonRequest(
    `${ctx.baseUrl}/api/public/v2/scores?datasetRunId=${encodeURIComponent(runId)}&fields=score&page=1&limit=100`,
    { authorization: auth, allowNotFound: true },
  );
  const existingScores = asRecord(existingScoresResult.body)?.["data"];
  const existingScore = Array.isArray(existingScores)
    ? existingScores.find((item) => asRecord(item)?.["id"] === scoreId)
    : undefined;
  if (!existingScore) {
    const scoreResult = await jsonRequest(`${ctx.baseUrl}/api/public/scores`, {
      method: "POST",
      authorization: auth,
      body: {
        id: scoreId,
        name: "foundation-correctness",
        value: 1,
        dataType: "NUMERIC",
        datasetRunId: runId,
        environment: ctx.environment,
        metadata: { scenario: "experiment-foundation" },
      },
    });
    if (asRecord(scoreResult.body)?.["id"] !== scoreId) {
      throw new SeedError(
        "score creation returned an unexpected id",
        "inspect the public score API response and retry",
      );
    }
  }

  await waitForFoundationReadback({
    baseUrl: ctx.baseUrl,
    authorization: auth,
    traceId,
    spanId,
    datasetId,
    runName,
    runId,
    runItemId,
    scoreId,
  });
  return summary({
    verified: counts,
    semanticHash: sha256({
      dataset: true,
      datasetItemId,
      traceId,
      spanId,
      runItem: true,
      run: true,
      scoreId,
    }),
    operationId,
    links: [
      `${ctx.baseUrl}/project/${ctx.projectId}/experiments/results?baseline=${encodeURIComponent(runId)}`,
      `${ctx.baseUrl}/project/${ctx.projectId}/traces/${traceId}?timestamp=${encodeURIComponent(new Date(startMs).toISOString())}`,
    ],
  });
};

export const experimentFoundationScenario: ScenarioDefinition = {
  name: "experiment-foundation",
  description:
    "One public dataset/item, OTLP experiment trace, dataset-run item, and associated score with backend-neutral public readback. Supported by ClickHouse and Doris once dataset-run ingestion is active.",
  supportsV4: false,
  flags: [ANALYTICS_SMOKE_FIXTURE_DATE_FLAG],
  run,
};
