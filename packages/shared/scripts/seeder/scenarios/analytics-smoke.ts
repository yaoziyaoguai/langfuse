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
const EVIDENCE_CONTRACT_VERSION = 1;
const PROJECT_ID_PLACEHOLDER = "$PROJECT_ID";
const TRACE_ID_PLACEHOLDER = "$TRACE_ID";
const SPAN_ID_PLACEHOLDER = "$SPAN_ID";
const TERMINAL_OPERATION_STATUSES = new Set([
  "PARTIAL_FAILED",
  "QUARANTINED",
  "UNRECOVERABLE",
  "CANCELLED_BY_DELETION",
  "COMPLETED_WITH_CANCELLATIONS",
]);

const stableHexId = (input: string, length: number): string =>
  createHash("sha256").update(input).digest("hex").slice(0, length);

const compareStrings = (left: string, right: string): number =>
  left < right ? -1 : left > right ? 1 : 0;

const canonicalize = (value: unknown): unknown => {
  if (Array.isArray(value)) return value.map(canonicalize);
  if (value && typeof value === "object") {
    return Object.fromEntries(
      Object.entries(value as Record<string, unknown>)
        .filter(([, entry]) => entry !== undefined)
        .sort(([left], [right]) => compareStrings(left, right))
        .map(([key, entry]) => [key, canonicalize(entry)]),
    );
  }
  return value;
};

const canonicalJson = (value: unknown): string =>
  JSON.stringify(canonicalize(value));

const sha256 = (value: unknown): string =>
  createHash("sha256").update(canonicalJson(value)).digest("hex");

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

const buildOtlpFixture = (input: {
  traceId: string;
  spanId: string;
  startMs: number;
  environment: string;
}) => ({
  resourceSpans: [
    {
      resource: {
        attributes: [
          {
            key: "service.name",
            value: { stringValue: "langfuse-seeder" },
          },
        ],
      },
      scopeSpans: [
        {
          scope: { name: "langfuse-seeder", version: "1" },
          spans: [
            {
              traceId: input.traceId,
              spanId: input.spanId,
              name: "analytics-backend-smoke",
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
                  value: { stringValue: "analytics-backend-smoke" },
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

const asRecord = (value: unknown): Record<string, unknown> | null =>
  value && typeof value === "object" && !Array.isArray(value)
    ? (value as Record<string, unknown>)
    : null;

const normalizeTimestamp = (value: unknown): unknown => {
  if (value instanceof Date) return value.toISOString();
  if (typeof value !== "string") return value ?? null;
  const timestamp = Date.parse(value);
  return Number.isFinite(timestamp) ? new Date(timestamp).toISOString() : value;
};

const replaceIdentifier = (
  value: unknown,
  expected: string,
  placeholder: string,
): unknown => (value === expected ? placeholder : (value ?? null));

const normalizeObservationMetadata = (
  value: unknown,
): Record<string, unknown> => {
  const metadata = asRecord(value);
  const resourceAttributes = asRecord(metadata?.["resourceAttributes"]);
  const scope = asRecord(metadata?.["scope"]);
  return {
    resourceAttributes: {
      "service.name": resourceAttributes?.["service.name"] ?? null,
    },
    scope: {
      name: scope?.["name"] ?? null,
      version: scope?.["version"] ?? null,
    },
  };
};

const normalizePublicTraceReadback = (input: {
  trace: Record<string, unknown>;
  projectId: string;
  traceId: string;
  spanId: string;
}): Record<string, unknown> => {
  const observations = Array.isArray(input.trace["observations"])
    ? input.trace["observations"]
        .map(asRecord)
        .filter(
          (observation): observation is Record<string, unknown> =>
            observation?.["id"] === input.spanId,
        )
        .map((observation) => ({
          id: replaceIdentifier(
            observation["id"],
            input.spanId,
            SPAN_ID_PLACEHOLDER,
          ),
          projectId: replaceIdentifier(
            observation["projectId"],
            input.projectId,
            PROJECT_ID_PLACEHOLDER,
          ),
          traceId: replaceIdentifier(
            observation["traceId"],
            input.traceId,
            TRACE_ID_PLACEHOLDER,
          ),
          parentObservationId: observation["parentObservationId"] ?? null,
          name: observation["name"] ?? null,
          type: observation["type"] ?? null,
          environment: observation["environment"] ?? null,
          startTime: normalizeTimestamp(observation["startTime"]),
          endTime: normalizeTimestamp(observation["endTime"]),
          input: observation["input"] ?? null,
          output: observation["output"] ?? null,
          metadata: normalizeObservationMetadata(observation["metadata"]),
          level: observation["level"] ?? null,
          statusMessage: observation["statusMessage"] ?? null,
        }))
        .sort((left, right) =>
          compareStrings(canonicalJson(left), canonicalJson(right)),
        )
    : [];

  return {
    id: replaceIdentifier(
      input.trace["id"],
      input.traceId,
      TRACE_ID_PLACEHOLDER,
    ),
    projectId: replaceIdentifier(
      input.trace["projectId"],
      input.projectId,
      PROJECT_ID_PLACEHOLDER,
    ),
    timestamp: normalizeTimestamp(input.trace["timestamp"]),
    name: input.trace["name"] ?? null,
    environment: input.trace["environment"] ?? null,
    observations,
  };
};

const basicAuth = (publicKey: string, secretKey: string): string =>
  `Basic ${Buffer.from(`${publicKey}:${secretKey}`).toString("base64")}`;

const credentials = (): { publicKey: string; secretKey: string } => {
  const values = process.env as Record<string, string | undefined>;
  return {
    publicKey: DEFAULT_SEED_PUBLIC_KEY,
    secretKey: values["SEED_SECRET_KEY"] ?? DEFAULT_SEED_SECRET_KEY,
  };
};

const sleep = (ms: number): Promise<void> =>
  new Promise((resolve) => setTimeout(resolve, ms));

const networkFailureKind = (error: unknown): string => {
  const errorRecord = asRecord(error);
  const cause = asRecord(errorRecord?.["cause"]);
  const code = cause?.["code"] ?? errorRecord?.["code"];
  if (typeof code === "string" && /^[A-Z0-9_]+$/.test(code)) return code;
  return error instanceof Error ? error.name : "UnknownError";
};

const request = async (
  url: string,
  init: RequestInit,
  timeoutMs = 10_000,
): Promise<Response> => {
  let lastError: unknown;
  for (let attempt = 0; attempt < 3; attempt += 1) {
    try {
      return await fetch(url, {
        ...init,
        redirect: "error",
        signal: AbortSignal.timeout(timeoutMs),
      });
    } catch (error) {
      lastError = error;
      if (attempt < 2) await sleep(100 * 2 ** attempt);
    }
  }
  throw new SeedError(
    `public API request failed (${networkFailureKind(lastError)})`,
    "confirm NEXTAUTH_URL points to the running web app and retry",
  );
};

const waitForDorisOperation = async (
  baseUrl: string,
  operationId: string,
  authorization: string,
): Promise<void> => {
  const deadline = Date.now() + WAIT_TIMEOUT_MS;
  const url = `${baseUrl}/api/public/ingestion-operations/${encodeURIComponent(operationId)}`;
  while (Date.now() < deadline) {
    const response = await request(url, {
      method: "GET",
      headers: { Accept: "application/json", Authorization: authorization },
    });
    if (response.status === 404) {
      await sleep(250);
      continue;
    }
    if (!response.ok) {
      throw new SeedError(
        `analytics ingestion status returned HTTP ${response.status}`,
        "confirm the web and worker analytics runtimes are ready",
      );
    }
    const result = (await response.json()) as { status?: unknown };
    if (result.status === "VISIBLE") return;
    if (
      typeof result.status === "string" &&
      TERMINAL_OPERATION_STATUSES.has(result.status)
    ) {
      throw new SeedError(
        `analytics ingestion operation ended in ${result.status}`,
        "inspect web/worker logs before replaying the same smoke seed",
      );
    }
    await sleep(250);
  }
  throw new SeedError(
    `timed out waiting for analytics ingestion operation ${operationId}`,
    "start the worker and confirm the analytics ingestion queue is healthy",
  );
};

const waitForTraceReadback = async (input: {
  baseUrl: string;
  traceId: string;
  spanId: string;
  authorization: string;
}): Promise<Record<string, unknown>> => {
  const deadline = Date.now() + WAIT_TIMEOUT_MS;
  let attempt = 0;
  while (Date.now() < deadline) {
    attempt += 1;
    const url = `${input.baseUrl}/api/public/traces/${encodeURIComponent(input.traceId)}?fields=observations&readbackAttempt=${attempt}`;
    const response = await request(url, {
      method: "GET",
      cache: "no-store",
      headers: {
        Accept: "application/json",
        Authorization: input.authorization,
        "Cache-Control": "no-cache",
      },
    });
    if (response.status === 404) {
      await sleep(250);
      continue;
    }
    if (!response.ok) {
      throw new SeedError(
        `trace readback returned HTTP ${response.status}`,
        "confirm the seed API key belongs to --project and the selected backend is ready",
      );
    }
    const trace = asRecord(await response.json());
    const observations = Array.isArray(trace?.["observations"])
      ? trace["observations"]
      : [];
    if (
      trace?.["id"] === input.traceId &&
      observations.some(
        (observation) => asRecord(observation)?.["id"] === input.spanId,
      )
    ) {
      return trace;
    }
    await sleep(250);
  }
  throw new SeedError(
    `readback mismatch: trace ${input.traceId} or observation ${input.spanId} was not visible`,
    "confirm the worker is running and its analytics backend matches LANGFUSE_ANALYTICS_BACKEND",
  );
};

const run = async (
  ctx: ScenarioContext,
  params: Record<string, string | number | boolean>,
): Promise<SeedSummary> => {
  const startedAt = Date.now();
  if (ctx.projectId !== DEFAULT_SEED_PROJECT_ID) {
    throw new SeedError(
      `analytics-smoke only supports the default seed project ${DEFAULT_SEED_PROJECT_ID}`,
      "omit --project or use the default seed project; its fixed local API key protects project-scoped readback",
    );
  }
  const traceId = stableHexId(`${ctx.projectId}:${ctx.idPrefix}:trace`, 32);
  const spanId = stableHexId(`${ctx.projectId}:${ctx.idPrefix}:span`, 16);
  const startMs = fixtureStartMs(params["fixture-date"]);
  const payload = buildOtlpFixture({
    traceId,
    spanId,
    startMs,
    environment: ctx.environment,
  });
  const fixtureHash = sha256({
    evidenceContractVersion: EVIDENCE_CONTRACT_VERSION,
    payload: buildOtlpFixture({
      traceId: TRACE_ID_PLACEHOLDER,
      spanId: SPAN_ID_PLACEHOLDER,
      startMs,
      environment: ctx.environment,
    }),
  });
  const traceUrl = `${ctx.baseUrl}/project/${ctx.projectId}/traces/${traceId}?timestamp=${encodeURIComponent(new Date(startMs).toISOString())}`;
  const summary = (input: {
    verified: Record<string, number>;
    semanticHash: string | null;
    operationId: string | null;
  }): SeedSummary => ({
    scenario: "analytics-smoke",
    target: ctx.backend,
    params,
    projectId: ctx.projectId,
    environment: ctx.environment,
    traceIds: [traceId],
    sessionIds: [],
    counts: { traces: 1, observations: 1 },
    verified: input.verified,
    links: [traceUrl],
    dryRun: ctx.dryRun,
    durationMs: Date.now() - startedAt,
    evidenceContractVersion: EVIDENCE_CONTRACT_VERSION,
    fixtureHash,
    semanticHash: input.semanticHash,
    operationId: input.operationId,
  });

  if (ctx.dryRun) {
    return summary({ verified: {}, semanticHash: null, operationId: null });
  }

  const { publicKey, secretKey } = credentials();
  const authorization = basicAuth(publicKey, secretKey);

  ctx.log(`submitting public OTLP smoke trace to ${ctx.backend}`);
  const response = await request(
    `${ctx.baseUrl}/api/public/otel/v1/traces`,
    {
      method: "POST",
      headers: {
        Accept: "application/json",
        "Content-Type": "application/json",
        Authorization: authorization,
        "x-langfuse-ingestion-version": "4",
        "x-langfuse-sdk-name": "langfuse-seeder",
        "x-langfuse-sdk-version": "1",
      },
      body: JSON.stringify(payload),
    },
    30_000,
  );
  if (!response.ok) {
    throw new SeedError(
      `public OTLP ingestion returned HTTP ${response.status}`,
      "confirm the local seed API key, web readiness, and worker availability",
    );
  }

  const operationId = response.headers.get("x-langfuse-ingestion-operation-id");
  if (ctx.backend === "doris" && !operationId) {
    throw new SeedError(
      "Doris smoke ingestion returned no analytics operation ID",
      "restart the web app with LANGFUSE_ANALYTICS_BACKEND=doris; the running web process appears to use ClickHouse",
    );
  }
  if (ctx.backend === "clickhouse" && operationId) {
    throw new SeedError(
      "ClickHouse smoke ingestion unexpectedly returned a Doris analytics operation ID",
      "restart the web app with LANGFUSE_ANALYTICS_BACKEND=clickhouse",
    );
  }
  if (operationId) {
    await waitForDorisOperation(ctx.baseUrl, operationId, authorization);
  }
  const trace = await waitForTraceReadback({
    baseUrl: ctx.baseUrl,
    traceId,
    spanId,
    authorization,
  });
  const semanticHash = sha256({
    evidenceContractVersion: EVIDENCE_CONTRACT_VERSION,
    trace: normalizePublicTraceReadback({
      trace,
      projectId: ctx.projectId,
      traceId,
      spanId,
    }),
  });
  return summary({
    verified: { traces: 1, observations: 1 },
    semanticHash,
    operationId,
  });
};

export const analyticsSmokeScenario: ScenarioDefinition = {
  name: "analytics-smoke",
  description:
    "One trace and observation through the public OTLP endpoint, followed by backend-neutral public API readback. Supported by ClickHouse and Doris.",
  supportsV4: false,
  flags: [ANALYTICS_SMOKE_FIXTURE_DATE_FLAG],
  run,
};
