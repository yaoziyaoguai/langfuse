import { afterEach, describe, expect, it, vi } from "vitest";

import { DEFAULT_SEED_PROJECT_ID } from "../defaults";
import type { ScenarioContext, SeedSummary } from "./types";

const mocks = vi.hoisted(() => {
  const client = {
    evalTemplate: {
      upsert: vi.fn().mockResolvedValue({ id: "template-id" }),
      count: vi.fn().mockResolvedValue(1),
    },
    jobConfiguration: {
      upsert: vi.fn(
        async ({
          where,
        }: {
          where: { id: string };
          create: { status: string };
        }) => ({
          id: where.id,
        }),
      ),
      count: vi.fn().mockResolvedValue(2),
    },
    jobExecution: {
      upsert: vi.fn(),
      count: vi.fn().mockResolvedValue(6),
    },
  };
  return {
    client,
    transaction: vi.fn(async (run: (tx: typeof client) => unknown) =>
      run(client),
    ),
    analyticsSmokeRun: vi.fn(),
  };
});

vi.mock("../../../src/db", () => ({
  prisma: {
    ...mocks.client,
    $transaction: mocks.transaction,
  },
}));

vi.mock("./analytics-smoke", () => ({
  analyticsSmokeScenario: {
    run: mocks.analyticsSmokeRun,
  },
}));

import { evaluatorStatesScenario } from "./evaluator-states";

const context = (backend: "clickhouse" | "doris"): ScenarioContext => ({
  projectId: DEFAULT_SEED_PROJECT_ID,
  environment: "default",
  seed: 42,
  idPrefix: "evaluator-states-s42",
  dryRun: false,
  baseUrl: "http://localhost:3000",
  backend,
  log: vi.fn(),
});

const smokeSummary = (
  backend: "clickhouse" | "doris",
  dryRun: boolean,
): SeedSummary => ({
  scenario: "analytics-smoke",
  target: backend,
  params: { "fixture-date": "2026-07-17" },
  projectId: DEFAULT_SEED_PROJECT_ID,
  environment: "default",
  traceIds: ["0123456789abcdef0123456789abcdef"],
  sessionIds: [],
  counts: { traces: 1, observations: 1 },
  verified: dryRun ? {} : { traces: 1, observations: 1 },
  links: dryRun ? [] : ["http://localhost:3000/project/test/traces/trace"],
  dryRun,
  durationMs: 1,
  operationId: backend === "doris" && !dryRun ? "operation-id" : null,
});

afterEach(() => {
  vi.clearAllMocks();
  vi.unstubAllEnvs();
  vi.unstubAllGlobals();
});

describe("evaluator-states scenario", () => {
  it("dry-runs all user-visible states without network or database writes", async () => {
    const fetchSpy = vi.fn();
    vi.stubGlobal("fetch", fetchSpy);
    mocks.analyticsSmokeRun.mockResolvedValue(smokeSummary("doris", true));

    const summary = await evaluatorStatesScenario.run(
      { ...context("doris"), dryRun: true },
      { "fixture-date": "2026-07-17" },
    );

    expect(summary).toMatchObject({
      scenario: "evaluator-states",
      target: "doris",
      counts: {
        traces: 1,
        observations: 1,
        evaluatorTemplates: 1,
        evaluationRules: 2,
        jobExecutions: 6,
        scores: 2,
      },
      verified: {},
      dryRun: true,
      operationId: null,
    });
    expect(fetchSpy).not.toHaveBeenCalled();
    expect(mocks.transaction).not.toHaveBeenCalled();
  });

  it.each(["clickhouse", "doris"] as const)(
    "uses public analytics writes and Postgres-only control fixtures on %s",
    async (backend) => {
      vi.stubEnv("SEED_SECRET_KEY", "seed-secret");
      mocks.analyticsSmokeRun.mockResolvedValue(smokeSummary(backend, false));
      const scoreIds: string[] = [];
      const fetchSpy = vi.fn(
        async (input: string | URL | Request, init?: RequestInit) => {
          const url = String(input);
          if (url.endsWith("/api/public/scores")) {
            const id = String(
              (JSON.parse(String(init?.body)) as { id?: unknown }).id,
            );
            scoreIds.push(id);
            return Response.json({ id });
          }
          if (url.includes("/api/public/v2/scores?")) {
            return Response.json({
              data: scoreIds.map((id) => ({ id })),
              meta: {
                page: 1,
                limit: 100,
                totalItems: scoreIds.length,
                totalPages: 1,
              },
            });
          }
          throw new Error(`unexpected URL: ${url}`);
        },
      );
      vi.stubGlobal("fetch", fetchSpy);

      const summary = await evaluatorStatesScenario.run(context(backend), {
        "fixture-date": "2026-07-17",
      });

      expect(summary).toMatchObject({
        target: backend,
        verified: {
          traces: 1,
          observations: 1,
          evaluatorTemplates: 1,
          evaluationRules: 2,
          jobExecutions: 6,
          scores: 2,
        },
        operationId: backend === "doris" ? "operation-id" : null,
      });
      expect(mocks.client.jobConfiguration.upsert).toHaveBeenCalledTimes(2);
      expect(
        mocks.client.jobConfiguration.upsert.mock.calls.map(
          ([input]) => input.create.status,
        ),
      ).toEqual(["INACTIVE", "INACTIVE"]);
      expect(mocks.client.jobExecution.upsert).toHaveBeenCalledTimes(6);
      expect(
        mocks.client.jobExecution.upsert.mock.calls.map(
          ([input]) => input.create.status,
        ),
      ).toEqual([
        "COMPLETED",
        "COMPLETED",
        "ERROR",
        "DELAYED",
        "PENDING",
        "PENDING",
      ]);
      expect(fetchSpy.mock.calls.map(([url]) => String(url))).not.toContain(
        "clickhouse",
      );
    },
  );

  it("rejects a non-default project before any write", async () => {
    await expect(
      evaluatorStatesScenario.run(
        { ...context("doris"), projectId: "other-project" },
        { "fixture-date": "2026-07-17" },
      ),
    ).rejects.toThrow("only supports the default seed project");
    expect(mocks.analyticsSmokeRun).not.toHaveBeenCalled();
    expect(mocks.transaction).not.toHaveBeenCalled();
  });
});
