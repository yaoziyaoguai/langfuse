import { afterEach, describe, expect, it, vi } from "vitest";

import { DEFAULT_SEED_PROJECT_ID } from "../defaults";
import { experimentFoundationScenario } from "./experiment-foundation";
import type { ScenarioContext } from "./types";

const context = (backend: "clickhouse" | "doris"): ScenarioContext => ({
  projectId: DEFAULT_SEED_PROJECT_ID,
  environment: "default",
  seed: 42,
  idPrefix: "experiment-foundation-s42",
  dryRun: false,
  baseUrl: "http://localhost:3000",
  backend,
  log: vi.fn(),
});

afterEach(() => {
  vi.unstubAllEnvs();
  vi.unstubAllGlobals();
});

describe("experiment-foundation scenario", () => {
  it("dry-runs deterministically without credentials or network access", async () => {
    const fetchSpy = vi.fn();
    vi.stubGlobal("fetch", fetchSpy);

    const summary = await experimentFoundationScenario.run(
      { ...context("doris"), dryRun: true },
      { "fixture-date": "2026-07-17" },
    );

    expect(summary).toMatchObject({
      scenario: "experiment-foundation",
      target: "doris",
      counts: {
        datasets: 1,
        datasetItems: 1,
        traces: 1,
        observations: 1,
        datasetRunItems: 1,
        scores: 1,
      },
      verified: {},
      dryRun: true,
      operationId: null,
      fixtureHash: expect.stringMatching(/^[a-f0-9]{64}$/),
    });
    expect(fetchSpy).not.toHaveBeenCalled();
  });

  it.each(["clickhouse", "doris"] as const)(
    "uses only public/canonical surfaces and verifies %s readback",
    async (backend) => {
      vi.stubEnv("SEED_SECRET_KEY", "seed-secret");
      let traceId = "";
      let spanId = "";
      let datasetItemId = "";
      let scoreId = "";
      let runItemCreated = false;
      let scoreCreated = false;
      const fetchSpy = vi.fn(
        async (input: string | URL | Request, init?: RequestInit) => {
          const url = String(input);
          if (url.endsWith("/api/public/v2/datasets")) {
            return Response.json({ id: "dataset-id" });
          }
          if (url.endsWith("/api/public/dataset-items")) {
            datasetItemId = String(
              (JSON.parse(String(init?.body)) as { id?: unknown }).id,
            );
            return Response.json({ id: datasetItemId });
          }
          if (url.endsWith("/api/public/otel/v1/traces")) {
            const body = JSON.parse(String(init?.body)) as {
              resourceSpans: Array<{
                scopeSpans: Array<{
                  spans: Array<{ traceId: string; spanId: string }>;
                }>;
              }>;
            };
            ({ traceId, spanId } =
              body.resourceSpans[0]!.scopeSpans[0]!.spans[0]!);
            return new Response("{}", {
              status: 200,
              headers:
                backend === "doris"
                  ? { "x-langfuse-ingestion-operation-id": "operation-id" }
                  : undefined,
            });
          }
          if (url.includes("/api/public/ingestion-operations/")) {
            return Response.json({ status: "VISIBLE" });
          }
          if (
            url.includes("/api/public/traces/") &&
            !url.includes("/dataset-run-items")
          ) {
            return Response.json({
              id: traceId,
              observations: [{ id: spanId }],
            });
          }
          if (
            url.endsWith("/api/public/dataset-run-items") &&
            init?.method === "POST"
          ) {
            runItemCreated = true;
            return Response.json({
              id: "run-item-id",
              datasetRunId: "run-id",
            });
          }
          if (url.endsWith("/api/public/scores") && init?.method === "POST") {
            scoreCreated = true;
            const body = JSON.parse(String(init?.body)) as {
              id?: unknown;
              traceId?: unknown;
              datasetRunId?: unknown;
            };
            expect(body.traceId).toBeUndefined();
            expect(body.datasetRunId).toBe("run-id");
            scoreId = String(body.id);
            return Response.json({ id: scoreId });
          }
          if (url.includes("/api/public/dataset-run-items?")) {
            return Response.json({
              data: runItemCreated
                ? [
                    {
                      id: "run-item-id",
                      datasetRunId: "run-id",
                      datasetItemId,
                      traceId,
                    },
                  ]
                : [],
            });
          }
          if (url.includes("/api/public/v2/scores?")) {
            return Response.json({
              data: scoreCreated
                ? [{ id: scoreId, datasetRunId: "run-id" }]
                : [],
            });
          }
          throw new Error(`unexpected URL: ${url}`);
        },
      );
      vi.stubGlobal("fetch", fetchSpy);

      const summary = await experimentFoundationScenario.run(context(backend), {
        "fixture-date": "2026-07-17",
      });

      expect(summary).toMatchObject({
        target: backend,
        verified: {
          datasets: 1,
          datasetItems: 1,
          traces: 1,
          observations: 1,
          datasetRunItems: 1,
          scores: 1,
        },
        operationId: backend === "doris" ? "operation-id" : null,
        semanticHash: expect.stringMatching(/^[a-f0-9]{64}$/),
      });
      expect(summary.links[0]).toBe(
        `http://localhost:3000/project/${DEFAULT_SEED_PROJECT_ID}/experiments/results?baseline=run-id`,
      );
      expect(fetchSpy.mock.calls.map(([url]) => String(url))).not.toContain(
        "clickhouse",
      );
    },
  );

  it("reuses an existing deterministic run item and score", async () => {
    vi.stubEnv("SEED_SECRET_KEY", "seed-secret");
    let traceId = "";
    let spanId = "";
    let datasetItemId = "";
    const postRunItem = vi.fn();
    const postScore = vi.fn();
    const fetchSpy = vi.fn(
      async (input: string | URL | Request, init?: RequestInit) => {
        const url = String(input);
        if (url.endsWith("/api/public/v2/datasets")) {
          return Response.json({ id: "dataset-id" });
        }
        if (url.endsWith("/api/public/dataset-items")) {
          datasetItemId = String(
            (JSON.parse(String(init?.body)) as { id?: unknown }).id,
          );
          return Response.json({ id: datasetItemId });
        }
        if (url.endsWith("/api/public/otel/v1/traces")) {
          const body = JSON.parse(String(init?.body)) as {
            resourceSpans: Array<{
              scopeSpans: Array<{
                spans: Array<{ traceId: string; spanId: string }>;
              }>;
            }>;
          };
          ({ traceId, spanId } =
            body.resourceSpans[0]!.scopeSpans[0]!.spans[0]!);
          return new Response("{}", {
            status: 200,
            headers: {
              "x-langfuse-ingestion-operation-id": "operation-id",
            },
          });
        }
        if (url.includes("/api/public/ingestion-operations/")) {
          return Response.json({ status: "VISIBLE" });
        }
        if (url.includes("/api/public/traces/")) {
          return Response.json({
            id: traceId,
            observations: [{ id: spanId }],
          });
        }
        if (
          url.endsWith("/api/public/dataset-run-items") &&
          init?.method === "POST"
        ) {
          postRunItem();
          throw new Error("run item POST must not be called");
        }
        if (url.endsWith("/api/public/scores") && init?.method === "POST") {
          postScore();
          throw new Error("score POST must not be called");
        }
        if (url.includes("/api/public/dataset-run-items?")) {
          return Response.json({
            data: [
              {
                id: "existing-run-item-id",
                datasetRunId: "existing-run-id",
                datasetItemId,
                traceId,
              },
            ],
          });
        }
        if (url.includes("/api/public/v2/scores?")) {
          return Response.json({
            data: [
              {
                id: "foundation-score-a5fc37fda675",
                datasetRunId: "existing-run-id",
              },
            ],
          });
        }
        throw new Error(`unexpected URL: ${url}`);
      },
    );
    vi.stubGlobal("fetch", fetchSpy);

    const summary = await experimentFoundationScenario.run(context("doris"), {
      "fixture-date": "2026-07-17",
    });

    expect(summary.verified).toEqual(summary.counts);
    expect(postRunItem).not.toHaveBeenCalled();
    expect(postScore).not.toHaveBeenCalled();
  });

  it("rejects a non-default project before any write", async () => {
    const fetchSpy = vi.fn();
    vi.stubGlobal("fetch", fetchSpy);

    await expect(
      experimentFoundationScenario.run(
        { ...context("doris"), projectId: "other-project" },
        { "fixture-date": "2026-07-17" },
      ),
    ).rejects.toThrow("only supports the default seed project");
    expect(fetchSpy).not.toHaveBeenCalled();
  });
});
