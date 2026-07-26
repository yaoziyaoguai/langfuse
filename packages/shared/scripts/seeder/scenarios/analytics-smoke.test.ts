import { afterEach, describe, expect, it, vi } from "vitest";

import { DEFAULT_SEED_PROJECT_ID, DEFAULT_SEED_PUBLIC_KEY } from "../defaults";
import { analyticsSmokeScenario } from "./analytics-smoke";
import type { ScenarioContext } from "./types";

const FIXTURE_DATE = "2026-07-17";
const params = (fixtureDate = FIXTURE_DATE) => ({
  "fixture-date": fixtureDate,
});

const context = (backend: "clickhouse" | "doris"): ScenarioContext => ({
  projectId: DEFAULT_SEED_PROJECT_ID,
  environment: "default",
  seed: 42,
  idPrefix: "analytics-smoke-s42",
  dryRun: false,
  baseUrl: "http://localhost:3000",
  backend,
  log: vi.fn(),
});

afterEach(() => {
  vi.unstubAllEnvs();
  vi.unstubAllGlobals();
});

describe("analytics-smoke scenario", () => {
  it("dry-runs without credentials or network access", async () => {
    const fetchSpy = vi.fn();
    vi.stubGlobal("fetch", fetchSpy);
    const ctx = { ...context("doris"), dryRun: true };

    const summary = await analyticsSmokeScenario.run(ctx, params());

    expect(summary).toMatchObject({
      scenario: "analytics-smoke",
      target: "doris",
      params: { "fixture-date": FIXTURE_DATE },
      counts: { traces: 1, observations: 1 },
      verified: {},
      evidenceContractVersion: 1,
      fixtureHash:
        "6c51d42a8ecac92eb0b53e8e8b1cdcfc1294354d7d030e8af974ef7cd473871b",
      semanticHash: null,
      operationId: null,
      dryRun: true,
    });
    expect(fetchSpy).not.toHaveBeenCalled();
  });

  it("uses the fixture date at UTC noon without changing deterministic IDs", async () => {
    const ctx = { ...context("clickhouse"), dryRun: true };

    const first = await analyticsSmokeScenario.run(ctx, params("2026-02-28"));
    const second = await analyticsSmokeScenario.run(ctx, params("2026-03-01"));

    expect(first.traceIds).toEqual(second.traceIds);
    expect(first.fixtureHash).not.toBe(second.fixtureHash);
    expect(first.links[0]).toContain(
      encodeURIComponent("2026-02-28T12:00:00.000Z"),
    );
    expect(second.links[0]).toContain(
      encodeURIComponent("2026-03-01T12:00:00.000Z"),
    );
  });

  it.each(["2026-2-01", "2026-02-29", "not-a-date"])(
    "rejects invalid fixture date %s before network access",
    async (fixtureDate) => {
      const fetchSpy = vi.fn();
      vi.stubGlobal("fetch", fetchSpy);

      await expect(
        analyticsSmokeScenario.run(context("doris"), params(fixtureDate)),
      ).rejects.toThrow("--fixture-date expects YYYY-MM-DD");
      expect(fetchSpy).not.toHaveBeenCalled();
    },
  );

  it("uses public OTLP ingestion and backend-neutral trace readback for Doris", async () => {
    vi.stubEnv("SEED_SECRET_KEY", "secret-not-for-output");
    let traceId = "";
    let spanId = "";
    const fetchSpy = vi.fn(
      async (input: string | URL | Request, init?: RequestInit) => {
        const url = String(input);
        if (url.endsWith("/api/public/otel/v1/traces")) {
          const payload = JSON.parse(String(init?.body)) as {
            resourceSpans: Array<{
              scopeSpans: Array<{
                spans: Array<{ traceId: string; spanId: string }>;
              }>;
            }>;
          };
          ({ traceId, spanId } =
            payload.resourceSpans[0]!.scopeSpans[0]!.spans[0]!);
          return new Response("{}", {
            status: 200,
            headers: { "x-langfuse-ingestion-operation-id": "operation-id" },
          });
        }
        if (url.includes("/api/public/ingestion-operations/")) {
          return Response.json({ status: "VISIBLE" });
        }
        if (url.includes("/api/public/traces/")) {
          return Response.json({ id: traceId, observations: [{ id: spanId }] });
        }
        throw new Error(`unexpected URL: ${url}`);
      },
    );
    vi.stubGlobal("fetch", fetchSpy);

    const summary = await analyticsSmokeScenario.run(
      context("doris"),
      params(),
    );

    expect(summary).toMatchObject({
      target: "doris",
      verified: { traces: 1, observations: 1 },
      evidenceContractVersion: 1,
      fixtureHash: expect.stringMatching(/^[a-f0-9]{64}$/),
      semanticHash: expect.stringMatching(/^[a-f0-9]{64}$/),
      operationId: "operation-id",
      dryRun: false,
    });
    expect(fetchSpy).toHaveBeenCalledWith(
      "http://localhost:3000/api/public/otel/v1/traces",
      expect.objectContaining({ method: "POST", redirect: "error" }),
    );
    const postInit = fetchSpy.mock.calls[0]?.[1] as RequestInit;
    expect(postInit.headers).toMatchObject({
      Authorization: `Basic ${Buffer.from(`${DEFAULT_SEED_PUBLIC_KEY}:secret-not-for-output`).toString("base64")}`,
    });
    expect(JSON.stringify(summary)).not.toContain("secret-not-for-output");
  });

  it("uses public OTLP ingestion and trace readback without Doris status polling for ClickHouse", async () => {
    let traceId = "";
    let spanId = "";
    const fetchSpy = vi.fn(
      async (input: string | URL | Request, init?: RequestInit) => {
        const url = String(input);
        if (url.endsWith("/api/public/otel/v1/traces")) {
          const payload = JSON.parse(String(init?.body)) as {
            resourceSpans: Array<{
              scopeSpans: Array<{
                spans: Array<{ traceId: string; spanId: string }>;
              }>;
            }>;
          };
          ({ traceId, spanId } =
            payload.resourceSpans[0]!.scopeSpans[0]!.spans[0]!);
          return Response.json({ success: true });
        }
        if (url.includes("/api/public/traces/")) {
          return Response.json({ id: traceId, observations: [{ id: spanId }] });
        }
        throw new Error(`unexpected URL: ${url}`);
      },
    );
    vi.stubGlobal("fetch", fetchSpy);

    const summary = await analyticsSmokeScenario.run(
      context("clickhouse"),
      params(),
    );

    expect(summary).toMatchObject({
      target: "clickhouse",
      verified: { traces: 1, observations: 1 },
      evidenceContractVersion: 1,
      fixtureHash: expect.stringMatching(/^[a-f0-9]{64}$/),
      semanticHash: expect.stringMatching(/^[a-f0-9]{64}$/),
      operationId: null,
      dryRun: false,
    });
    expect(
      fetchSpy.mock.calls.some(([input]) =>
        String(input).includes("/api/public/ingestion-operations/"),
      ),
    ).toBe(false);
  });

  it("produces backend-comparable hashes from allowlisted readback semantics", async () => {
    const runBackend = async (
      backend: "clickhouse" | "doris",
      reverseOrder: boolean,
    ) => {
      let traceId = "";
      let spanId = "";
      vi.stubGlobal(
        "fetch",
        vi.fn(async (input: string | URL | Request, init?: RequestInit) => {
          const url = String(input);
          if (url.endsWith("/api/public/otel/v1/traces")) {
            const payload = JSON.parse(String(init?.body)) as {
              resourceSpans: Array<{
                scopeSpans: Array<{
                  spans: Array<{ traceId: string; spanId: string }>;
                }>;
              }>;
            };
            ({ traceId, spanId } =
              payload.resourceSpans[0]!.scopeSpans[0]!.spans[0]!);
            return new Response("{}", {
              status: 200,
              headers:
                backend === "doris"
                  ? {
                      "x-langfuse-ingestion-operation-id":
                        "random-doris-operation",
                    }
                  : undefined,
            });
          }
          if (url.includes("/api/public/ingestion-operations/")) {
            return Response.json({ status: "VISIBLE" });
          }
          if (url.includes("/api/public/traces/")) {
            const expectedObservation = {
              updatedAt: backend === "doris" ? "2099-01-02" : "2098-01-02",
              metadata: reverseOrder
                ? {
                    scope: { version: "1", name: "langfuse-seeder" },
                    resourceAttributes: {
                      "service.name": "langfuse-seeder",
                    },
                    backendOnly: backend,
                  }
                : {
                    backendOnly: backend,
                    resourceAttributes: {
                      "service.name": "langfuse-seeder",
                    },
                    scope: { name: "langfuse-seeder", version: "1" },
                  },
              id: spanId,
              projectId: DEFAULT_SEED_PROJECT_ID,
              traceId,
              parentObservationId: null,
              name: "analytics-backend-smoke",
              type: "SPAN",
              environment: "default",
              startTime: "2026-07-17T12:00:00.000Z",
              endTime: "2026-07-17T12:00:00.250Z",
              input: null,
              output: null,
              level: "DEFAULT",
              statusMessage: null,
              createdAt: backend === "doris" ? "2099-01-01" : "2098-01-01",
            };
            const unrelatedObservation = {
              id: `unrelated-${backend}`,
              traceId,
              backendOnly: true,
            };
            return Response.json({
              updatedAt: backend === "doris" ? "2099-01-02" : "2098-01-02",
              id: traceId,
              projectId: DEFAULT_SEED_PROJECT_ID,
              timestamp: "2026-07-17T12:00:00.000Z",
              name: "analytics-backend-smoke",
              environment: "default",
              createdAt: backend === "doris" ? "2099-01-01" : "2098-01-01",
              observations: reverseOrder
                ? [unrelatedObservation, expectedObservation]
                : [expectedObservation, unrelatedObservation],
            });
          }
          throw new Error(`unexpected URL: ${url}`);
        }),
      );

      return analyticsSmokeScenario.run(
        {
          ...context(backend),
          idPrefix: `analytics-smoke-${backend}`,
        },
        params(),
      );
    };

    const clickhouse = await runBackend("clickhouse", false);
    const doris = await runBackend("doris", true);

    expect(clickhouse.fixtureHash).toBe(doris.fixtureHash);
    expect(clickhouse.semanticHash).toBe(doris.semanticHash);
    expect(clickhouse.operationId).toBeNull();
    expect(doris.operationId).toBe("random-doris-operation");
  });

  it("does not reuse a negatively cached trace readback URL", async () => {
    let traceId = "";
    let spanId = "";
    const traceReadbackUrls: string[] = [];
    const fetchSpy = vi.fn(
      async (input: string | URL | Request, init?: RequestInit) => {
        const url = String(input);
        if (url.endsWith("/api/public/otel/v1/traces")) {
          const payload = JSON.parse(String(init?.body)) as {
            resourceSpans: Array<{
              scopeSpans: Array<{
                spans: Array<{ traceId: string; spanId: string }>;
              }>;
            }>;
          };
          ({ traceId, spanId } =
            payload.resourceSpans[0]!.scopeSpans[0]!.spans[0]!);
          return Response.json({ success: true });
        }
        if (url.includes("/api/public/traces/")) {
          traceReadbackUrls.push(url);
          if (traceReadbackUrls.length === 1) {
            return new Response("{}", { status: 404 });
          }
          if (url === traceReadbackUrls[0]) {
            throw new Error("negative trace response URL was reused");
          }
          return Response.json({ id: traceId, observations: [{ id: spanId }] });
        }
        throw new Error(`unexpected URL: ${url}`);
      },
    );
    vi.stubGlobal("fetch", fetchSpy);

    await expect(
      analyticsSmokeScenario.run(context("clickhouse"), params()),
    ).resolves.toMatchObject({ verified: { traces: 1, observations: 1 } });
    expect(traceReadbackUrls).toHaveLength(2);
    expect(traceReadbackUrls[0]).not.toBe(traceReadbackUrls[1]);
  });

  it("fails Doris smoke when the selected web runtime returns no operation ID", async () => {
    const fetchSpy = vi.fn(async () => Response.json({ success: true }));
    vi.stubGlobal("fetch", fetchSpy);

    await expect(
      analyticsSmokeScenario.run(context("doris"), params()),
    ).rejects.toThrow("returned no analytics operation ID");
    expect(fetchSpy).toHaveBeenCalledOnce();
  });

  it("fails ClickHouse smoke when the web runtime returns a Doris operation ID", async () => {
    const fetchSpy = vi.fn(
      async () =>
        new Response("{}", {
          status: 200,
          headers: { "x-langfuse-ingestion-operation-id": "operation-id" },
        }),
    );
    vi.stubGlobal("fetch", fetchSpy);

    await expect(
      analyticsSmokeScenario.run(context("clickhouse"), params()),
    ).rejects.toThrow("unexpectedly returned a Doris analytics operation ID");
    expect(fetchSpy).toHaveBeenCalledOnce();
  });

  it("fails a terminal Doris ingestion operation without attempting readback", async () => {
    const fetchSpy = vi
      .fn()
      .mockResolvedValueOnce(
        new Response("{}", {
          status: 200,
          headers: { "x-langfuse-ingestion-operation-id": "operation-id" },
        }),
      )
      .mockResolvedValueOnce(Response.json({ status: "QUARANTINED" }));
    vi.stubGlobal("fetch", fetchSpy);

    await expect(
      analyticsSmokeScenario.run(context("doris"), params()),
    ).rejects.toThrow("ended in QUARANTINED");
    expect(fetchSpy).toHaveBeenCalledTimes(2);
  });

  it("rejects a project that cannot match the fixed local API key before ingestion", async () => {
    const fetchSpy = vi.fn();
    vi.stubGlobal("fetch", fetchSpy);

    await expect(
      analyticsSmokeScenario.run(
        { ...context("doris"), projectId: "another-project" },
        params(),
      ),
    ).rejects.toThrow("only supports the default seed project");
    expect(fetchSpy).not.toHaveBeenCalled();
  });

  it("turns network failures into an actionable seed error", async () => {
    const fetchSpy = vi.fn().mockRejectedValue(new Error("fetch failed"));
    vi.stubGlobal("fetch", fetchSpy);

    const error = await analyticsSmokeScenario
      .run(context("doris"), params())
      .catch((cause: unknown) => cause);

    expect(error).toMatchObject({
      name: "SeedError",
      message: "public API request failed (Error)",
      fix: expect.stringContaining("NEXTAUTH_URL"),
    });
    expect(fetchSpy).toHaveBeenCalledTimes(3);
  });

  it("retries a transient public API connection reset", async () => {
    let traceId = "";
    let spanId = "";
    const reset = Object.assign(new TypeError("fetch failed"), {
      cause: { code: "ECONNRESET" },
    });
    const fetchSpy = vi.fn(
      async (input: string | URL | Request, init?: RequestInit) => {
        const url = String(input);
        if (fetchSpy.mock.calls.length === 1) throw reset;
        if (url.endsWith("/api/public/otel/v1/traces")) {
          const payload = JSON.parse(String(init?.body)) as {
            resourceSpans: Array<{
              scopeSpans: Array<{
                spans: Array<{ traceId: string; spanId: string }>;
              }>;
            }>;
          };
          ({ traceId, spanId } =
            payload.resourceSpans[0]!.scopeSpans[0]!.spans[0]!);
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
        return Response.json({
          id: traceId,
          observations: [{ id: spanId }],
        });
      },
    );
    vi.stubGlobal("fetch", fetchSpy);

    await expect(
      analyticsSmokeScenario.run(context("doris"), params()),
    ).resolves.toMatchObject({ verified: { traces: 1, observations: 1 } });
    expect(fetchSpy.mock.calls[0]?.[0]).toBe(
      "http://localhost:3000/api/public/otel/v1/traces",
    );
    expect(fetchSpy.mock.calls[1]?.[0]).toBe(
      "http://localhost:3000/api/public/otel/v1/traces",
    );
  });
});
