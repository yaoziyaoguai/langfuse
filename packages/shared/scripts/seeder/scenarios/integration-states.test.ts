import { afterEach, describe, expect, it, vi } from "vitest";

import { DEFAULT_SEED_PROJECT_ID } from "../defaults";
import type { ScenarioContext } from "./types";

const mocks = vi.hoisted(() => {
  const client = {
    posthogIntegration: {
      upsert: vi.fn(),
      count: vi.fn().mockResolvedValue(1),
    },
    mixpanelIntegration: {
      upsert: vi.fn(),
      count: vi.fn().mockResolvedValue(1),
    },
    blobStorageIntegration: {
      upsert: vi.fn(),
      count: vi.fn().mockResolvedValue(1),
    },
  };
  return {
    client,
    projectFindUnique: vi
      .fn()
      .mockResolvedValue({ id: "7a88fb47-b4e2-43b8-a06c-a5ce950dc53a" }),
    transaction: vi.fn(async (run: (tx: typeof client) => unknown) =>
      run(client),
    ),
    encrypt: vi.fn((value: string) => `encrypted:${value}`),
  };
});

vi.mock("../../../src/db.js", () => ({
  prisma: {
    ...mocks.client,
    project: { findUnique: mocks.projectFindUnique },
    $transaction: mocks.transaction,
  },
}));

vi.mock("../../../src/encryption/index.js", () => ({
  encrypt: mocks.encrypt,
}));

import { integrationStatesScenario } from "./integration-states";

const context = (backend: "clickhouse" | "doris"): ScenarioContext => ({
  projectId: DEFAULT_SEED_PROJECT_ID,
  environment: "default",
  seed: 42,
  idPrefix: "integration-states-s42",
  dryRun: false,
  baseUrl: "http://localhost:3000",
  backend,
  log: vi.fn(),
});

afterEach(() => vi.clearAllMocks());

describe("integration-states scenario", () => {
  it("dry-runs without encryption, network, or database writes", async () => {
    const fetchSpy = vi.fn();
    vi.stubGlobal("fetch", fetchSpy);

    const result = await integrationStatesScenario.run(
      { ...context("doris"), dryRun: true },
      {},
    );

    expect(result).toMatchObject({
      scenario: "integration-states",
      target: "doris",
      counts: {
        posthogIntegrations: 1,
        mixpanelIntegrations: 1,
        blobStorageIntegrations: 1,
      },
      verified: {},
      links: [],
      dryRun: true,
    });
    expect(fetchSpy).not.toHaveBeenCalled();
    expect(mocks.encrypt).not.toHaveBeenCalled();
    expect(mocks.transaction).not.toHaveBeenCalled();
  });

  it.each(["clickhouse", "doris"] as const)(
    "seeds disabled, zero-egress configuration states on %s",
    async (backend) => {
      const result = await integrationStatesScenario.run(context(backend), {});

      expect(result).toMatchObject({
        target: backend,
        verified: {
          posthogIntegrations: 1,
          mixpanelIntegrations: 1,
          blobStorageIntegrations: 1,
        },
      });
      expect(result.links).toHaveLength(3);
      expect(mocks.client.posthogIntegration.upsert).toHaveBeenCalledWith(
        expect.objectContaining({
          create: expect.objectContaining({
            enabled: false,
            posthogHostName: "https://example.invalid",
            exportSource: "EVENTS",
          }),
        }),
      );
      expect(mocks.client.mixpanelIntegration.upsert).toHaveBeenCalledWith(
        expect.objectContaining({
          create: expect.objectContaining({
            enabled: false,
            exportSource: "EVENTS",
          }),
        }),
      );
      expect(mocks.client.blobStorageIntegration.upsert).toHaveBeenCalledWith(
        expect.objectContaining({
          create: expect.objectContaining({
            enabled: false,
            endpoint: "https://example.invalid",
            exportSource:
              backend === "doris" ? "EVENTS" : "TRACES_OBSERVATIONS",
            lastError: expect.stringContaining("no outbound request"),
          }),
        }),
      );
    },
  );

  it("rejects a non-default project before encryption or writes", async () => {
    await expect(
      integrationStatesScenario.run(
        { ...context("doris"), projectId: "other-project" },
        {},
      ),
    ).rejects.toThrow("only supports the default seed project");
    expect(mocks.encrypt).not.toHaveBeenCalled();
    expect(mocks.transaction).not.toHaveBeenCalled();
  });
});
