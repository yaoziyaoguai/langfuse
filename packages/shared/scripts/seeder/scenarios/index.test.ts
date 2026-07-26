import { afterEach, describe, expect, it, vi } from "vitest";

import {
  listScenarioRegistrations,
  loadScenarioDefinition,
  resolveScenarioRegistration,
  scenarioRegistry,
  type ScenarioRegistry,
} from ".";
import type { ScenarioDefinition } from "./types";

afterEach(() => vi.unstubAllEnvs());

const definition: ScenarioDefinition = {
  name: "legacy",
  description: "legacy ClickHouse scenario",
  flags: [],
  supportsV4: false,
  run: vi.fn(),
};

const registry = (): {
  entries: ScenarioRegistry;
  load: ReturnType<typeof vi.fn>;
} => {
  const load = vi.fn(async () => definition);
  return {
    load,
    entries: {
      legacy: {
        name: "legacy",
        description: definition.description,
        flags: [],
        supportsV4: false,
        supportedBackends: ["clickhouse"],
        needsWeb: false,
        loadsSharedClients: true,
        load,
      },
    },
  };
};

describe("lazy scenario registry", () => {
  it("advertises the analytics smoke fixture date as an additive UTC date flag", () => {
    const registration = scenarioRegistry["analytics-smoke"];

    expect(registration?.flags).toEqual([
      expect.objectContaining({
        flag: "fixture-date",
        type: "string",
        default: expect.stringMatching(/^\d{4}-\d{2}-\d{2}$/),
      }),
    ]);
  });

  it("advertises the experiment foundation scenario on both backends", () => {
    expect(scenarioRegistry["experiment-foundation"]).toMatchObject({
      supportedBackends: ["clickhouse", "doris"],
      needsWeb: true,
      loadsSharedClients: false,
    });
  });

  it("advertises backend-neutral evaluator states with Postgres cleanup", () => {
    expect(scenarioRegistry["evaluator-states"]).toMatchObject({
      supportedBackends: ["clickhouse", "doris"],
      needsWeb: true,
      loadsSharedClients: true,
    });
  });

  it("advertises zero-egress integration states on both backends", () => {
    expect(scenarioRegistry["integration-states"]).toMatchObject({
      supportedBackends: ["clickhouse", "doris"],
      needsWeb: false,
      loadsSharedClients: true,
    });
  });

  it("lists Doris availability without loading ClickHouse scenario code", () => {
    const { entries, load } = registry();

    expect(listScenarioRegistrations("doris", entries)).toEqual([
      expect.objectContaining({
        name: "legacy",
        target: "doris",
        availability: "unavailable",
        supportedBackends: ["clickhouse"],
      }),
    ]);
    expect(load).not.toHaveBeenCalled();
  });

  it("fails an unsupported Doris scenario before loading its implementation", () => {
    const { entries, load } = registry();

    expect(() =>
      resolveScenarioRegistration("legacy", "doris", entries),
    ).toThrow('scenario "legacy" is unavailable for analytics backend "doris"');
    expect(load).not.toHaveBeenCalled();
  });

  it("loads an available ClickHouse scenario only after selection", async () => {
    const { entries, load } = registry();
    const registration = resolveScenarioRegistration(
      "legacy",
      "clickhouse",
      entries,
    );

    await expect(loadScenarioDefinition(registration)).resolves.toBe(
      definition,
    );
    expect(load).toHaveBeenCalledOnce();
  });

  it("keeps lazy registry metadata aligned with every scenario module", async () => {
    vi.stubEnv("LANGFUSE_S3_EVENT_UPLOAD_BUCKET", "seeder-unit-test");
    vi.stubEnv("LANGFUSE_ANALYTICS_BACKEND", "clickhouse");

    for (const registration of Object.values(scenarioRegistry)) {
      const loaded = await loadScenarioDefinition(registration);
      expect({
        name: loaded.name,
        description: loaded.description,
        flags: loaded.flags,
        supportsV4: loaded.supportsV4,
      }).toEqual({
        name: registration.name,
        description: registration.description,
        flags: registration.flags,
        supportsV4: registration.supportsV4,
      });
    }
  }, 120_000);
});
