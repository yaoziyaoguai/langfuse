import { describe, expect, it, vi } from "vitest";

import type { DorisQueryExecutor } from "../doris/client";
import {
  CLICKHOUSE_REQUIRED_ANALYTICS_PHYSICAL_TABLES,
  type ClickHouseEmptinessQueryExecutor,
} from "../clickhouse/emptiness";
import {
  probeAnalyticsBackendSwitchEmptiness,
  probeSelectedAnalyticsBackendEmptiness,
} from "./analyticsBackendEmptiness";

function emptyClickHouseExecutor(): ClickHouseEmptinessQueryExecutor {
  return {
    query: vi.fn(async (sql: string) => {
      if (sql.includes("FROM schema_migrations")) {
        return [{ version: 36, dirty: 0 }];
      }
      if (sql.includes("FROM system.tables")) {
        return [
          { name: "schema_migrations", engine: "MergeTree" },
          ...CLICKHOUSE_REQUIRED_ANALYTICS_PHYSICAL_TABLES.map((name) => ({
            name,
            engine: "ReplacingMergeTree",
          })),
        ];
      }
      if (sql.includes("FROM system.parts")) return [];
      throw new Error(`unexpected query: ${sql}`);
    }) as unknown as ClickHouseEmptinessQueryExecutor["query"],
  };
}

function unavailableDorisExecutor(): DorisQueryExecutor {
  return {
    query: vi
      .fn()
      .mockRejectedValue(
        new Error("Doris unavailable"),
      ) as unknown as DorisQueryExecutor["query"],
  };
}

describe("selected analytics backend emptiness", () => {
  it("probes only the explicitly selected backend", async () => {
    const clickhouseExecutor = emptyClickHouseExecutor();
    const dorisExecutor = unavailableDorisExecutor();

    await expect(
      probeSelectedAnalyticsBackendEmptiness({
        backend: "clickhouse",
        clickhouseExecutor,
      }),
    ).resolves.toMatchObject({
      selectedBackendEmpty: true,
      evidenceDigest: expect.stringMatching(/^[a-f0-9]{64}$/),
    });
    expect(dorisExecutor.query).not.toHaveBeenCalled();
  });

  it("returns fail-closed selected-backend evidence without exposing failures", async () => {
    await expect(
      probeSelectedAnalyticsBackendEmptiness({
        backend: "doris",
        dorisExecutor: unavailableDorisExecutor(),
      }),
    ).resolves.toMatchObject({
      selectedBackendEmpty: false,
      evidenceDigest: expect.stringMatching(/^[a-f0-9]{64}$/),
    });
  });
});

describe("analytics backend switch emptiness", () => {
  it("returns the deployment switch evidence contract for both explicit backends", async () => {
    const evidence = await probeAnalyticsBackendSwitchEmptiness({
      sourceBackend: "clickhouse",
      targetBackend: "doris",
      clickhouseExecutor: emptyClickHouseExecutor(),
      dorisExecutor: unavailableDorisExecutor(),
    });

    expect(evidence).toEqual({
      source: {
        backend: "clickhouse",
        empty: true,
        evidenceDigest: expect.stringMatching(/^[a-f0-9]{64}$/),
      },
      target: {
        backend: "doris",
        empty: false,
        evidenceDigest: expect.stringMatching(/^[a-f0-9]{64}$/),
      },
    });
  });

  it("rejects a same-backend switch probe", async () => {
    await expect(
      probeAnalyticsBackendSwitchEmptiness({
        sourceBackend: "clickhouse",
        targetBackend: "clickhouse",
        clickhouseExecutor: emptyClickHouseExecutor(),
      }),
    ).rejects.toThrow("different analytics backends");
  });
});
