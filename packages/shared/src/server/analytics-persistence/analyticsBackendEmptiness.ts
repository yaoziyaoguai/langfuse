import type { AnalyticsBackendEmptinessEvidence } from "../repositories/analyticsBackendDeployment";
import {
  probeClickHouseAnalyticsBackendEmptiness,
  type ClickHouseEmptinessQueryExecutor,
} from "../clickhouse/emptiness";
import type { DorisQueryExecutor } from "../doris/client";
import { probeDorisAnalyticsBackendEmptiness } from "../doris/emptiness";
import type { AnalyticsBackend } from "./analyticsBackend";

export type SelectedAnalyticsBackendEmptinessInput =
  | {
      readonly backend: "clickhouse";
      readonly clickhouseExecutor?: ClickHouseEmptinessQueryExecutor;
    }
  | {
      readonly backend: "doris";
      readonly dorisExecutor?: DorisQueryExecutor;
    };

export type SelectedAnalyticsBackendEmptinessResult = {
  readonly selectedBackendEmpty: boolean;
  readonly evidenceDigest: string;
};

export async function probeSelectedAnalyticsBackendEmptiness(
  input: SelectedAnalyticsBackendEmptinessInput,
): Promise<SelectedAnalyticsBackendEmptinessResult> {
  const result =
    input.backend === "clickhouse"
      ? await probeClickHouseAnalyticsBackendEmptiness({
          executor: input.clickhouseExecutor,
        })
      : await probeDorisAnalyticsBackendEmptiness({
          executor: input.dorisExecutor,
        });

  return {
    selectedBackendEmpty: result.empty,
    evidenceDigest: result.evidenceDigest,
  };
}

export async function probeAnalyticsBackendSwitchEmptiness(input: {
  readonly sourceBackend: AnalyticsBackend;
  readonly targetBackend: AnalyticsBackend;
  readonly clickhouseExecutor?: ClickHouseEmptinessQueryExecutor;
  readonly dorisExecutor?: DorisQueryExecutor;
}): Promise<AnalyticsBackendEmptinessEvidence> {
  if (input.sourceBackend === input.targetBackend) {
    throw new TypeError(
      "Switch emptiness probes require different analytics backends",
    );
  }

  const clickhouse = await probeClickHouseAnalyticsBackendEmptiness({
    executor: input.clickhouseExecutor,
  });
  const doris = await probeDorisAnalyticsBackendEmptiness({
    executor: input.dorisExecutor,
  });
  const byBackend = { clickhouse, doris } as const;

  return {
    source: {
      backend: input.sourceBackend,
      empty: byBackend[input.sourceBackend].empty,
      evidenceDigest: byBackend[input.sourceBackend].evidenceDigest,
    },
    target: {
      backend: input.targetBackend,
      empty: byBackend[input.targetBackend].empty,
      evidenceDigest: byBackend[input.targetBackend].evidenceDigest,
    },
  };
}
