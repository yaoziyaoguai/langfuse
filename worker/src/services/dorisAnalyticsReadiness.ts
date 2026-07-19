import {
  AnalyticsPersistenceError,
  checkAnalyticsReadiness,
  DorisClientManager,
  parseDorisQueryConfig,
  PrismaAnalyticsCompatibilityControlState,
  resolveDorisNodeEnv,
  SUPPORTED_DORIS_CANONICALIZER_VERSIONS,
  SUPPORTED_DORIS_SCHEMA_VERSIONS,
  type DorisReadinessResult,
} from "@langfuse/shared/src/server";
import { prisma } from "@langfuse/shared/src/db";

import { env } from "../env";

const CACHE_TTL_MS = 5_000;
let cached:
  | { readonly expiresAt: number; readonly result: DorisReadinessResult }
  | undefined;
let inFlight: Promise<DorisReadinessResult> | undefined;

export async function probeDorisAnalyticsReadiness(input?: {
  readonly force?: boolean;
}): Promise<DorisReadinessResult> {
  const now = Date.now();
  if (!input?.force && cached && cached.expiresAt > now) return cached.result;
  if (inFlight) return inFlight;

  inFlight = (async () => {
    const client = DorisClientManager.getInstance().getClient(
      parseDorisQueryConfig(
        process.env,
        resolveDorisNodeEnv(env.NODE_ENV, env.DORIS_LOCAL_DEV_MODE),
      ),
    );
    const result = await checkAnalyticsReadiness({
      executor: client,
      controlState: new PrismaAnalyticsCompatibilityControlState(prisma),
      supportedCanonicalizerVersions: SUPPORTED_DORIS_CANONICALIZER_VERSIONS,
      supportedSchemaVersions: SUPPORTED_DORIS_SCHEMA_VERSIONS,
    });
    cached = { expiresAt: Date.now() + CACHE_TTL_MS, result };
    return result;
  })();
  try {
    return await inFlight;
  } finally {
    inFlight = undefined;
  }
}

export async function assertDorisAnalyticsReady(input?: {
  readonly force?: boolean;
}): Promise<void> {
  const readiness = await probeDorisAnalyticsReadiness(input);
  if (!readiness.ready) {
    throw new AnalyticsPersistenceError("ANALYTICS_UNAVAILABLE", true, {
      tags: {
        phase: "runtime_readiness",
        reasonCode: readiness.code,
      },
    });
  }
}

export function resetDorisAnalyticsReadinessCacheForTest(): void {
  cached = undefined;
  inFlight = undefined;
}
