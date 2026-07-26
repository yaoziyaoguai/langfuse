import { createHash } from "node:crypto";

import type { AnalyticsBackend } from "./analyticsBackend";

export type AnalyticsBackendEmptinessProbeResult = {
  readonly empty: boolean;
  readonly evidenceDigest: string;
};

/**
 * The digest is an audit fence, not a diagnostic payload. Callers provide a
 * deterministic, credential-free proof document and expose only its hash.
 */
export function createAnalyticsBackendEmptinessEvidence(
  backend: AnalyticsBackend,
  empty: boolean,
  proof: Readonly<Record<string, unknown>>,
): AnalyticsBackendEmptinessProbeResult {
  const evidenceDigest = createHash("sha256")
    .update(
      JSON.stringify({
        contract: "langfuse-analytics-backend-emptiness-v1",
        backend,
        empty,
        proof,
      }),
    )
    .digest("hex");

  return { empty, evidenceDigest };
}
