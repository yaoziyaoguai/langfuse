import type { AnalyticsIntegrationExportSource } from "@langfuse/shared";

/**
 * Legacy-source write-mode guard for the export workers (blob storage,
 * PostHog, Mixpanel) — thin adapter over validateExportSource; policy and
 * rationale in export-source-policy.ts. Throws BEFORE any export work so each
 * handler's normal failure path (log + rethrow, BullMQ retry, and for blob
 * storage lastError persistence + admin notification) takes over instead of
 * silently exporting stale/empty data while sync state advances (LFE-10148).
 *
 * The env read lives here in worker code. `remediation` is the
 * operator-actionable closing sentence, per integration.
 */
export function assertLegacyExportSourceWritable(
  _exportSource: AnalyticsIntegrationExportSource,
  remediation: string,
): void {
  throw new Error(
    `Analytics integrations are unavailable in the Doris R1A release. ${remediation}`,
  );
}
