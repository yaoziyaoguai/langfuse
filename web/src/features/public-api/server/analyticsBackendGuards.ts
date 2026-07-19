import type { AnalyticsBackend } from "@langfuse/shared/analytics-backend";

export function legacyIngestionRejection(input: {
  rejectLegacyRoute: boolean;
  backend: AnalyticsBackend;
  clickhouseWriteMode: string;
}): "events_only" | null {
  if (!input.rejectLegacyRoute) return null;
  if (input.backend === "doris") return null;
  return input.clickhouseWriteMode === "events_only" ? "events_only" : null;
}
