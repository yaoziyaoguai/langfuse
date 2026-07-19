import type { AnalyticsBackend } from "@langfuse/shared/analytics-backend";

export function legacyIngestionRejection(input: {
  rejectLegacyRoute: boolean;
  backend: AnalyticsBackend;
  clickhouseWriteMode: string;
}): "doris" | "events_only" | null {
  if (!input.rejectLegacyRoute) return null;
  if (input.backend === "doris") return "doris";
  return input.clickhouseWriteMode === "events_only" ? "events_only" : null;
}
