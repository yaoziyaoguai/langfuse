import { context, propagation } from "@opentelemetry/api";

export const analyticsQuerySurfaces = [
  "trpc",
  "worker",
  "publicapi",
  "mcp",
] as const;

export type AnalyticsQuerySurface = (typeof analyticsQuerySurfaces)[number];

export type AnalyticsQueryTags = {
  surface?: AnalyticsQuerySurface | (string & {});
  route?: string;
  projectId?: string;
};

export const ANALYTICS_QUERY_TAG_BAGGAGE_KEYS = {
  surface: "langfuse.analytics.surface",
  route: "langfuse.analytics.route",
  projectId: "langfuse.project.id",
} as const;

const surfaceSet = new Set<string>(analyticsQuerySurfaces);

export function normalizeAnalyticsQueryTags(tags?: AnalyticsQueryTags) {
  const baggage = propagation.getBaggage(context.active());
  const surface =
    tags?.surface ??
    baggage?.getEntry(ANALYTICS_QUERY_TAG_BAGGAGE_KEYS.surface)?.value;
  const route =
    tags?.route ??
    baggage?.getEntry(ANALYTICS_QUERY_TAG_BAGGAGE_KEYS.route)?.value;
  const projectId =
    tags?.projectId ??
    baggage?.getEntry(ANALYTICS_QUERY_TAG_BAGGAGE_KEYS.projectId)?.value;
  const normalizedRoute = typeof route === "string" ? route.trim() : "";

  return {
    surface:
      typeof surface === "string" && surfaceSet.has(surface)
        ? (surface as AnalyticsQuerySurface)
        : ("unknown" as const),
    ...(normalizedRoute ? { route: normalizedRoute } : {}),
    ...(projectId ? { projectId } : {}),
  };
}
