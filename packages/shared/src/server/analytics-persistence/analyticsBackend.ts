export const ANALYTICS_BACKENDS = ["clickhouse", "doris"] as const;

export type AnalyticsBackend = (typeof ANALYTICS_BACKENDS)[number];

/**
 * 解析部署级 analytics backend。该选择在进程启动时固定，不支持双写或热切换。
 */
export function resolveAnalyticsBackend(
  configured: string | undefined,
): AnalyticsBackend {
  const backend = configured ?? "clickhouse";
  if (backend === "clickhouse" || backend === "doris") return backend;

  throw new Error(
    `LANGFUSE_ANALYTICS_BACKEND must be one of: ${ANALYTICS_BACKENDS.join(
      ", ",
    )}`,
  );
}

export function isAnalyticsBackend(
  configured: string | undefined,
  expected: AnalyticsBackend,
): boolean {
  return resolveAnalyticsBackend(configured) === expected;
}
