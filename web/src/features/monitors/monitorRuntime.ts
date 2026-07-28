type MonitorRuntimeEnvironment = {
  readonly analyticsBackend: "clickhouse" | "doris" | undefined;
  readonly v4WriteMode: "legacy" | "dual" | "events_only" | undefined;
};

export function isMonitorRuntimeAvailable(
  environment: MonitorRuntimeEnvironment,
): boolean {
  if (environment.analyticsBackend === "doris") return true;
  if (environment.analyticsBackend !== "clickhouse") return false;
  return (
    environment.v4WriteMode === "dual" ||
    environment.v4WriteMode === "events_only"
  );
}
