import { getDorisTelemetryRepositories } from "./telemetry/doris/runtime";

export type EnvironmentFilterProps = {
  projectId: string;
  fromTimestamp?: Date;
};

export const getEnvironmentsForProject = async (
  props: EnvironmentFilterProps,
): Promise<{ environment: string }[]> => {
  const { projectId, fromTimestamp } = props;

  const to = new Date();
  const from = fromTimestamp ?? new Date(0);
  const rows =
    await getDorisTelemetryRepositories().observations.filterOptionValues({
      projectId,
      range: { from, to },
      filters: [],
      column: "environment",
      limit: 1_000,
    });
  return [...new Set([...rows.map(({ value }) => value), "default"])].map(
    (environment) => ({ environment }),
  );
};
