import type {
  AnalyticsBackgroundMigrationRetirement,
  PrismaClient,
} from "@prisma/client";

import { prisma } from "../../db";

export const CLICKHOUSE_BACKGROUND_MIGRATION_FENCE =
  "clickhouse-background-migrations";
export const BACKGROUND_MIGRATION_FENCE_INSTANCE = "__fence__";
export const BACKGROUND_MIGRATION_HEARTBEAT_TTL_MS = 60_000;
export const CLICKHOUSE_BACKGROUND_MIGRATION_SCRIPTS = [
  "migrateTracesFromPostgresToClickhouse",
  "migrateObservationsFromPostgresToClickhouse",
  "migrateScoresFromPostgresToClickhouse",
  "migrateDatasetRunItemsFromPostgresToClickhouse",
  "migrateDatasetRunItemsFromPostgresToClickhouseRmt",
  "backfillEventsFullFromObservations",
  "backfillEventsFullFromDatasetRunItems",
  "createRootSpansFromTraces",
  "rewriteObservationsToPidTidSorting",
  "dropPidTidSortingTables",
] as const;

export async function heartbeatAnalyticsBackgroundMigrationManager(input: {
  readonly client?: PrismaClient;
  readonly fenceName?: string;
  readonly managerInstanceId: string;
  readonly managerBuildId: string;
  readonly activeMigrationName: string | null;
  readonly activeLeaseMs?: number;
  readonly now?: Date;
}): Promise<{
  readonly shouldDrain: boolean;
  readonly fenceStatus: "OBSERVING" | "DRAINING" | "DRAINED" | "TERMINALIZED";
}> {
  if (!input.managerInstanceId || !input.managerBuildId) {
    throw new TypeError("Invalid background migration manager heartbeat");
  }
  const client = input.client ?? prisma;
  const fenceName = input.fenceName ?? CLICKHOUSE_BACKGROUND_MIGRATION_FENCE;
  const now = input.now ?? new Date();
  const activeLeaseMs = input.activeLeaseMs ?? 30_000;
  return client.$transaction(async (transaction) => {
    const fence =
      await transaction.analyticsBackgroundMigrationRetirement.findUniqueOrThrow(
        {
          where: {
            fenceName_managerInstanceId: {
              fenceName,
              managerInstanceId: BACKGROUND_MIGRATION_FENCE_INSTANCE,
            },
          },
        },
      );
    const shouldDrain = fence.status !== "OBSERVING";
    const status = shouldDrain
      ? input.activeMigrationName
        ? "DRAINING"
        : "DRAINED"
      : "OBSERVING";
    await transaction.analyticsBackgroundMigrationRetirement.upsert({
      where: {
        fenceName_managerInstanceId: {
          fenceName,
          managerInstanceId: input.managerInstanceId,
        },
      },
      create: {
        fenceName,
        managerInstanceId: input.managerInstanceId,
        generation: fence.generation,
        status,
        minimumManagerBuildId: fence.minimumManagerBuildId,
        managerBuildId: input.managerBuildId,
        managerHeartbeatAt: now,
        activeMigrationName: input.activeMigrationName,
        activeLeaseExpiresAt: input.activeMigrationName
          ? new Date(now.getTime() + activeLeaseMs)
          : null,
        drainedAt: status === "DRAINED" ? now : null,
      },
      update: {
        generation: fence.generation,
        status,
        minimumManagerBuildId: fence.minimumManagerBuildId,
        managerBuildId: input.managerBuildId,
        managerHeartbeatAt: now,
        activeMigrationName: input.activeMigrationName,
        activeLeaseExpiresAt: input.activeMigrationName
          ? new Date(now.getTime() + activeLeaseMs)
          : null,
        drainedAt: status === "DRAINED" ? now : null,
      },
    });
    return { shouldDrain, fenceStatus: fence.status };
  });
}

export async function requestAnalyticsBackgroundMigrationRetirement(input: {
  readonly client?: PrismaClient;
  readonly fenceName?: string;
  readonly minimumManagerBuildId: string;
  readonly now?: Date;
}): Promise<AnalyticsBackgroundMigrationRetirement> {
  if (!input.minimumManagerBuildId) {
    throw new TypeError("A minimum manager build ID is required");
  }
  const client = input.client ?? prisma;
  const fenceName = input.fenceName ?? CLICKHOUSE_BACKGROUND_MIGRATION_FENCE;
  const now = input.now ?? new Date();
  const heartbeatCutoff = new Date(
    now.getTime() - BACKGROUND_MIGRATION_HEARTBEAT_TTL_MS,
  );
  return client.$transaction(async (transaction) => {
    const current =
      await transaction.analyticsBackgroundMigrationRetirement.findUniqueOrThrow(
        {
          where: {
            fenceName_managerInstanceId: {
              fenceName,
              managerInstanceId: BACKGROUND_MIGRATION_FENCE_INSTANCE,
            },
          },
        },
      );
    if (current.status !== "OBSERVING") {
      if (current.minimumManagerBuildId !== input.minimumManagerBuildId) {
        throw new Error("Background migration retirement build ID conflicts");
      }
      return current;
    }
    const managers =
      await transaction.analyticsBackgroundMigrationRetirement.findMany({
        where: {
          fenceName,
          managerInstanceId: {
            not: BACKGROUND_MIGRATION_FENCE_INSTANCE,
          },
          managerHeartbeatAt: { gt: heartbeatCutoff },
        },
      });
    if (managers.length === 0) {
      throw new Error("No Release-A background migration manager heartbeat");
    }
    const oldBuilds = managers.filter(
      ({ managerBuildId }) => managerBuildId !== input.minimumManagerBuildId,
    );
    if (oldBuilds.length > 0) {
      throw new Error("A pre-Release-A background migration manager is active");
    }
    return transaction.analyticsBackgroundMigrationRetirement.update({
      where: {
        fenceName_managerInstanceId: {
          fenceName,
          managerInstanceId: BACKGROUND_MIGRATION_FENCE_INSTANCE,
        },
      },
      data: {
        generation: { increment: 1 },
        status: "DRAINING",
        minimumManagerBuildId: input.minimumManagerBuildId,
        managerHeartbeatAt: now,
        activeMigrationName: null,
        activeLeaseExpiresAt: null,
        drainedAt: null,
      },
    });
  });
}

export async function markAnalyticsBackgroundMigrationRetirementDrained(input: {
  readonly client?: PrismaClient;
  readonly fenceName?: string;
  readonly now?: Date;
}): Promise<boolean> {
  const client = input.client ?? prisma;
  const fenceName = input.fenceName ?? CLICKHOUSE_BACKGROUND_MIGRATION_FENCE;
  const now = input.now ?? new Date();
  const heartbeatCutoff = new Date(
    now.getTime() - BACKGROUND_MIGRATION_HEARTBEAT_TTL_MS,
  );
  return client.$transaction(async (transaction) => {
    const blocking =
      await transaction.analyticsBackgroundMigrationRetirement.count({
        where: {
          fenceName,
          managerInstanceId: {
            not: BACKGROUND_MIGRATION_FENCE_INSTANCE,
          },
          managerHeartbeatAt: { gt: heartbeatCutoff },
          OR: [
            { status: { not: "DRAINED" } },
            { activeMigrationName: { not: null } },
            { activeLeaseExpiresAt: { gt: now } },
          ],
        },
      });
    if (blocking > 0) return false;
    const updated =
      await transaction.analyticsBackgroundMigrationRetirement.updateMany({
        where: {
          fenceName,
          managerInstanceId: BACKGROUND_MIGRATION_FENCE_INSTANCE,
          status: "DRAINING",
        },
        data: { status: "DRAINED", drainedAt: now },
      });
    return updated.count === 1;
  });
}
