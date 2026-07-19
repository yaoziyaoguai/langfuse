import { randomUUID } from "crypto";
import { IBackgroundMigration } from "./IBackgroundMigration";
import { prisma, Prisma } from "@langfuse/shared/src/db";
import {
  CLICKHOUSE_BACKGROUND_MIGRATION_SCRIPTS,
  heartbeatAnalyticsBackgroundMigrationManager,
  instrumentAsync,
  logger,
} from "@langfuse/shared/src/server";
import { env } from "../env";

const ENV_GATE_PREFIX = "LANGFUSE_BACKGROUND_MIGRATION_";
const HEARTBEAT_INTERVAL_MS = 15_000;
const CLICKHOUSE_MIGRATION_SCRIPTS = new Set<string>(
  CLICKHOUSE_BACKGROUND_MIGRATION_SCRIPTS,
);

export class BackgroundMigrationManager {
  private static workerId = randomUUID();
  private static activeMigration:
    | {
        id: string;
        name: string;
        script: string;
        args: Record<string, unknown>;
        migration: IBackgroundMigration;
      }
    | undefined;
  private static heartbeatTimer: NodeJS.Timeout | undefined;
  private static isClosed = false;
  private static retirementRequested = false;

  private static async recordHeartbeat(): Promise<boolean> {
    const active = BackgroundMigrationManager.activeMigration;
    if (active) {
      await prisma.backgroundMigration.updateMany({
        where: {
          id: active.id,
          workerId: BackgroundMigrationManager.workerId,
          finishedAt: null,
          failedAt: null,
        },
        data: { lockedAt: new Date() },
      });
    }
    const retirement = await heartbeatAnalyticsBackgroundMigrationManager({
      managerInstanceId: BackgroundMigrationManager.workerId,
      managerBuildId: env.BUILD_ID ?? "development",
      activeMigrationName: active?.script ?? null,
    });
    const shouldAbortActive =
      retirement.shouldDrain &&
      active !== undefined &&
      CLICKHOUSE_MIGRATION_SCRIPTS.has(active.script);
    if (shouldAbortActive && !BackgroundMigrationManager.retirementRequested) {
      BackgroundMigrationManager.retirementRequested = true;
      await active.migration.abort();
      logger.info(
        `[Background Migration] Retirement fence requested cooperative abort for ${active.name}`,
      );
    }
    return retirement.shouldDrain;
  }

  private static async heartBeat(): Promise<void> {
    try {
      await BackgroundMigrationManager.recordHeartbeat();
    } finally {
      if (!BackgroundMigrationManager.isClosed) {
        BackgroundMigrationManager.heartbeatTimer = setTimeout(
          BackgroundMigrationManager.heartBeat,
          HEARTBEAT_INTERVAL_MS,
        );
      }
    }
  }

  public static async run(): Promise<void> {
    BackgroundMigrationManager.isClosed = false;
    await BackgroundMigrationManager.heartBeat();
    await instrumentAsync({ name: "background-migration-run" }, async () => {
      // A migration row may declare `args.envGate = "<ENV_VAR>"` to remain dormant
      // until the operator sets that env var to "true" (e.g. ship dormant on v3,
      // activate on v4). We push the gate check into the findFirst predicate so
      // dormant rows never get picked — otherwise the first dormant row in name
      // order would head-of-line block every unrelated later migration.
      //
      // Gate names share the LANGFUSE_BACKGROUND_MIGRATION_ prefix so we can
      // discover them via the validated env without scanning all of process.env.
      const activeGates = Object.entries(env)
        .filter(
          ([key, value]) => key.startsWith(ENV_GATE_PREFIX) && value === "true",
        )
        .map(([key]) => key);

      let migrationToRun = true;

      while (migrationToRun) {
        const retirementActive =
          await BackgroundMigrationManager.recordHeartbeat();
        await prisma.$transaction(
          async (tx) => {
            // Read background migrations from database, ignoring any row whose
            // envGate is set to an env var that is not currently "true".
            const migration = await tx.backgroundMigration.findFirst({
              where: {
                finishedAt: null,
                failedAt: null,
                ...(retirementActive
                  ? {
                      script: {
                        notIn: [...CLICKHOUSE_BACKGROUND_MIGRATION_SCRIPTS],
                      },
                    }
                  : {}),
                OR: [
                  { args: { path: ["envGate"], equals: Prisma.AnyNull } },
                  ...activeGates.map((gate) => ({
                    args: { path: ["envGate"], equals: gate },
                  })),
                ],
              },
              orderBy: { name: "asc" },
            });

            // Abort if there is no migration to run or migration was locked less than 60s ago
            // We do not check lockedAt in the DB query, because findFirst might return other uncompleted migrations
            // which would lead to concurrent execution.
            if (
              !migration ||
              (migration.lockedAt &&
                migration.lockedAt > new Date(Date.now() - 60 * 1000))
            ) {
              logger.info(
                "[Background Migration] No background migrations to run",
              );
              migrationToRun = false;
              return;
            }

            logger.info(
              `[Background Migration] Found background migrations ${migration.name} to run`,
            );

            // Acquire lock
            await tx.backgroundMigration.update({
              where: {
                id: migration.id,
              },
              data: {
                workerId: BackgroundMigrationManager.workerId,
                lockedAt: new Date(),
              },
            });
            logger.info(
              `[Background Migration] Acquired lock for background migration ${migration.name}`,
            );
            BackgroundMigrationManager.activeMigration = {
              id: migration.id,
              name: migration.name,
              script: migration.script,
              args: migration.args as any,
              migration: new (require(`./${migration.script}`).default)(),
            };
          },
          {
            maxWait: 5000,
            isolationLevel: Prisma.TransactionIsolationLevel.Serializable,
          },
        );

        if (!BackgroundMigrationManager.activeMigration) {
          continue;
        }

        // Capture a local reference so the catch handler stays type-safe even if
        // close() concurrently nulls activeMigration during shutdown.
        const active = BackgroundMigrationManager.activeMigration;
        const { migration, args } = active;
        const { valid, invalidReason } = await migration.validate(args);
        if (!valid) {
          logger.error(
            `[Background Migration] Validation failed for background migration ${active.name}: ${invalidReason}`,
          );
          await prisma.backgroundMigration.update({
            where: {
              id: active.id,
              workerId: BackgroundMigrationManager.workerId,
            },
            data: {
              lockedAt: null,
              failedAt: new Date(),
              failedReason: invalidReason,
            },
          });
          BackgroundMigrationManager.activeMigration = undefined;
          continue;
        }

        try {
          await migration.run(args);

          if (
            BackgroundMigrationManager.activeMigration !== undefined &&
            !BackgroundMigrationManager.retirementRequested
          ) {
            // Only mark as complete if still active. Otherwise, it was aborted.
            await prisma.backgroundMigration.update({
              where: {
                id: active.id,
                workerId: BackgroundMigrationManager.workerId,
              },
              data: {
                finishedAt: new Date(),
                lockedAt: null,
              },
            });
            logger.info(
              `[Background Migration] Finished background migration ${active.name}`,
            );
          } else if (BackgroundMigrationManager.retirementRequested) {
            await prisma.backgroundMigration.updateMany({
              where: {
                id: active.id,
                workerId: BackgroundMigrationManager.workerId,
              },
              data: { lockedAt: null, workerId: null },
            });
          }
        } catch (err) {
          logger.error(
            `[Background Migration] Failed to run background migration ${active.name}: ${err}`,
          );
          if (BackgroundMigrationManager.retirementRequested) {
            await prisma.backgroundMigration.updateMany({
              where: {
                id: active.id,
                workerId: BackgroundMigrationManager.workerId,
              },
              data: { lockedAt: null, workerId: null },
            });
          } else {
            await prisma.backgroundMigration.update({
              where: {
                id: active.id,
                workerId: BackgroundMigrationManager.workerId,
              },
              data: {
                lockedAt: null,
                failedAt: new Date(),
                failedReason:
                  err instanceof Error ? err.message : "Unknown error",
              },
            });
          }
        }
        BackgroundMigrationManager.activeMigration = undefined;
        BackgroundMigrationManager.retirementRequested = false;
      }
      await BackgroundMigrationManager.recordHeartbeat();
    });
  }

  public static async close(): Promise<void> {
    BackgroundMigrationManager.isClosed = true;
    if (BackgroundMigrationManager.heartbeatTimer) {
      clearTimeout(BackgroundMigrationManager.heartbeatTimer);
      BackgroundMigrationManager.heartbeatTimer = undefined;
    }
    if (BackgroundMigrationManager.activeMigration) {
      await BackgroundMigrationManager.activeMigration.migration.abort();
      await prisma.backgroundMigration.update({
        where: {
          id: BackgroundMigrationManager.activeMigration.id,
          workerId: BackgroundMigrationManager.workerId,
        },
        data: {
          lockedAt: null,
        },
      });
      logger.info(
        `[Background Migration] Aborted active migration ${BackgroundMigrationManager.activeMigration.name}`,
      );
      BackgroundMigrationManager.activeMigration = undefined;
    }
  }
}
