import { PrismaClient } from "@prisma/client";
import { afterAll, beforeAll, describe, expect, it } from "vitest";

const controlDatabaseUrl = process.env.DORIS_CONTROL_TEST_DATABASE_URL;

describe.skipIf(!controlDatabaseUrl)(
  "analytics background migration retirement",
  () => {
    let repository: typeof import("./analyticsBackgroundMigrationRetirement.js");
    const prisma = new PrismaClient({ datasourceUrl: controlDatabaseUrl });
    const suffix = `${Date.now()}-${Math.random().toString(16).slice(2)}`;
    const fenceName = `background-retirement-${suffix}`;

    beforeAll(async () => {
      repository = await import("./analyticsBackgroundMigrationRetirement.js");
      await prisma.analyticsBackgroundMigrationRetirement.create({
        data: {
          id: `background-retirement-control-${suffix}`,
          fenceName,
          managerInstanceId: repository.BACKGROUND_MIGRATION_FENCE_INSTANCE,
          generation: 1n,
          status: "OBSERVING",
          minimumManagerBuildId: "release-a",
          managerBuildId: "control",
          managerHeartbeatAt: new Date(0),
        },
      });
    });

    afterAll(async () => {
      await prisma.analyticsBackgroundMigrationRetirement.deleteMany({
        where: { fenceName },
      });
      await prisma.$disconnect();
    });

    it("requires Release-A heartbeats and drains an active script cooperatively", async () => {
      const now = new Date("2026-07-18T12:00:00.000Z");
      await expect(
        repository.requestAnalyticsBackgroundMigrationRetirement({
          client: prisma,
          fenceName,
          minimumManagerBuildId: "release-a",
          now,
        }),
      ).rejects.toThrow("No Release-A");

      await expect(
        repository.heartbeatAnalyticsBackgroundMigrationManager({
          client: prisma,
          fenceName,
          managerInstanceId: "manager-a",
          managerBuildId: "release-a",
          activeMigrationName: null,
          now,
        }),
      ).resolves.toMatchObject({
        shouldDrain: false,
        fenceStatus: "OBSERVING",
      });

      await expect(
        repository.requestAnalyticsBackgroundMigrationRetirement({
          client: prisma,
          fenceName,
          minimumManagerBuildId: "release-bad",
          now: new Date(now.getTime() + 1_000),
        }),
      ).rejects.toThrow("pre-Release-A");

      const fence =
        await repository.requestAnalyticsBackgroundMigrationRetirement({
          client: prisma,
          fenceName,
          minimumManagerBuildId: "release-a",
          now: new Date(now.getTime() + 2_000),
        });
      expect(fence).toMatchObject({ status: "DRAINING", generation: 2n });

      await expect(
        repository.heartbeatAnalyticsBackgroundMigrationManager({
          client: prisma,
          fenceName,
          managerInstanceId: "manager-a",
          managerBuildId: "release-a",
          activeMigrationName: "migrateTracesFromPostgresToClickhouse",
          now: new Date(now.getTime() + 3_000),
        }),
      ).resolves.toMatchObject({ shouldDrain: true });
      await expect(
        repository.markAnalyticsBackgroundMigrationRetirementDrained({
          client: prisma,
          fenceName,
          now: new Date(now.getTime() + 4_000),
        }),
      ).resolves.toBe(false);

      await repository.heartbeatAnalyticsBackgroundMigrationManager({
        client: prisma,
        fenceName,
        managerInstanceId: "manager-a",
        managerBuildId: "release-a",
        activeMigrationName: null,
        now: new Date(now.getTime() + 5_000),
      });
      await expect(
        repository.markAnalyticsBackgroundMigrationRetirementDrained({
          client: prisma,
          fenceName,
          now: new Date(now.getTime() + 6_000),
        }),
      ).resolves.toBe(true);
    });
  },
);
