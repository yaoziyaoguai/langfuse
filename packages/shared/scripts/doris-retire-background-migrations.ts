import {
  markAnalyticsBackgroundMigrationRetirementDrained,
  requestAnalyticsBackgroundMigrationRetirement,
} from "../src/server";
import { prisma } from "../src/db";

function required(name: string): string {
  const value = process.env[name];
  if (!value) throw new Error(`Missing required ${name}`);
  return value;
}

function positiveInteger(name: string, fallback: number): number {
  const value = Number(process.env[name] ?? fallback);
  if (!Number.isSafeInteger(value) || value <= 0) {
    throw new Error(`Invalid ${name}`);
  }
  return value;
}

async function main(): Promise<void> {
  const minimumManagerBuildId = required(
    "LANGFUSE_RETIREMENT_MINIMUM_MANAGER_BUILD_ID",
  );
  const timeoutMs = positiveInteger(
    "LANGFUSE_RETIREMENT_DRAIN_TIMEOUT_MS",
    10 * 60_000,
  );
  const pollMs = positiveInteger("LANGFUSE_RETIREMENT_POLL_INTERVAL_MS", 1_000);
  const fence = await requestAnalyticsBackgroundMigrationRetirement({
    minimumManagerBuildId,
  });
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (fence.status === "DRAINED" || fence.status === "TERMINALIZED") break;
    if (await markAnalyticsBackgroundMigrationRetirementDrained({})) break;
    await new Promise((resolve) => setTimeout(resolve, pollMs));
  }
  const current =
    await prisma.analyticsBackgroundMigrationRetirement.findUniqueOrThrow({
      where: {
        fenceName_managerInstanceId: {
          fenceName: fence.fenceName,
          managerInstanceId: "__fence__",
        },
      },
    });
  if (current.status !== "DRAINED" && current.status !== "TERMINALIZED") {
    throw new Error("Background migration retirement drain timed out");
  }
  process.stdout.write(
    `${JSON.stringify({
      fenceName: current.fenceName,
      generation: current.generation.toString(),
      status: current.status,
      minimumManagerBuildId: current.minimumManagerBuildId,
      drainedAt: current.drainedAt?.toISOString() ?? null,
    })}\n`,
  );
}

main()
  .catch((error) => {
    process.stderr.write(
      `${error instanceof Error ? error.message : "Retirement failed"}\n`,
    );
    process.exitCode = 1;
  })
  .finally(async () => prisma.$disconnect());
