import { readFile } from "node:fs/promises";

import { prisma } from "../src/db";
import { probeAnalyticsBackendSwitchEmptiness } from "../src/server/analytics-persistence/analyticsBackendEmptiness";
import {
  digestAnalyticsRuntimeInventory,
  fingerprintAnalyticsWorkloadEpoch,
  switchAnalyticsBackend,
} from "../src/server/repositories/analyticsBackendDeployment";
import { createAnalyticsScoreDeletionDrainProbe } from "../src/server/redis/analyticsScoreDeletionDrain";
import {
  executeSwitchBackendOperator,
  formatAnalyticsBackendOperatorFailure,
  formatAnalyticsBackendOperatorSuccess,
} from "./analytics-backend-operator";

const scoreDeletionQueueDrain = createAnalyticsScoreDeletionDrainProbe();

async function main(): Promise<void> {
  const state = await executeSwitchBackendOperator({
    argv: process.argv.slice(2),
    env: process.env,
    dependencies: {
      readTextFile: (path) => readFile(path, "utf8"),
      fingerprintWorkloadEpoch: fingerprintAnalyticsWorkloadEpoch,
      digestRuntimeInventory: digestAnalyticsRuntimeInventory,
      probeBackendEmptiness: probeAnalyticsBackendSwitchEmptiness,
      verifyScoreDeletionQueuesEmpty: scoreDeletionQueueDrain.verify,
      switchBackend: switchAnalyticsBackend,
    },
  });
  process.stdout.write(
    formatAnalyticsBackendOperatorSuccess("switch-backend", state),
  );
}

main()
  .catch((error: unknown) => {
    process.stderr.write(formatAnalyticsBackendOperatorFailure(error));
    process.exitCode = 1;
  })
  .finally(async () => {
    await Promise.allSettled([
      scoreDeletionQueueDrain.close(),
      prisma.$disconnect(),
    ]);
  });
