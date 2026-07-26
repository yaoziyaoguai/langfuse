import { readFile } from "node:fs/promises";

import { prisma } from "../src/db";
import {
  adoptExistingAnalyticsBackend,
  digestAnalyticsRuntimeInventory,
  fingerprintAnalyticsWorkloadEpoch,
} from "../src/server/repositories/analyticsBackendDeployment";
import { createAnalyticsScoreDeletionDrainProbe } from "../src/server/redis/analyticsScoreDeletionDrain";
import {
  executeAdoptExistingBackendOperator,
  formatAnalyticsBackendOperatorFailure,
  formatAnalyticsBackendOperatorSuccess,
} from "./analytics-backend-operator";

const analyticsQueueDrain = createAnalyticsScoreDeletionDrainProbe();

async function main(): Promise<void> {
  const state = await executeAdoptExistingBackendOperator({
    argv: process.argv.slice(2),
    env: process.env,
    dependencies: {
      readTextFile: (path) => readFile(path, "utf8"),
      fingerprintWorkloadEpoch: fingerprintAnalyticsWorkloadEpoch,
      digestRuntimeInventory: digestAnalyticsRuntimeInventory,
      verifyScoreDeletionQueuesEmpty: analyticsQueueDrain.verify,
      adopt: adoptExistingAnalyticsBackend,
    },
  });
  process.stdout.write(
    formatAnalyticsBackendOperatorSuccess("adopt-existing-backend", state),
  );
}

main()
  .catch((error: unknown) => {
    process.stderr.write(formatAnalyticsBackendOperatorFailure(error));
    process.exitCode = 1;
  })
  .finally(async () => {
    await Promise.allSettled([
      analyticsQueueDrain.close(),
      prisma.$disconnect(),
    ]);
  });
