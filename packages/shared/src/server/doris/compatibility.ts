import type { PrismaClient } from "../../db";

import type {
  AnalyticsCompatibilityControlState,
  AnalyticsCompatibilityQuery,
} from "./readiness";

/** Reads the durable receipt horizon that constrains contract migration. */
export class PrismaAnalyticsCompatibilityControlState implements AnalyticsCompatibilityControlState {
  constructor(
    private readonly prisma: Pick<PrismaClient, "analyticsIngestionOperation">,
  ) {}

  countIncompatibleRecoverableOperations(
    query: AnalyticsCompatibilityQuery,
  ): Promise<number> {
    return this.prisma.analyticsIngestionOperation.count({
      where: {
        recoverableUntil: { gt: query.now },
        OR: [
          {
            canonicalizerVersion: {
              notIn: [...query.supportedCanonicalizerVersions],
            },
          },
          { schemaVersion: { notIn: [...query.supportedSchemaVersions] } },
        ],
      },
    });
  }
}
