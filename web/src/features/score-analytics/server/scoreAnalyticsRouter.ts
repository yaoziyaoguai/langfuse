import { z } from "zod";

import {
  createTRPCRouter,
  protectedProjectProcedure,
} from "@/src/server/api/trpc";
import { getScoresGroupedByNameSourceType } from "@langfuse/shared/src/server";
import {
  estimateDorisScoreComparison,
  getDorisScoreComparisonAnalytics,
} from "./dorisScoreAnalytics";

const scoreIdentifier = z.object({
  name: z.string(),
  dataType: z.string(),
  source: z.string(),
});

const objectType = z
  .enum(["all", "trace", "session", "observation", "dataset_run"])
  .default("all");

const interval = z
  .object({
    count: z.number().int().positive(),
    unit: z.enum(["second", "minute", "hour", "day", "month", "year"]),
  })
  .refine(
    ({ count, unit }) => {
      const allowed: Record<string, readonly number[]> = {
        second: [1, 5, 10, 30],
        minute: [1, 5, 10, 30],
        hour: [1, 3, 6, 12],
        day: [1, 2, 5, 7, 14],
        month: [1, 3, 6],
        year: [1],
      };
      return allowed[unit]?.includes(count) ?? false;
    },
    { message: "Invalid score analytics interval" },
  )
  .default({ count: 1, unit: "day" });

export const scoreAnalyticsRouter = createTRPCRouter({
  getScoreIdentifiers: protectedProjectProcedure
    .input(z.object({ projectId: z.string() }))
    .query(async ({ input }) => {
      const groupedScores = await getScoresGroupedByNameSourceType({
        projectId: input.projectId,
        filter: [],
      });
      return {
        scores: groupedScores.map(({ name, source, dataType }) => ({
          value: `${name}-${dataType}-${source}`,
          name,
          dataType,
          source,
        })),
      };
    }),

  estimateScoreComparisonSize: protectedProjectProcedure
    .input(
      z.object({
        projectId: z.string(),
        score1: scoreIdentifier,
        score2: scoreIdentifier,
        fromTimestamp: z.date(),
        toTimestamp: z.date(),
        objectType,
        mode: z.enum(["single", "two"]).optional(),
      }),
    )
    .query(async ({ input }) => {
      const counts = await estimateDorisScoreComparison(input);
      const largest = Math.max(counts.score1Count, counts.score2Count);
      return {
        ...counts,
        estimatedMatchedCount: counts.matchedCount,
        willSample: largest > 100_000,
        willSkipFinal: false,
        estimatedQueryTime:
          counts.matchedCount > 1_000_000
            ? "30-60s"
            : counts.matchedCount > 500_000
              ? "15-30s"
              : counts.matchedCount > 100_000
                ? "10-20s"
                : "<10s",
        mode: input.mode ?? "two",
      };
    }),

  getScoreComparisonAnalytics: protectedProjectProcedure
    .input(
      z.object({
        projectId: z.string(),
        score1: scoreIdentifier,
        score2: scoreIdentifier,
        mode: z.enum(["single", "two"]).optional(),
        fromTimestamp: z.date(),
        toTimestamp: z.date(),
        interval,
        nBins: z.number().int().min(5).max(50).default(10),
        objectType,
        estimateResults: z
          .object({
            score1Count: z.number(),
            score2Count: z.number(),
            estimatedMatchedCount: z.number(),
          })
          .optional(),
      }),
    )
    .query(({ input }) =>
      getDorisScoreComparisonAnalytics({
        projectId: input.projectId,
        score1: input.score1,
        score2: input.score2,
        fromTimestamp: input.fromTimestamp,
        toTimestamp: input.toTimestamp,
        interval: input.interval,
        nBins: input.nBins,
        objectType: input.objectType,
        mode: input.mode ?? "two",
      }),
    ),
});
