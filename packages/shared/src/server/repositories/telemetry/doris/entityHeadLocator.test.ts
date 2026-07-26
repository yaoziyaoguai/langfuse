import type { PrismaClient } from "@prisma/client";
import { describe, expect, it, vi } from "vitest";

import {
  findObservationHeadLocators,
  findScoreHeadLocators,
  findTraceEventHeadLocators,
  findTraceEventHeadLocatorsByIds,
} from "./entityHeadLocator";

describe("Doris entity-head locators", () => {
  it("finds only project-scoped observation partitions and preserves collisions", async () => {
    const findMany = vi.fn().mockResolvedValue([
      {
        partitionDate: new Date("2026-07-18T00:00:00.000Z"),
        owningTraceId: "trace-b",
        lookupId: "shared-span",
      },
      {
        partitionDate: new Date("2026-07-17T00:00:00.000Z"),
        owningTraceId: "trace-a",
        lookupId: "shared-span",
      },
    ]);
    const client = {
      analyticsEntityHead: { findMany },
    } as unknown as PrismaClient;

    await expect(
      findObservationHeadLocators({
        client,
        projectId: "project-1",
        observationId: "shared-span",
      }),
    ).resolves.toEqual([
      {
        partitionDate: "2026-07-18",
        traceId: "trace-b",
        observationId: "shared-span",
      },
      {
        partitionDate: "2026-07-17",
        traceId: "trace-a",
        observationId: "shared-span",
      },
    ]);
    expect(findMany).toHaveBeenCalledWith({
      where: {
        projectId: "project-1",
        entityType: "EVENT",
        lookupId: "shared-span",
      },
      select: {
        partitionDate: true,
        owningTraceId: true,
        lookupId: true,
      },
      orderBy: [{ partitionDate: "desc" }, { entityKey: "asc" }],
    });
  });

  it("enumerates a trace through its indexed owning-trace locator", async () => {
    const findMany = vi.fn().mockResolvedValue([
      {
        partitionDate: new Date("2026-07-17T00:00:00.000Z"),
        owningTraceId: "trace-1",
        lookupId: "span-1",
      },
    ]);
    const client = {
      analyticsEntityHead: { findMany },
    } as unknown as PrismaClient;

    await expect(
      findTraceEventHeadLocators({
        client,
        projectId: "project-1",
        traceId: "trace-1",
      }),
    ).resolves.toEqual([
      {
        partitionDate: "2026-07-17",
        traceId: "trace-1",
        observationId: "span-1",
      },
    ]);
    expect(findMany).toHaveBeenCalledWith(
      expect.objectContaining({
        where: {
          projectId: "project-1",
          entityType: "EVENT",
          owningTraceId: "trace-1",
        },
      }),
    );
  });

  it("enumerates multiple traces with one project-scoped locator lookup", async () => {
    const findMany = vi.fn().mockResolvedValue([
      {
        partitionDate: new Date("2026-07-18T00:00:00.000Z"),
        owningTraceId: "trace-2",
        lookupId: "span-2",
      },
      {
        partitionDate: new Date("2026-07-17T00:00:00.000Z"),
        owningTraceId: "trace-1",
        lookupId: "span-1",
      },
    ]);
    const client = {
      analyticsEntityHead: { findMany },
    } as unknown as PrismaClient;

    await expect(
      findTraceEventHeadLocatorsByIds({
        client,
        projectId: "project-1",
        traceIds: ["trace-1", "trace-2", "trace-1"],
      }),
    ).resolves.toHaveLength(2);
    expect(findMany).toHaveBeenCalledWith(
      expect.objectContaining({
        where: {
          projectId: "project-1",
          entityType: "EVENT",
          owningTraceId: { in: ["trace-1", "trace-2"] },
        },
      }),
    );
  });
});

describe("findScoreHeadLocators", () => {
  it("finds the immutable score partition by project and lookup ID", async () => {
    const findMany = vi.fn().mockResolvedValue([
      {
        partitionDate: new Date("2026-07-17T00:00:00.000Z"),
        lookupId: "score-1",
      },
    ]);
    const client = {
      analyticsEntityHead: { findMany },
    } as unknown as Parameters<typeof findScoreHeadLocators>[0]["client"];

    await expect(
      findScoreHeadLocators({
        client,
        projectId: "project-1",
        scoreId: "score-1",
      }),
    ).resolves.toEqual([{ partitionDate: "2026-07-17", scoreId: "score-1" }]);
    expect(findMany).toHaveBeenCalledWith(
      expect.objectContaining({
        where: expect.objectContaining({
          projectId: "project-1",
          entityType: "SCORE",
          lookupId: "score-1",
        }),
      }),
    );
  });
});
