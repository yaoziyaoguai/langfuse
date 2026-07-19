import type { AnalyticsEntityType, Prisma, PrismaClient } from "@prisma/client";

import { InvalidRequestError } from "../../../../errors";

type EntityHeadLocatorClient = PrismaClient | Prisma.TransactionClient;

export type EventHeadLocator = {
  readonly partitionDate: string;
  readonly traceId: string;
  readonly observationId: string;
};

export type ScoreHeadLocator = {
  readonly partitionDate: string;
  readonly scoreId: string;
};

function requireId(value: string, label: string): void {
  if (!value.trim()) {
    throw new InvalidRequestError(`${label} is required`);
  }
}

function dateOnly(value: Date): string {
  return value.toISOString().slice(0, 10);
}

async function findEventHeadLocators(input: {
  readonly client: EntityHeadLocatorClient;
  readonly projectId: string;
  readonly where:
    | { readonly lookupId: string; readonly owningTraceId?: string }
    | { readonly owningTraceId: string };
}): Promise<readonly EventHeadLocator[]> {
  const rows = await input.client.analyticsEntityHead.findMany({
    where: {
      projectId: input.projectId,
      entityType: "EVENT" satisfies AnalyticsEntityType,
      ...input.where,
    },
    select: {
      partitionDate: true,
      owningTraceId: true,
      lookupId: true,
    },
    orderBy: [{ partitionDate: "desc" }, { entityKey: "asc" }],
  });

  return rows.map((row) => {
    if (!row.owningTraceId || !row.lookupId) {
      throw new Error("Analytics entity head is missing its event locator");
    }
    return {
      partitionDate: dateOnly(row.partitionDate),
      traceId: row.owningTraceId,
      observationId: row.lookupId,
    };
  });
}

export async function findObservationHeadLocators(input: {
  readonly client?: EntityHeadLocatorClient;
  readonly projectId: string;
  readonly observationId: string;
  readonly traceId?: string;
}): Promise<readonly EventHeadLocator[]> {
  requireId(input.projectId, "projectId");
  requireId(input.observationId, "observationId");
  const client = input.client ?? (await import("../../../../db.js")).prisma;
  return findEventHeadLocators({
    client,
    projectId: input.projectId,
    where: {
      lookupId: input.observationId,
      ...(input.traceId && { owningTraceId: input.traceId }),
    },
  });
}

export async function findTraceEventHeadLocators(input: {
  readonly client?: EntityHeadLocatorClient;
  readonly projectId: string;
  readonly traceId: string;
}): Promise<readonly EventHeadLocator[]> {
  requireId(input.projectId, "projectId");
  requireId(input.traceId, "traceId");
  const client = input.client ?? (await import("../../../../db.js")).prisma;
  return findEventHeadLocators({
    client,
    projectId: input.projectId,
    where: { owningTraceId: input.traceId },
  });
}

export async function findScoreHeadLocators(input: {
  readonly client?: EntityHeadLocatorClient;
  readonly projectId: string;
  readonly scoreId: string;
}): Promise<readonly ScoreHeadLocator[]> {
  requireId(input.projectId, "projectId");
  requireId(input.scoreId, "scoreId");
  const client = input.client ?? (await import("../../../../db.js")).prisma;
  const rows = await client.analyticsEntityHead.findMany({
    where: {
      projectId: input.projectId,
      entityType: "SCORE" satisfies AnalyticsEntityType,
      lookupId: input.scoreId,
    },
    select: {
      partitionDate: true,
      lookupId: true,
    },
    orderBy: [{ partitionDate: "desc" }, { entityKey: "asc" }],
  });

  return rows.map((row) => {
    if (!row.lookupId) {
      throw new Error("Analytics entity head is missing its score locator");
    }
    return {
      partitionDate: dateOnly(row.partitionDate),
      scoreId: row.lookupId,
    };
  });
}
