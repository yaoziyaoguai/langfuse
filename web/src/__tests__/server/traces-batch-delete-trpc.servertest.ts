const mockAddBatchAction = vi.fn();
const mockGetBatchActionJobState = vi.fn();

vi.mock("@langfuse/shared/src/server", async () => {
  const originalModule = await vi.importActual("@langfuse/shared/src/server");
  return {
    ...originalModule,
    BatchActionQueue: {
      getInstance: vi.fn(() => ({
        add: mockAddBatchAction,
        getJobState: mockGetBatchActionJobState,
      })),
    },
  };
});

import type { Session } from "next-auth";
import { randomUUID } from "crypto";
import { beforeEach, describe, expect, it } from "vitest";
import { prisma } from "@langfuse/shared/src/db";
import { appRouter } from "@/src/server/api/root";
import { createInnerTRPCContext } from "@/src/server/api/trpc";
import {
  ActionId,
  BatchExportTableName,
  BatchActionStatus,
  createTraceDeleteBatchActionConfig,
  TraceDeleteBatchActionConfigSchema,
} from "@langfuse/shared";
import { createOrgProjectAndApiKey } from "@langfuse/shared/src/server";

const traceDeleteQuery = (userId: string) => ({
  filter: [
    {
      column: "userId",
      operator: "=" as const,
      value: userId,
      type: "string" as const,
    },
  ],
  orderBy: { column: "timestamp", order: "DESC" as const },
});

const createCaller = async (opts: { v4BetaEnabled?: boolean } = {}) => {
  const { project, org } = await createOrgProjectAndApiKey({ plan: "Team" });
  const session: Session = {
    expires: "1",
    user: {
      id: `user-${randomUUID()}`,
      name: "Batch Delete Test User",
      canCreateOrganizations: true,
      admin: true,
      v4BetaEnabled: opts.v4BetaEnabled ?? false,
      organizations: [
        {
          id: org.id,
          name: org.name,
          role: "OWNER",
          plan: "cloud:team",
          cloudConfig: undefined,
          metadata: {},
          aiFeaturesEnabled: false,
          aiTelemetryEnabled: false,
          projects: [
            {
              id: project.id,
              role: "ADMIN",
              retentionDays: 30,
              deletedAt: null,
              name: project.name,
              hasTraces: true,
              metadata: {},
              createdAt: new Date().toISOString(),
            },
          ],
        },
      ],
      featureFlags: {
        templateFlag: true,
        searchBar: false,
        v4BetaToggleVisible: false,
        observationEvals: false,
        experimentsV4Enabled: false,
      },
    },
    environment: {} as any,
  };

  const ctx = createInnerTRPCContext({ session, headers: {} });
  return {
    projectId: project.id,
    session,
    caller: appRouter.createCaller({ ...ctx, prisma }),
  };
};

describe("traces.deleteMany batch action", () => {
  beforeEach(() => {
    mockAddBatchAction.mockClear();
    mockGetBatchActionJobState.mockReset();
    mockGetBatchActionJobState.mockResolvedValue("unknown");
  });

  it("creates the trace-delete BatchAction row without a queue payload", async () => {
    const { projectId, session, caller } = await createCaller();
    const userId = `delete-user-${randomUUID()}`;
    const query = traceDeleteQuery(userId);
    const batchActionId = `${projectId}-traces-trace-delete`;

    await caller.traces.deleteMany({
      projectId,
      traceIds: [randomUUID()],
      isBatchAction: true,
      query,
    });

    const batchAction = await prisma.batchAction.findUniqueOrThrow({
      where: { id: batchActionId },
    });
    expect(batchAction).toMatchObject({
      projectId,
      userId: session.user!.id,
      actionType: "trace-delete",
      tableName: "traces",
      status: BatchActionStatus.Queued,
      totalCount: null,
      processedCount: 0,
      failedCount: 0,
    });
    expect(batchAction.query).toMatchObject({
      ...query,
    });
    expect(
      TraceDeleteBatchActionConfigSchema.parse(batchAction.config),
    ).toMatchObject({
      version: 1,
      source: "events",
      inFlightBatch: null,
    });

    expect(mockAddBatchAction).not.toHaveBeenCalled();
  });

  it("does not overwrite an active trace-delete BatchAction row", async () => {
    const { projectId, session, caller } = await createCaller();
    const batchActionId = `${projectId}-traces-trace-delete`;
    const existingQuery = traceDeleteQuery(`existing-user-${randomUUID()}`);

    await prisma.batchAction.create({
      data: {
        id: batchActionId,
        projectId,
        userId: session.user!.id,
        actionType: "trace-delete",
        tableName: "traces",
        status: BatchActionStatus.Processing,
        query: {
          ...existingQuery,
        },
        config: createTraceDeleteBatchActionConfig({
          cutoffCreatedAt: new Date(),
        }),
        totalCount: null,
        processedCount: 1,
        failedCount: 0,
      },
    });

    await expect(
      caller.traces.deleteMany({
        projectId,
        traceIds: [randomUUID()],
        isBatchAction: true,
        query: traceDeleteQuery(`new-user-${randomUUID()}`),
      }),
    ).rejects.toMatchObject({
      code: "CONFLICT",
    });

    expect(mockAddBatchAction).not.toHaveBeenCalled();

    const batchAction = await prisma.batchAction.findUniqueOrThrow({
      where: { id: batchActionId },
    });
    expect(batchAction.status).toBe(BatchActionStatus.Processing);
    expect(batchAction.processedCount).toBe(1);
    expect(batchAction.query).toMatchObject({
      ...existingQuery,
    });
  });

  it("resets a completed trace-delete BatchAction row exactly once", async () => {
    const { projectId, session, caller } = await createCaller();
    const batchActionId = `${projectId}-traces-trace-delete`;
    const completedQuery = traceDeleteQuery(`completed-user-${randomUUID()}`);
    const nextQuery = traceDeleteQuery(`next-user-${randomUUID()}`);

    await prisma.batchAction.create({
      data: {
        id: batchActionId,
        projectId,
        userId: session.user!.id,
        actionType: "trace-delete",
        tableName: "traces",
        status: BatchActionStatus.Completed,
        query: {
          ...completedQuery,
        },
        config: createTraceDeleteBatchActionConfig({
          cutoffCreatedAt: new Date(Date.now() - 1_000),
        }),
        totalCount: 1,
        processedCount: 1,
        failedCount: 0,
        finishedAt: new Date(),
        log: "completed",
      },
    });

    await caller.traces.deleteMany({
      projectId,
      traceIds: [randomUUID()],
      isBatchAction: true,
      query: nextQuery,
    });

    await expect(
      caller.traces.deleteMany({
        projectId,
        traceIds: [randomUUID()],
        isBatchAction: true,
        query: traceDeleteQuery(`conflicting-user-${randomUUID()}`),
      }),
    ).rejects.toMatchObject({
      code: "CONFLICT",
    });

    const batchAction = await prisma.batchAction.findUniqueOrThrow({
      where: { id: batchActionId },
    });
    expect(batchAction).toMatchObject({
      status: BatchActionStatus.Queued,
      totalCount: null,
      processedCount: 0,
      failedCount: 0,
      finishedAt: null,
      log: null,
    });
    expect(batchAction.query).toMatchObject({
      ...nextQuery,
    });
    expect(batchAction.query).not.toMatchObject({
      ...completedQuery,
    });
    expect(mockAddBatchAction).not.toHaveBeenCalled();
  });

  it("reports durable active trace-delete BatchActions as in progress", async () => {
    const { projectId, session, caller } = await createCaller();
    const batchActionId = `${projectId}-traces-trace-delete`;

    await prisma.batchAction.create({
      data: {
        id: batchActionId,
        projectId,
        userId: session.user!.id,
        actionType: "trace-delete",
        tableName: "traces",
        status: BatchActionStatus.Processing,
        query: {
          ...traceDeleteQuery(`existing-user-${randomUUID()}`),
        },
        config: createTraceDeleteBatchActionConfig({
          cutoffCreatedAt: new Date(),
        }),
        totalCount: null,
        processedCount: 1,
        failedCount: 0,
      },
    });

    await expect(
      caller.table.getIsBatchActionInProgress({
        projectId,
        actionId: ActionId.TraceDelete,
        tableName: BatchExportTableName.Traces,
      }),
    ).resolves.toBe(true);
    expect(mockGetBatchActionJobState).not.toHaveBeenCalled();
  });

  it("rejects batch deletes with comment filters without creating a batch action", async () => {
    const { projectId, caller } = await createCaller();
    const batchActionId = `${projectId}-traces-trace-delete`;

    await expect(
      caller.traces.deleteMany({
        projectId,
        traceIds: [randomUUID()],
        isBatchAction: true,
        query: {
          filter: [
            {
              column: "commentCount",
              operator: ">=" as const,
              value: 1,
              type: "number" as const,
            },
          ],
          orderBy: { column: "timestamp", order: "DESC" as const },
        },
      }),
    ).rejects.toMatchObject({
      code: "BAD_REQUEST",
      message:
        "Batch deletion does not support comment filters. Remove the comment filter and try again.",
    });

    await expect(
      prisma.batchAction.findUnique({ where: { id: batchActionId } }),
    ).resolves.toBeNull();
  });

  it("rejects deletes with an empty traceIds array even for batch actions", async () => {
    // An empty selection while select-all is armed signals a client-side
    // consistency issue; the server contract requires at least one traceId
    // for every delete and must fail loudly rather than absorb it.
    const { projectId, caller } = await createCaller();
    const batchActionId = `${projectId}-traces-trace-delete`;

    await expect(
      caller.traces.deleteMany({
        projectId,
        traceIds: [],
        isBatchAction: true,
        query: traceDeleteQuery(`delete-user-${randomUUID()}`),
      }),
    ).rejects.toMatchObject({
      code: "BAD_REQUEST",
      message: expect.stringContaining("Minimum 1 traceId is required."),
    });

    await expect(
      prisma.batchAction.findUnique({ where: { id: batchActionId } }),
    ).resolves.toBeNull();
  });
});
