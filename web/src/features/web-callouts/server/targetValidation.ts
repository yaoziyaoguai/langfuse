import { TRPCError } from "@trpc/server";

import { type WebCalloutInvokeInput } from "@/src/features/web-callouts/types";
import { type PrismaClient } from "@langfuse/shared/src/db";
import {
  getObservationByIdFromEventsTable,
  getTraceByIdFromEventsTable,
  getTracesIdentifierForSessionFromEvents,
} from "@langfuse/shared/src/server";

export const assertTargetBelongsToProject = async ({
  prisma,
  input,
}: {
  prisma: PrismaClient;
  input: WebCalloutInvokeInput;
}) => {
  if (!input.traceId && !input.sessionId) {
    throw new TRPCError({
      code: "BAD_REQUEST",
      message: "Trace or session id is required.",
    });
  }

  const trace = input.traceId
    ? await getTraceByIdFromEventsTable({
        traceId: input.traceId,
        projectId: input.projectId,
        renderingProps: { truncated: true, shouldJsonParse: false },
      })
    : null;

  if (input.traceId && !trace) {
    throw new TRPCError({
      code: "NOT_FOUND",
      message: "Trace not found in project.",
    });
  }

  if (input.observationId) {
    if (!input.traceId) {
      throw new TRPCError({
        code: "BAD_REQUEST",
        message: "Observation callouts require a trace id.",
      });
    }

    const observation = await getObservationByIdFromEventsTable({
      id: input.observationId,
      traceId: input.traceId,
      projectId: input.projectId,
      fetchWithInputOutput: false,
      renderingProps: { truncated: true, shouldJsonParse: false },
    });
    if (!observation) {
      throw new TRPCError({
        code: "NOT_FOUND",
        message: "Observation not found in project.",
      });
    }
  }

  if (input.sessionId) {
    if (trace && trace.sessionId !== input.sessionId) {
      throw new TRPCError({
        code: "BAD_REQUEST",
        message: "Trace does not belong to the provided session.",
      });
    }

    const postgresSession = await prisma.traceSession.findFirst({
      where: { id: input.sessionId, projectId: input.projectId },
      select: { id: true },
    });
    const sessionExists =
      Boolean(postgresSession) ||
      (
        await getTracesIdentifierForSessionFromEvents(
          input.projectId,
          input.sessionId,
        )
      ).length > 0;
    if (!sessionExists) {
      throw new TRPCError({
        code: "NOT_FOUND",
        message: "Session not found in project.",
      });
    }
  }
};
