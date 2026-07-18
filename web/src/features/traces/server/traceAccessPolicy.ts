type TraceAccessClient = {
  readonly traceControlState: {
    findUnique(input: {
      where: {
        projectId_traceId: { projectId: string; traceId: string };
      };
      select: { public: true; revision: true };
    }): Promise<{ public: boolean; revision: bigint } | null>;
  };
  readonly traceSession: {
    findFirst(input: {
      where: { id: string; projectId: string };
      select: { public: true };
    }): Promise<{ public: boolean } | null>;
  };
};

type TraceAccessSubject = {
  readonly id: string;
  readonly public: boolean | null | undefined;
  readonly sessionId: string | null;
};

export async function resolveTraceAccess(input: {
  readonly client: TraceAccessClient;
  readonly projectId: string;
  readonly trace: TraceAccessSubject | null;
  readonly isProjectMember: boolean;
  readonly isAdmin: boolean;
  readonly useTraceControlState: boolean;
}): Promise<{
  readonly allowed: boolean;
  readonly isTracePublic: boolean;
  readonly isSessionPublic: boolean;
}> {
  if (!input.trace) {
    return {
      allowed: input.isProjectMember || input.isAdmin,
      isTracePublic: false,
      isSessionPublic: false,
    };
  }

  const [controlState, traceSession] = await Promise.all([
    input.useTraceControlState
      ? input.client.traceControlState.findUnique({
          where: {
            projectId_traceId: {
              projectId: input.projectId,
              traceId: input.trace.id,
            },
          },
          select: { public: true, revision: true },
        })
      : null,
    input.trace.sessionId
      ? input.client.traceSession.findFirst({
          where: {
            id: input.trace.sessionId,
            projectId: input.projectId,
          },
          select: { public: true },
        })
      : null,
  ]);

  const isTracePublic = input.useTraceControlState
    ? controlState?.public === true
    : input.trace.public === true;
  const isSessionPublic = traceSession?.public === true;
  return {
    allowed:
      input.isProjectMember ||
      input.isAdmin ||
      isTracePublic ||
      isSessionPublic,
    isTracePublic,
    isSessionPublic,
  };
}
