import { describe, expect, it, vi } from "vitest";

import { resolveTraceAccess } from "./traceAccessPolicy";

function client(input: { tracePublic?: boolean; sessionPublic?: boolean }) {
  return {
    traceControlState: {
      findUnique: vi
        .fn()
        .mockResolvedValue(
          input.tracePublic === undefined
            ? null
            : { public: input.tracePublic, revision: 2n },
        ),
    },
    traceSession: {
      findFirst: vi
        .fn()
        .mockResolvedValue(
          input.sessionPublic === undefined
            ? null
            : { public: input.sessionPublic },
        ),
    },
  };
}

describe("trace access policy", () => {
  it("uses revisioned TraceControlState as the Doris public source", async () => {
    const prisma = client({ tracePublic: false });

    await expect(
      resolveTraceAccess({
        client: prisma,
        projectId: "project-1",
        trace: {
          id: "trace-1",
          public: true,
          sessionId: null,
        },
        isProjectMember: false,
        isAdmin: false,
        useTraceControlState: true,
      }),
    ).resolves.toMatchObject({ allowed: false, isTracePublic: false });
  });

  it("allows a Doris trace published in TraceControlState", async () => {
    await expect(
      resolveTraceAccess({
        client: client({ tracePublic: true }),
        projectId: "project-1",
        trace: {
          id: "trace-1",
          public: false,
          sessionId: null,
        },
        isProjectMember: false,
        isAdmin: false,
        useTraceControlState: true,
      }),
    ).resolves.toMatchObject({ allowed: true, isTracePublic: true });
  });

  it("keeps project, admin, session-public, and legacy access paths explicit", async () => {
    const prisma = client({ sessionPublic: true });
    await expect(
      resolveTraceAccess({
        client: prisma,
        projectId: "project-1",
        trace: {
          id: "trace-1",
          public: false,
          sessionId: "session-1",
        },
        isProjectMember: false,
        isAdmin: false,
        useTraceControlState: false,
      }),
    ).resolves.toMatchObject({ allowed: true, isSessionPublic: true });

    await expect(
      resolveTraceAccess({
        client: client({}),
        projectId: "project-1",
        trace: { id: "trace-1", public: false, sessionId: null },
        isProjectMember: true,
        isAdmin: false,
        useTraceControlState: false,
      }),
    ).resolves.toMatchObject({ allowed: true });
  });
});
