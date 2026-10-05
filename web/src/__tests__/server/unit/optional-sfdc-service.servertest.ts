import { beforeEach, describe, expect, it, vi } from "vitest";

const mocks = vi.hoisted(() => ({
  env: { NEXT_PUBLIC_LANGFUSE_CLOUD_REGION: undefined as string | undefined },
  loaded: vi.fn(),
  getSfdcService: vi.fn(),
}));

vi.mock("@/src/env.mjs", () => ({ env: mocks.env }));
vi.mock("@/src/ee/features/sfdc-sync/server", () => {
  mocks.loaded();
  return { getSfdcService: mocks.getSfdcService };
});

describe("optional upstream SFDC integration", () => {
  beforeEach(() => {
    vi.resetModules();
    vi.clearAllMocks();
    mocks.env.NEXT_PUBLIC_LANGFUSE_CLOUD_REGION = undefined;
  });

  it("does not load upstream Cloud integration in a self-hosted deployment", async () => {
    const { getOptionalSfdcService } =
      await import("@/src/features/sfdc-sync/server/getOptionalSfdcService");
    expect(await getOptionalSfdcService()).toBeNull();
    expect(mocks.loaded).not.toHaveBeenCalled();
    expect(mocks.getSfdcService).not.toHaveBeenCalled();
  });

  it("preserves the upstream service in Cloud deployments", async () => {
    mocks.env.NEXT_PUBLIC_LANGFUSE_CLOUD_REGION = "US";
    const service = { setUserRole: vi.fn() };
    mocks.getSfdcService.mockReturnValue(service);
    const { getOptionalSfdcService } =
      await import("@/src/features/sfdc-sync/server/getOptionalSfdcService");
    expect(await getOptionalSfdcService()).toBe(service);
    expect(mocks.getSfdcService).toHaveBeenCalledOnce();
  });
});
