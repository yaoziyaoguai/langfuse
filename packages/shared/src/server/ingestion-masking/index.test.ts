import { beforeEach, describe, expect, it, vi } from "vitest";

const state = vi.hoisted(() => ({ communityEnabled: true }));
const mocks = vi.hoisted(() => ({
  community: vi.fn(async (input: { data: unknown }) => ({
    success: true,
    data: input.data,
    masked: true,
  })),
  enterprise: vi.fn(async (input: { data: unknown }) => ({
    success: true,
    data: input.data,
    masked: false,
  })),
}));

vi.mock("../community-extensions/capabilities", () => ({
  isCommunityExtensionEnabled: () => state.communityEnabled,
}));

vi.mock("../community-extensions/ingestion-masking/index.js", () => ({
  applyCommunityIngestionMasking: mocks.community,
}));

vi.mock("../ee/ingestionMasking/index.js", () => ({
  applyIngestionMasking: mocks.enterprise,
}));

import { applyConfiguredIngestionMasking } from ".";

describe("applyConfiguredIngestionMasking", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    state.communityEnabled = true;
  });

  it("loads only the Community Extensions implementation when enabled", async () => {
    const input = { data: { value: "secret" }, projectId: "project-1" };

    await applyConfiguredIngestionMasking(input);

    expect(mocks.community).toHaveBeenCalledWith(input);
    expect(mocks.enterprise).not.toHaveBeenCalled();
  });

  it("preserves the upstream implementation when Community Extensions is disabled", async () => {
    state.communityEnabled = false;
    const input = {
      data: { value: "secret" },
      projectId: "project-1",
      propagatedHeaders: { "x-mask-tenant": "tenant-1" },
    };

    await applyConfiguredIngestionMasking(input);

    expect(mocks.enterprise).toHaveBeenCalledWith({
      ...input,
      propagatedHeaders: { "x-mask-tenant": "tenant-1" },
    });
    expect(mocks.community).not.toHaveBeenCalled();
  });
});
