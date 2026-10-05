import { describe, expect, it, vi } from "vitest";

vi.mock("../../env", () => ({
  env: {
    LANGFUSE_COMMUNITY_EXTENSIONS_ENABLED: "false",
  },
}));

import {
  communityExtensionCapabilities,
  hasCommunityExtensionCapability,
  isCommunityExtensionEnabled,
} from "./capabilities";

describe("community edition capabilities", () => {
  it("is disabled unless explicitly enabled", () => {
    expect(
      isCommunityExtensionEnabled({
        LANGFUSE_COMMUNITY_EXTENSIONS_ENABLED: "false",
      }),
    ).toBe(false);
  });

  it("enables every independently implemented capability with one flag", () => {
    const communityEnv = {
      LANGFUSE_COMMUNITY_EXTENSIONS_ENABLED: "true" as const,
    };

    expect(isCommunityExtensionEnabled(communityEnv)).toBe(true);
    expect(communityExtensionCapabilities).toEqual([
      "audit-logs",
      "project-rbac",
      "data-retention",
      "ingestion-masking",
      "protected-prompt-labels",
      "organization-creators",
      "ui-customization",
      "admin-api",
      "scim",
    ]);
    expect(
      communityExtensionCapabilities.every((capability) =>
        hasCommunityExtensionCapability(capability, communityEnv),
      ),
    ).toBe(true);
  });
});
