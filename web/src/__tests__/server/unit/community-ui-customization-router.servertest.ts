import type { Session } from "next-auth";
import { describe, expect, it, vi } from "vitest";
import type * as EnvModule from "@/src/env.mjs";

vi.mock("@/src/env.mjs", async (importOriginal) => {
  const actual = await importOriginal<typeof EnvModule>();
  return {
    ...actual,
    env: {
      ...actual.env,
      LANGFUSE_COMMUNITY_EXTENSIONS_ENABLED: "true",
      LANGFUSE_UI_API_HOST: "langfuse.internal.example",
      LANGFUSE_UI_DOCUMENTATION_HREF: "https://docs.internal.example",
      LANGFUSE_UI_LOGO_LIGHT_MODE_HREF:
        "https://assets.internal.example/logo-light.svg",
      LANGFUSE_UI_VISIBLE_PRODUCT_MODULES: "tracing,datasets",
      LANGFUSE_UI_HIDDEN_PRODUCT_MODULES: "playground",
    },
  };
});

import { uiCustomizationRouter } from "@/src/features/ui-customization/server/uiCustomizationRouter";
import { createInnerTRPCContext } from "@/src/server/api/trpc";

const session: Session = {
  expires: "1",
  user: {
    id: "user-1",
    canCreateOrganizations: true,
    organizations: [],
    featureFlags: {
      excludeClickhouseRead: false,
      templateFlag: false,
      searchBar: false,
      v4BetaToggleVisible: false,
      observationEvals: false,
      experimentsV4Enabled: false,
    },
    admin: false,
  },
  environment: {
    enableExperimentalFeatures: false,
    selfHostedInstancePlan: null,
    communityExtensionEnabled: true,
  },
};

describe("Community Extensions UI customization", () => {
  it("returns configured branding and product modules without a licensed plan", async () => {
    const caller = uiCustomizationRouter.createCaller(
      createInnerTRPCContext({ session, headers: {} }),
    );

    await expect(caller.get()).resolves.toMatchObject({
      hostname: "langfuse.internal.example",
      documentationHref: "https://docs.internal.example",
      logoLightModeHref: "https://assets.internal.example/logo-light.svg",
      visibleModules: ["tracing", "datasets"],
    });
  });

  it("still requires an authenticated session", async () => {
    const caller = uiCustomizationRouter.createCaller(
      createInnerTRPCContext({ session: null, headers: {} }),
    );

    await expect(caller.get()).rejects.toMatchObject({
      code: "UNAUTHORIZED",
    });
  });
});
