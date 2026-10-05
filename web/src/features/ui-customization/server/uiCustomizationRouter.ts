import { env } from "@/src/env.mjs";
import { hasPlanEntitlementOrCommunityCapability } from "@/src/features/community-extensions/server/access";
import {
  authenticatedProcedure,
  createTRPCRouter,
} from "@/src/server/api/trpc";
import { getVisibleProductModules } from "../productModules";

const readUiCustomization = () => {
  const navigationLinks = {
    documentationHref: env.LANGFUSE_UI_DOCUMENTATION_HREF,
    supportHref: env.LANGFUSE_UI_SUPPORT_HREF,
    feedbackHref: env.LANGFUSE_UI_FEEDBACK_HREF,
  };
  const branding = {
    logoLightModeHref: env.LANGFUSE_UI_LOGO_LIGHT_MODE_HREF,
    logoDarkModeHref: env.LANGFUSE_UI_LOGO_DARK_MODE_HREF,
  };
  const modelDefaults = {
    defaultModelAdapter: env.LANGFUSE_UI_DEFAULT_MODEL_ADAPTER,
    defaultBaseUrlOpenAI: env.LANGFUSE_UI_DEFAULT_BASE_URL_OPENAI,
    defaultBaseUrlAnthropic: env.LANGFUSE_UI_DEFAULT_BASE_URL_ANTHROPIC,
    defaultBaseUrlAzure: env.LANGFUSE_UI_DEFAULT_BASE_URL_AZURE,
  };

  return {
    hostname: env.LANGFUSE_UI_API_HOST,
    ...navigationLinks,
    ...branding,
    ...modelDefaults,
    visibleModules: getVisibleProductModules(
      env.LANGFUSE_UI_VISIBLE_PRODUCT_MODULES,
      env.LANGFUSE_UI_HIDDEN_PRODUCT_MODULES,
    ),
  };
};

export const uiCustomizationRouter = createTRPCRouter({
  get: authenticatedProcedure.query(({ ctx }) => {
    const hasAccess = hasPlanEntitlementOrCommunityCapability({
      plan: ctx.session.environment.selfHostedInstancePlan,
      entitlement: "self-host-ui-customization",
      capability: "ui-customization",
    });
    if (!hasAccess) return null;
    return readUiCustomization();
  }),
});
