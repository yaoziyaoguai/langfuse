import { env } from "@/src/env.mjs";
import {
  hasEntitlement,
  hasEntitlementBasedOnPlan,
  type HasEntitlementParams,
} from "@/src/features/entitlements/server/hasEntitlement";
import { type Entitlement } from "@/src/features/entitlements/constants/entitlements";
import { type CommunityExtensionCapability, type Plan } from "@langfuse/shared";
import { TRPCError } from "@trpc/server";

type CapabilityAccess = HasEntitlementParams & {
  entitlement: Entitlement;
  capability: CommunityExtensionCapability;
};

type CapabilityAccessDependencies = {
  hasCommunityExtensionCapability: (
    capability: CommunityExtensionCapability,
  ) => boolean;
  hasEntitlement: (params: HasEntitlementParams) => Boolean;
};

type PlanCapabilityAccess = {
  entitlement: Entitlement;
  capability: CommunityExtensionCapability;
  plan: Plan | null;
};

type PlanCapabilityAccessDependencies = {
  hasCommunityExtensionCapability: (
    capability: CommunityExtensionCapability,
  ) => boolean;
  hasEntitlementBasedOnPlan: typeof hasEntitlementBasedOnPlan;
};

const defaultDependencies: CapabilityAccessDependencies = {
  hasCommunityExtensionCapability: () =>
    env.LANGFUSE_COMMUNITY_EXTENSIONS_ENABLED === "true",
  hasEntitlement,
};

const defaultPlanDependencies: PlanCapabilityAccessDependencies = {
  hasCommunityExtensionCapability: () =>
    env.LANGFUSE_COMMUNITY_EXTENSIONS_ENABLED === "true",
  hasEntitlementBasedOnPlan,
};

export const requireEntitlementOrCommunityCapability = (
  access: CapabilityAccess,
  dependencies: CapabilityAccessDependencies = defaultDependencies,
): void => {
  if (dependencies.hasCommunityExtensionCapability(access.capability)) return;
  if (dependencies.hasEntitlement(access)) return;

  throw new TRPCError({
    code: "FORBIDDEN",
    message: `Unauthorized, access requires entitlement or Community Extensions capability: ${access.capability}`,
  });
};

export const hasPlanEntitlementOrCommunityCapability = (
  access: PlanCapabilityAccess,
  dependencies: PlanCapabilityAccessDependencies = defaultPlanDependencies,
): boolean =>
  dependencies.hasCommunityExtensionCapability(access.capability) ||
  dependencies.hasEntitlementBasedOnPlan({
    plan: access.plan,
    entitlement: access.entitlement,
  });
