import { env } from "@/src/env.mjs";
import { hasPlanEntitlementOrCommunityCapability } from "@/src/features/community-extensions/server/access";
import { getSelfHostedInstancePlanServerSide } from "@/src/features/entitlements/server/getPlan";

type OrganizationCreatorDependencies = {
  allowedCreators?: string;
  hasRestrictedCreatorAccess: () => boolean;
};

const defaultDependencies: OrganizationCreatorDependencies = {
  allowedCreators: env.LANGFUSE_ALLOWED_ORGANIZATION_CREATORS,
  hasRestrictedCreatorAccess: () =>
    hasPlanEntitlementOrCommunityCapability({
      plan: getSelfHostedInstancePlanServerSide(),
      entitlement: "self-host-allowed-organization-creators",
      capability: "organization-creators",
    }),
};

export function canCreateOrganizations(
  userEmail: string | null,
  dependencies: OrganizationCreatorDependencies = defaultDependencies,
): boolean {
  // If no allowlist is configured, or the entitlement is unavailable, allow
  // all users to create organizations.
  if (
    !dependencies.allowedCreators ||
    !dependencies.hasRestrictedCreatorAccess()
  ) {
    return true;
  }

  if (!userEmail) {
    return false;
  }

  const allowedOrgCreators = dependencies.allowedCreators
    .toLowerCase()
    .split(",");

  return allowedOrgCreators.includes(userEmail.toLowerCase());
}
