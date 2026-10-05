import { type CommunityExtensionCapability } from "@langfuse/shared";
import { useSession } from "next-auth/react";
import { type Entitlement } from "@/src/features/entitlements/constants/entitlements";
import { useHasEntitlement } from "@/src/features/entitlements/hooks";

export const useHasCommunityExtensionCapability = (
  _capability: CommunityExtensionCapability,
): boolean => {
  const session = useSession();
  return session.data?.environment.communityExtensionEnabled === true;
};

export const useHasEntitlementOrCommunityCapability = (
  entitlement: Entitlement,
  capability: CommunityExtensionCapability,
): boolean => {
  const hasEntitlement = useHasEntitlement(entitlement);
  const hasCommunityCapability = useHasCommunityExtensionCapability(capability);
  return hasEntitlement || hasCommunityCapability;
};
