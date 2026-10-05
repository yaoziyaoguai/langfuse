import { useHasEntitlementOrCommunityCapability } from "@/src/features/community-extensions/hooks";
import { api } from "@/src/utils/api";

export const useUiCustomization = () => {
  const hasAccess = useHasEntitlementOrCommunityCapability(
    "self-host-ui-customization",
    "ui-customization",
  );
  const customization = api.uiCustomization.get.useQuery(undefined, {
    enabled: hasAccess,
    refetchOnMount: false,
    refetchOnWindowFocus: false,
    refetchOnReconnect: false,
  });

  if (!hasAccess) return null;
  return customization.data ?? null;
};

export type UiCustomizationOption = keyof NonNullable<
  ReturnType<typeof useUiCustomization>
>;
