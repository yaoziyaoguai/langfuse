import { env, type SharedEnv } from "../../env";
import {
  communityExtensionCapabilities,
  type CommunityExtensionCapability,
} from "../../features/community-extensions/capabilities";

type CommunityExtensionEnv = Pick<
  SharedEnv,
  "LANGFUSE_COMMUNITY_EXTENSIONS_ENABLED"
>;

export { communityExtensionCapabilities };
export type { CommunityExtensionCapability };

export const isCommunityExtensionEnabled = (
  envOverride: CommunityExtensionEnv = env,
): boolean => envOverride.LANGFUSE_COMMUNITY_EXTENSIONS_ENABLED === "true";

export const hasCommunityExtensionCapability = (
  _capability: CommunityExtensionCapability,
  envOverride: CommunityExtensionEnv = env,
): boolean => isCommunityExtensionEnabled(envOverride);
