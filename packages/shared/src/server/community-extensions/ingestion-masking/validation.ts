import type { SharedEnv } from "../../../env";
import {
  type OutboundUrlValidationWhitelist,
  OutboundUrlValidationError,
  parseOutboundUrl,
  validateOutboundUrlHost,
} from "../../outbound-url";

type CommunityMaskingValidationEnv = Pick<
  SharedEnv,
  | "LANGFUSE_COMMUNITY_MASKING_WHITELISTED_HOST"
  | "LANGFUSE_COMMUNITY_MASKING_WHITELISTED_IPS"
  | "LANGFUSE_COMMUNITY_MASKING_WHITELISTED_IP_SEGMENTS"
  | "NEXT_PUBLIC_LANGFUSE_CLOUD_REGION"
>;

export const COMMUNITY_MASKING_URL_VALIDATION_LOG_CONTEXT =
  "Community ingestion masking callback";

export function communityMaskingWhitelistFromEnv(
  environment: CommunityMaskingValidationEnv,
): OutboundUrlValidationWhitelist {
  if (environment.NEXT_PUBLIC_LANGFUSE_CLOUD_REGION) {
    return { hosts: [], ips: [], ip_ranges: [] };
  }

  return {
    hosts: environment.LANGFUSE_COMMUNITY_MASKING_WHITELISTED_HOST,
    ips: environment.LANGFUSE_COMMUNITY_MASKING_WHITELISTED_IPS,
    ip_ranges: environment.LANGFUSE_COMMUNITY_MASKING_WHITELISTED_IP_SEGMENTS,
  };
}

export async function validateCommunityMaskingCallbackUrl(
  urlString: string,
  environment: CommunityMaskingValidationEnv,
): Promise<void> {
  const url = parseOutboundUrl(urlString);
  if (!["http:", "https:"].includes(url.protocol)) {
    throw new OutboundUrlValidationError(
      "protocol-not-allowed",
      "Community masking callback must use HTTP or HTTPS",
    );
  }
  if (
    environment.NEXT_PUBLIC_LANGFUSE_CLOUD_REGION &&
    url.protocol !== "https:"
  ) {
    throw new OutboundUrlValidationError(
      "https-required",
      "Community masking callback must use HTTPS in Langfuse Cloud",
    );
  }

  await validateOutboundUrlHost({
    url,
    whitelist: communityMaskingWhitelistFromEnv(environment),
    logContext: COMMUNITY_MASKING_URL_VALIDATION_LOG_CONTEXT,
    shouldSkipDnsCheckForLiteralIps: true,
  });
}
