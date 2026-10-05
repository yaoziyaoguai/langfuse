import { isCommunityExtensionEnabled } from "../community-extensions/capabilities";
import type { IngestionMaskingInput, IngestionMaskingResult } from "./types";

export type { IngestionMaskingInput, IngestionMaskingResult };

export async function applyConfiguredIngestionMasking<T>(
  input: IngestionMaskingInput<T>,
): Promise<IngestionMaskingResult<T>> {
  if (isCommunityExtensionEnabled()) {
    const { applyCommunityIngestionMasking } =
      await import("../community-extensions/ingestion-masking/index.js");
    return applyCommunityIngestionMasking(input);
  }

  const { applyIngestionMasking } =
    await import("../ee/ingestionMasking/index.js");
  const result = await applyIngestionMasking({
    ...input,
    propagatedHeaders: input.propagatedHeaders
      ? { ...input.propagatedHeaders }
      : undefined,
  });
  return result;
}
