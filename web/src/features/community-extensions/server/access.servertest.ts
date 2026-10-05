import { describe, expect, it, vi } from "vitest";
import {
  hasPlanEntitlementOrCommunityCapability,
  requireEntitlementOrCommunityCapability,
} from "./access";

describe("requireEntitlementOrCommunityCapability", () => {
  const access = {
    entitlement: "audit-logs" as const,
    capability: "audit-logs" as const,
    sessionUser: {} as never,
    orgId: "org-1",
  };

  it("allows an independently implemented community capability", () => {
    const hasEntitlement = vi.fn(() => false);

    expect(() =>
      requireEntitlementOrCommunityCapability(access, {
        hasCommunityExtensionCapability: () => true,
        hasEntitlement,
      }),
    ).not.toThrow();
    expect(hasEntitlement).not.toHaveBeenCalled();
  });

  it("preserves official entitlement access when community edition is disabled", () => {
    expect(() =>
      requireEntitlementOrCommunityCapability(access, {
        hasCommunityExtensionCapability: () => false,
        hasEntitlement: () => true,
      }),
    ).not.toThrow();
  });

  it("rejects access when neither path grants the capability", () => {
    expect(() =>
      requireEntitlementOrCommunityCapability(access, {
        hasCommunityExtensionCapability: () => false,
        hasEntitlement: () => false,
      }),
    ).toThrow("audit-logs");
  });
});

describe("hasPlanEntitlementOrCommunityCapability", () => {
  const access = {
    entitlement: "rbac-project-roles" as const,
    capability: "project-rbac" as const,
    plan: "oss" as const,
  };

  it("accepts the community capability without changing the official plan", () => {
    expect(
      hasPlanEntitlementOrCommunityCapability(access, {
        hasCommunityExtensionCapability: () => true,
        hasEntitlementBasedOnPlan: () => false,
      }),
    ).toBe(true);
  });

  it("preserves official plan entitlement behavior", () => {
    expect(
      hasPlanEntitlementOrCommunityCapability(access, {
        hasCommunityExtensionCapability: () => false,
        hasEntitlementBasedOnPlan: () => true,
      }),
    ).toBe(true);
  });
});
