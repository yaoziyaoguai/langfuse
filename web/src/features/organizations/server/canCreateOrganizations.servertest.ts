import { describe, expect, it, vi } from "vitest";

import { canCreateOrganizations } from "./canCreateOrganizations";

describe("canCreateOrganizations", () => {
  it("allows everyone when no allowlist is configured", () => {
    expect(
      canCreateOrganizations("user@example.com", {
        allowedCreators: undefined,
        hasRestrictedCreatorAccess: vi.fn(() => true),
      }),
    ).toBe(true);
  });

  it("preserves unrestricted upstream behavior without the capability", () => {
    expect(
      canCreateOrganizations("user@example.com", {
        allowedCreators: "admin@example.com",
        hasRestrictedCreatorAccess: vi.fn(() => false),
      }),
    ).toBe(true);
  });

  it("matches allowed creator emails case-insensitively", () => {
    const dependencies = {
      allowedCreators: "admin@example.com,owner@example.com",
      hasRestrictedCreatorAccess: vi.fn(() => true),
    };

    expect(canCreateOrganizations("ADMIN@EXAMPLE.COM", dependencies)).toBe(
      true,
    );
    expect(canCreateOrganizations("user@example.com", dependencies)).toBe(
      false,
    );
  });

  it("rejects an account without an email when restriction is active", () => {
    expect(
      canCreateOrganizations(null, {
        allowedCreators: "admin@example.com",
        hasRestrictedCreatorAccess: vi.fn(() => true),
      }),
    ).toBe(false);
  });
});
