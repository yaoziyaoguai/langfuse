export const communityExtensionCapabilities = [
  "audit-logs",
  "project-rbac",
  "data-retention",
  "ingestion-masking",
  "protected-prompt-labels",
  "organization-creators",
  "ui-customization",
  "admin-api",
  "scim",
] as const;

export type CommunityExtensionCapability =
  (typeof communityExtensionCapabilities)[number];
