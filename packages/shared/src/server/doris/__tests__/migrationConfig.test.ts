import { describe, expect, it } from "vitest";

import { migrationConfigFromEnv } from "../../../../doris/scripts/migrate";

describe("Doris migrator configuration", () => {
  it("uses explicit credential-free local settings", () => {
    expect(
      migrationConfigFromEnv(
        {
          DORIS_MIGRATION_URL: "mysql://127.0.0.1:9031/langfuse",
          DORIS_MIGRATION_USER: "root",
          DORIS_MIGRATION_PASSWORD: "local-only",
        },
        "development",
      ),
    ).toMatchObject({
      host: "127.0.0.1",
      port: 9031,
      database: "langfuse",
      user: "root",
      tls: false,
    });
  });

  it("requires a dedicated, TLS-verified production identity", () => {
    expect(() =>
      migrationConfigFromEnv(
        {
          DORIS_MIGRATION_URL: "mysql://doris-fe.internal:9030/langfuse",
          DORIS_MIGRATION_USER: "root",
          DORIS_MIGRATION_PASSWORD: "secret",
          DORIS_MIGRATION_TLS_ENABLED: "true",
        },
        "production",
      ),
    ).toThrow(/least-privilege/i);

    expect(() =>
      migrationConfigFromEnv(
        {
          DORIS_MIGRATION_URL: "mysql://doris-fe.internal:9030/langfuse",
          DORIS_MIGRATION_USER: "langfuse_migrator",
          DORIS_MIGRATION_PASSWORD: "secret",
          DORIS_MIGRATION_TLS_ENABLED: "false",
        },
        "production",
      ),
    ).toThrow(/verified TLS/i);
  });

  it("rejects credentials embedded in the migration URL", () => {
    expect(() =>
      migrationConfigFromEnv(
        {
          DORIS_MIGRATION_URL:
            "mysql://langfuse_migrator:secret@doris-fe.internal:9030/langfuse",
          DORIS_MIGRATION_USER: "langfuse_migrator",
        },
        "production",
      ),
    ).toThrow(/without embedded credentials/i);
  });
});
