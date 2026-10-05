import { describe, expect, it, vi } from "vitest";

import { auditLog } from "@/src/features/audit-logs/auditLog";

describe("auditLog serialization", () => {
  it("serializes BigInt fields in durable analytics records", async () => {
    const create = vi.fn().mockResolvedValue({});
    const transaction = {
      auditLog: { create },
    };

    await auditLog(
      {
        userId: "user-1",
        orgId: "org-1",
        projectId: "project-1",
        resourceType: "batchExport",
        resourceId: "export-1",
        action: "create",
        after: {
          deploymentGeneration: 7n,
          nested: { deliveryGeneration: 9n },
        },
      },
      transaction as never,
    );

    expect(create).toHaveBeenCalledOnce();
    expect(JSON.parse(create.mock.calls[0]?.[0].data.after)).toEqual({
      deploymentGeneration: "7",
      nested: { deliveryGeneration: "9" },
    });
  });

  it("redacts credential fields before they become durable audit data", async () => {
    const create = vi.fn().mockResolvedValue({});
    const transaction = {
      auditLog: { create },
    };

    await auditLog(
      {
        userId: "user-1",
        orgId: "org-1",
        projectId: "project-1",
        resourceType: "llmApiKey",
        resourceId: "connection-1",
        action: "delete",
        before: {
          provider: "openai",
          secretKey: "encrypted-or-plain-secret",
          secretAccessKey: "aws-secret",
          encryptionKey: "encryption-secret",
          signingKey: "signing-secret",
          apiSecret: "provider-secret",
          encryptedPosthogApiKey: "encrypted-api-key",
          extraHeaders: "encrypted-headers",
          authConfig: { clientId: "client-id", clientSecret: "oauth-secret" },
          serviceCredentials: "encrypted-credentials",
          remoteExperimentRequestHeaders: { "x-secret": "encrypted-value" },
          nested: {
            clientSecret: "oauth-secret",
            publicKey: "pk-safe-to-display",
            accessKeyId: "AKIA-safe-to-display",
          },
        },
      },
      transaction as never,
    );

    expect(JSON.parse(create.mock.calls[0]?.[0].data.before)).toEqual({
      provider: "openai",
      secretKey: "[REDACTED]",
      secretAccessKey: "[REDACTED]",
      encryptionKey: "[REDACTED]",
      signingKey: "[REDACTED]",
      apiSecret: "[REDACTED]",
      encryptedPosthogApiKey: "[REDACTED]",
      extraHeaders: "[REDACTED]",
      authConfig: "[REDACTED]",
      serviceCredentials: "[REDACTED]",
      remoteExperimentRequestHeaders: "[REDACTED]",
      nested: {
        clientSecret: "[REDACTED]",
        publicKey: "pk-safe-to-display",
        accessKeyId: "AKIA-safe-to-display",
      },
    });
  });
});
