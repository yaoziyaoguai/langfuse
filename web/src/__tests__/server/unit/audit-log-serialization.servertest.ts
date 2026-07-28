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
});
