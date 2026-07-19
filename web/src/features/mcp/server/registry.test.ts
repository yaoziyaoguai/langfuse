import { describe, expect, it, vi } from "vitest";

import type { ServerContext } from "../types";
import { ToolRegistry } from "./registry";

vi.mock("@langfuse/shared/src/server", () => ({
  logger: {
    info: vi.fn(),
  },
}));

const context: ServerContext = {
  projectId: "project-1",
  orgId: "org-1",
  apiKeyId: "api-key-1",
  accessLevel: "project",
  publicKey: "pk-lf-test",
};

describe("MCP tool registry community capability gates", () => {
  it("keeps deferred tools discoverable but blocks their handlers", async () => {
    const handler = vi.fn().mockResolvedValue({ executed: true });
    const registry = new ToolRegistry();
    registry.register({
      name: "evals",
      description: "Deferred evaluator tools",
      isEnabled: () => false,
      tools: [
        {
          definition: {
            name: "create-evaluator",
            description: "Create an evaluator",
            inputSchema: { type: "object" },
          },
          handler,
        },
      ],
    });

    await expect(registry.getToolDefinitions(context)).resolves.toEqual([
      expect.objectContaining({ name: "create-evaluator" }),
    ]);

    const tool = await registry.getEnabledTool("create-evaluator", context);
    await expect(tool?.handler({}, context)).rejects.toMatchObject({
      message: expect.stringContaining("R1B_EVALUATIONS_UNAVAILABLE"),
    });
    expect(handler).not.toHaveBeenCalled();
  });
});
