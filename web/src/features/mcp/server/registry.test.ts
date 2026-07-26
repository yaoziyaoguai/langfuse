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
    const registry = new ToolRegistry("doris", async () => false);
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

  it("blocks R1B dataset-run tools without hiding R1A dataset tools", async () => {
    const runHandler = vi.fn().mockResolvedValue({ executed: true });
    const itemHandler = vi.fn().mockResolvedValue({ executed: true });
    const registry = new ToolRegistry("doris", async () => false);
    registry.register({
      name: "datasets",
      description: "Dataset tools",
      tools: [
        {
          definition: {
            name: "listDatasetRuns",
            description: "List dataset runs",
            inputSchema: { type: "object" },
          },
          handler: runHandler,
        },
        {
          definition: {
            name: "listDatasetItems",
            description: "List dataset items",
            inputSchema: { type: "object" },
          },
          handler: itemHandler,
        },
      ],
    });

    await expect(registry.getToolDefinitions(context)).resolves.toHaveLength(2);

    const runTool = await registry.getEnabledTool("listDatasetRuns", context);
    await expect(runTool?.handler({}, context)).rejects.toMatchObject({
      message: expect.stringContaining("R1B_EXPERIMENTS_UNAVAILABLE"),
    });
    expect(runHandler).not.toHaveBeenCalled();

    const itemTool = await registry.getEnabledTool("listDatasetItems", context);
    await expect(itemTool?.handler({}, context)).resolves.toEqual({
      executed: true,
    });
    expect(itemHandler).toHaveBeenCalledOnce();
  });

  it("preserves the original handlers with ClickHouse", async () => {
    const handler = vi.fn().mockResolvedValue({ executed: true });
    const registry = new ToolRegistry("clickhouse");
    registry.register({
      name: "evals",
      description: "ClickHouse evaluator tools",
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

    const tool = await registry.getEnabledTool("create-evaluator", context);
    await expect(tool?.handler({}, context)).resolves.toEqual({
      executed: true,
    });
    expect(handler).toHaveBeenCalledOnce();
  });

  it("enables Doris evaluator handlers only after durable activation", async () => {
    const handler = vi.fn().mockResolvedValue({ executed: true });
    const isCapabilityAvailable = vi.fn().mockResolvedValue(true);
    const registry = new ToolRegistry("doris", isCapabilityAvailable);
    registry.register({
      name: "evals",
      description: "Activated evaluator tools",
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

    const tool = await registry.getEnabledTool("create-evaluator", context);
    await expect(tool?.handler({}, context)).resolves.toEqual({
      executed: true,
    });
    expect(isCapabilityAvailable).toHaveBeenCalledWith("evaluations", "doris");
    expect(handler).toHaveBeenCalledOnce();
  });
});
