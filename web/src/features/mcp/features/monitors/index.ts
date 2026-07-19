import type { McpFeatureModule } from "../../server/registry";
import { getMonitorTool, handleGetMonitor } from "./tools/getMonitor";
import { listMonitorsTool, handleListMonitors } from "./tools/listMonitors";

export const monitorsFeature = {
  name: "monitors",
  description: "Inspect monitors in the current Langfuse project",
  tools: [
    { definition: listMonitorsTool, handler: handleListMonitors },
    { definition: getMonitorTool, handler: handleGetMonitor },
  ],
} as const satisfies McpFeatureModule;
