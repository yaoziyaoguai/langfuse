import type {
  AnalyticsBackend,
  ScenarioDefinition,
  ScenarioFlag,
  ScenarioRegistration,
} from "./types";
import { ANALYTICS_SMOKE_FIXTURE_DATE_FLAG, SeedError } from "./types";

export type ScenarioRegistry = Record<string, ScenarioRegistration>;

const clickhouseOnly = {
  supportedBackends: ["clickhouse"] as const,
  needsWeb: false,
  loadsSharedClients: true,
};

/**
 * Metadata stays lightweight so `seed list` and unsupported Doris runs never
 * import legacy scenario modules (which in turn import ClickHouse clients).
 */
export const scenarioRegistry: ScenarioRegistry = {
  "analytics-smoke": {
    name: "analytics-smoke",
    description:
      "One trace and observation through the public OTLP endpoint, followed by backend-neutral public API readback. Supported by ClickHouse and Doris.",
    supportsV4: false,
    flags: [ANALYTICS_SMOKE_FIXTURE_DATE_FLAG],
    supportedBackends: ["clickhouse", "doris"],
    needsWeb: true,
    loadsSharedClients: false,
    load: async () =>
      (await import("./analytics-smoke.js")).analyticsSmokeScenario,
  },
  "experiment-foundation": {
    name: "experiment-foundation",
    description:
      "One public dataset/item, OTLP experiment trace, dataset-run item, and associated score with backend-neutral public readback. Supported by ClickHouse and Doris once dataset-run ingestion is active.",
    supportsV4: false,
    flags: [ANALYTICS_SMOKE_FIXTURE_DATE_FLAG],
    supportedBackends: ["clickhouse", "doris"],
    needsWeb: true,
    loadsSharedClients: false,
    load: async () =>
      (await import("./experiment-foundation.js")).experimentFoundationScenario,
  },
  "evaluator-states": {
    name: "evaluator-states",
    description:
      "Backend-neutral evaluator list/detail fixture with empty, queued, running, success, terminal error, retrying, and recovered states. Analytics writes use public APIs; only evaluator control-plane rows are seeded in Postgres.",
    supportsV4: false,
    flags: [ANALYTICS_SMOKE_FIXTURE_DATE_FLAG],
    supportedBackends: ["clickhouse", "doris"],
    needsWeb: true,
    loadsSharedClients: true,
    load: async () =>
      (await import("./evaluator-states.js")).evaluatorStatesScenario,
  },
  "integration-states": {
    name: "integration-states",
    description:
      "Backend-neutral PostHog, Mixpanel, and Blob Storage settings fixture. All integrations are disabled to guarantee zero third-party egress; Blob Storage includes a visible terminal-error state.",
    supportsV4: false,
    flags: [],
    supportedBackends: ["clickhouse", "doris"],
    needsWeb: false,
    loadsSharedClients: true,
    load: async () =>
      (await import("./integration-states.js")).integrationStatesScenario,
  },
  "trace-tree": {
    name: "trace-tree",
    description:
      "One trace with a large, branching observation tree: all observation kinds, guaranteed depth, a hub node with many children, errors/retries/missing end times, configurable payload size and style.",
    supportsV4: true,
    flags: [
      flag("observations", "number", 200, "total observations in the tree"),
      flag("depth", "number", 8, "guaranteed tree depth (backbone chain)"),
      flag("breadth", "number", 30, "children under the hub node"),
      flag(
        "payload-bytes",
        "number",
        25_000,
        "approx bytes for the root input payload (max 50 MB)",
      ),
      flag(
        "payload-style",
        "string",
        "json",
        "json | text | malformed | unicode | bignum | base64",
      ),
      flag(
        "v4",
        "boolean",
        false,
        "also mirror the tree into v4 events_full/events_core",
      ),
      flag(
        "async-parents",
        "boolean",
        false,
        "root + hub nodes end immediately while their subtree keeps running (async/fire-and-forget shape; surfaces the subtree wall-clock duration badge)",
      ),
      flag(
        "plain",
        "boolean",
        false,
        "restrict observation types to SPAN/GENERATION/EVENT (no agentic types) — the shape whose graph panel is collapsed by default (LFE-10665)",
      ),
      flag(
        "scores-per-node",
        "number",
        0,
        "attach N distinct scores to every observation (the LFE-10591 'lots of scores' shape; try 12), 0-100",
      ),
    ],
    ...clickhouseOnly,
    load: async () => (await import("./trace-tree.js")).traceTreeScenario,
  },
  "agent-timeline": {
    name: "agent-timeline",
    description:
      "One trace: a realistic LangGraph-style refine-loop agent (planner → retriever → generator → critic → loop) unrolled over N turns, with observations staggered across a real timeline and langgraph_node/step metadata. Exercises the graph's real-flow-with-loop rendering and a scrubbable timeline (vs. trace-tree's all-at-once hub).",
    supportsV4: true,
    flags: [
      flag(
        "turns",
        "number",
        6,
        "refine-loop iterations (each is planner→retriever→generator→critic)",
      ),
      flag(
        "timing-only",
        "boolean",
        false,
        "omit langgraph_node/langgraph_step metadata, so the graph is built from the pure timing-based fallback (every observation becomes a node)",
      ),
      flag(
        "v4",
        "boolean",
        false,
        "also mirror into v4 events_full/events_core",
      ),
    ],
    ...clickhouseOnly,
    load: async () =>
      (await import("./agent-timeline.js")).agentTimelineScenario,
  },
  "deep-chain": {
    name: "deep-chain",
    description:
      "One trace whose observations form a single deep parent chain of sequential generations (child starts after parent ends; depth = observation count) — the mis-parented-instrumentation shape that collapses tree/timeline layouts at extreme depth (LFE-10959).",
    supportsV4: true,
    flags: [
      flag(
        "observations",
        "number",
        1401,
        "chain length = tree depth (default mirrors the reported trace)",
      ),
      flag(
        "v4",
        "boolean",
        false,
        "also mirror the chain into v4 events_full/events_core",
      ),
    ],
    ...clickhouseOnly,
    load: async () => (await import("./deep-chain.js")).deepChainScenario,
  },
  "long-session": {
    name: "long-session",
    description:
      "One session with many traces for session-detail and virtualization work: mixed payload sizes (incl. 100KB outliers), long and unicode trace names, generation usage/cost, trace + session scores. Creates the required Postgres trace_sessions row.",
    supportsV4: true,
    flags: [
      flag("traces", "number", 120, "traces in the session"),
      flag("observations-per-trace", "number", 6, "observations per trace"),
      flag(
        "payload-bytes",
        "number",
        2_000,
        "approx max bytes for regular trace payloads (generation granularity is ~one paragraph/object, so very small values overshoot)",
      ),
      flag(
        "minutes",
        "number",
        180,
        "session time window ending at UTC midnight of today",
      ),
      flag("session-id", "string", "", "override the generated session id"),
      flag("v4", "boolean", false, "also mirror traces into v4 events tables"),
    ],
    ...clickhouseOnly,
    load: async () => (await import("./long-session.js")).longSessionScenario,
  },
  "many-traces": {
    name: "many-traces",
    description:
      "Bulk traces/observations/scores for trace-list and filter performance work: ClickHouse numbers() SQL, fast even for 100k+ traces, deterministic ids so re-runs do not duplicate.",
    supportsV4: false,
    flags: [
      flag("count", "number", 10_000, "number of traces"),
      flag("days", "number", 3, "spread timestamps over the past N days"),
      flag("observations-per-trace", "number", 5, "observations per trace"),
      flag("scores-per-trace", "number", 2, "scores per trace"),
      flag(
        "rich-payloads",
        "boolean",
        false,
        "embed bundled markdown/JSON fixtures as payloads",
      ),
    ],
    ...clickhouseOnly,
    load: async () => (await import("./many-traces.js")).manyTracesScenario,
  },
  "scored-traces": {
    name: "scored-traces",
    description:
      'Standalone traces each carrying numeric + categorical scores whose names contain SPACES (e.g. "Rouge Score", "Score With A Space") at both observation level (scores.) and trace level (traceScores.). For exercising the score filter sidebar and the grammar search bar with quoted score names.',
    supportsV4: true,
    flags: [
      flag("traces", "number", 24, "number of standalone traces to create"),
      flag(
        "v4",
        "boolean",
        false,
        "also mirror traces/observations into v4 events_full/events_core",
      ),
    ],
    ...clickhouseOnly,
    load: async () => (await import("./scored-traces.js")).scoredTracesScenario,
  },
  "session-shapes": {
    name: "session-shapes",
    description:
      "Diverse v4 session shapes for the session-detail view: a clean multi-turn CHAT session (renders as chat), a coding/AGENT session whose I/O lives on AGENT/TOOL observations with NO GENERATION (the default 'first generation' preset yields empty cards — LFE-10520), and a MIXED session. Creates the Postgres trace_sessions rows.",
    supportsV4: true,
    flags: [
      flag(
        "shape",
        "string",
        "all",
        "session shape: chat | agent | mixed | all",
      ),
      flag("turns", "number", 8, "traces (turns) per session"),
      flag(
        "v4",
        "boolean",
        true,
        "mirror into v4 events tables (on by default: v4-only surface)",
      ),
    ],
    ...clickhouseOnly,
    load: async () =>
      (await import("./session-shapes.js")).sessionShapesScenario,
  },
  "annotation-queue": {
    name: "annotation-queue",
    description:
      "Two human-annotation queues for testing the annotate UI keyboard-first: a 'core types' queue with one of every score-field render path (categorical toggle/combobox, boolean, ranged/decimal/unranged numeric, text) over fresh trace items, and an 'edge cases' queue adding archived/stale/partial scores, comments, and observation/session/deleted/completed items.",
    supportsV4: true,
    flags: [
      flag(
        "core-items",
        "number",
        12,
        "number of fresh trace items on the core-types queue",
      ),
      flag(
        "v4",
        "boolean",
        true,
        "mirror traces/observations into v4 events_full (on by default so they render on a v4 local dev instance)",
      ),
    ],
    ...clickhouseOnly,
    load: async () =>
      (await import("./annotation-queue.js")).annotationQueueScenario,
  },
  "support-agent": {
    name: "support-agent",
    description:
      "One demo-grade, fully handcrafted trace: a customer-support copilot resolving a duplicate-charge refund — input guardrail → intent classification → parallel context fan-out (CRM/billing/tickets) → 3-turn ReAct loop (llm.chat + Stripe tools) → drafted reply → output guardrail → send. Real-looking payloads, per-model token/cost numbers, deterministic timings. Built for videos/screenshots; exercises the graph view's Aggregated (llm.chat 3/3 loop) vs Expanded (as-it-ran DAG with fork/join) modes.",
    supportsV4: true,
    flags: [
      flag(
        "v4",
        "boolean",
        false,
        "also mirror into v4 events_full/events_core",
      ),
    ],
    ...clickhouseOnly,
    load: async () => (await import("./support-agent.js")).supportAgentScenario,
  },
};

export const listScenarioRegistrations = (
  backend: AnalyticsBackend,
  registry: ScenarioRegistry = scenarioRegistry,
) =>
  Object.values(registry).map((scenario) => ({
    name: scenario.name,
    description: scenario.description,
    supportsV4: scenario.supportsV4,
    flags: scenario.flags,
    target: backend,
    availability: scenario.supportedBackends.includes(backend)
      ? ("available" as const)
      : ("unavailable" as const),
    supportedBackends: scenario.supportedBackends,
  }));

export const resolveScenarioRegistration = (
  name: string,
  backend: AnalyticsBackend,
  registry: ScenarioRegistry = scenarioRegistry,
): ScenarioRegistration => {
  const scenario = Object.hasOwn(registry, name) ? registry[name] : undefined;
  if (!scenario) {
    throw new SeedError(
      `unknown scenario "${name}" — available: ${Object.keys(registry).join(", ")}, doctor, list`,
      "run `pnpm run seed -- list` to see scenarios and flags",
    );
  }
  if (!scenario.supportedBackends.includes(backend)) {
    throw new SeedError(
      `scenario "${name}" is unavailable for analytics backend "${backend}"`,
      backend === "doris"
        ? "run `pnpm run seed -- analytics-smoke`, or select ClickHouse for legacy seed scenarios"
        : "run `pnpm run seed -- list` to see backend availability",
    );
  }
  return scenario;
};

export const loadScenarioDefinition = async (
  registration: ScenarioRegistration,
): Promise<ScenarioDefinition> => {
  const scenario = await registration.load();
  if (scenario.name !== registration.name) {
    throw new SeedError(
      `scenario registry mismatch: expected "${registration.name}", loaded "${scenario.name}"`,
    );
  }
  return scenario;
};

function flag(
  name: string,
  type: ScenarioFlag["type"],
  defaultValue: ScenarioFlag["default"],
  description: string,
): ScenarioFlag {
  return { flag: name, type, default: defaultValue, description };
}

export * from "./types";
