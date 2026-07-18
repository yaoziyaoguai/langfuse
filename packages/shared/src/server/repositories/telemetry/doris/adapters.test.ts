import { describe, expect, it } from "vitest";

import { projectDorisObservation, toDorisTraceDomain } from "./adapters";
import type { DorisObservation } from "./observations";
import type { DorisTrace } from "./traces";

const observation = {
  id: "span-1",
  traceId: "trace-1",
  projectId: "project-1",
  partitionDate: "2026-07-17",
  parentObservationId: null,
  type: "GENERATION",
  name: "generation",
  environment: "production",
  userId: "user-1",
  sessionId: "session-1",
  traceName: "trace",
  release: "release-1",
  version: "version-1",
  level: "DEFAULT",
  statusMessage: null,
  isAppRoot: true,
  bookmarked: false,
  public: false,
  startTime: new Date("2026-07-17T10:00:00.000Z"),
  endTime: new Date("2026-07-17T10:00:02.000Z"),
  completionStartTime: new Date("2026-07-17T10:00:00.500Z"),
  createdAt: new Date("2026-07-17T10:00:00.000Z"),
  updatedAt: new Date("2026-07-17T10:00:02.000Z"),
  providedModelName: "gpt-test",
  internalModelId: "model-1",
  promptId: "prompt-1",
  promptName: "prompt",
  promptVersion: 1,
  totalInputTokens: 10,
  totalOutputTokens: 5,
  totalUsage: 15,
  totalCost: 0.125,
  latency: 2,
  timeToFirstToken: 0.5,
  tags: ["prod"],
  usageDetails: { input: 10, output: 5, total: 15 },
  costDetails: { input: 0.1, output: 0.025, total: 0.125 },
  providedUsageDetails: {},
  providedCostDetails: {},
  toolDefinitionsCount: 1,
  toolCallsCount: 1,
  inputPreview: "input preview",
  outputPreview: "output preview",
  input: { question: "price" },
  output: { answer: "ok" },
  metadata: { region: "eu" },
  modelParameters: { temperature: 0 },
  toolDefinitions: { search: "{}" },
  toolCalls: ["search"],
  toolCallNames: ["search"],
} satisfies DorisObservation;

describe("Doris telemetry domain adapters", () => {
  it("projects only requested public observation groups", () => {
    const compact = projectDorisObservation(observation, ["core", "basic"]);
    const full = projectDorisObservation(observation, [
      "core",
      "io",
      "metadata",
      "model",
      "usage",
      "metrics",
    ]);

    expect(compact).toMatchObject({
      id: "span-1",
      traceId: "trace-1",
      name: "generation",
      modelId: null,
    });
    expect(compact).not.toHaveProperty("input");
    expect(compact).not.toHaveProperty("metadata");
    expect(full).toMatchObject({
      input: { question: "price" },
      metadata: { region: "eu" },
      model: "gpt-test",
      inputUsage: 10,
      inputCost: 0.1,
      latency: 2,
    });
  });

  it("keeps trace control fields out of Doris and applies service values", () => {
    const trace = {
      id: "trace-1",
      projectId: "project-1",
      timestamp: new Date("2026-07-17T10:00:00.000Z"),
      endTime: new Date("2026-07-17T10:00:02.000Z"),
      name: "trace",
      environment: "production",
      userId: "user-1",
      sessionId: "session-1",
      release: null,
      version: null,
      tags: ["prod"],
      inputPreview: "input preview",
      outputPreview: "output preview",
      input: { question: "full" },
      output: { answer: "full" },
      metadata: { region: "eu" },
      rootObservationId: "span-1",
      fallbackObservationId: "span-1",
      incomplete: false,
      observationCount: 1,
      totalInputTokens: 10,
      totalOutputTokens: 5,
      totalUsage: 15,
      totalCost: 0.125,
      latency: 2,
    } satisfies DorisTrace;

    expect(
      toDorisTraceDomain(trace, { bookmarked: true, public: false }),
    ).toMatchObject({
      id: "trace-1",
      bookmarked: true,
      public: false,
      input: { question: "full" },
      output: { answer: "full" },
      metadata: { region: "eu" },
    });
  });
});
