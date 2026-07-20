import type { FullEventsObservation } from "@langfuse/shared/src/server";
import { describe, expect, it } from "vitest";

import { mapObservationToBatchExportEvent } from "./mapObservationToBatchExportEvent";

describe("mapObservationToBatchExportEvent", () => {
  it("preserves the events export contract and converts seconds to milliseconds", () => {
    const observation = {
      id: "span-1",
      traceId: "trace-1",
      traceName: "trace",
      type: "GENERATION",
      name: null,
      startTime: new Date("2026-07-20T00:00:00.000Z"),
      endTime: null,
      completionStartTime: null,
      environment: "production",
      version: null,
      userId: "user-1",
      sessionId: "session-1",
      level: "DEFAULT",
      statusMessage: null,
      promptName: null,
      promptId: null,
      promptVersion: null,
      internalModelId: "model-1",
      model: "gpt-test",
      modelParameters: null,
      usageDetails: { input: 10 },
      costDetails: { total: 0.1 },
      totalCost: 0.1,
      input: { question: "hello" },
      output: { answer: "world" },
      metadata: { region: "eu" },
      latency: 2.5,
      timeToFirstToken: 0.25,
      traceTags: ["prod"],
      release: "release-1",
      parentObservationId: null,
    } as FullEventsObservation;

    expect(
      mapObservationToBatchExportEvent(observation, { quality: [0.9] }, [
        { id: "comment-1" },
      ]),
    ).toEqual(
      expect.objectContaining({
        id: "span-1",
        traceId: "trace-1",
        name: "",
        modelId: "model-1",
        providedModelName: "gpt-test",
        latencyMs: 2_500,
        timeToFirstTokenMs: 250,
        tags: ["prod"],
        scores: { quality: [0.9] },
        comments: [{ id: "comment-1" }],
      }),
    );
  });

  it("rejects an event without a trace ID instead of exporting corrupt data", () => {
    expect(() =>
      mapObservationToBatchExportEvent(
        { traceId: null } as FullEventsObservation,
        {},
        [],
      ),
    ).toThrow("Batch export event is missing its trace ID");
  });
});
