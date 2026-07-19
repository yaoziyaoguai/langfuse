import CallbackHandler from "langfuse-langchain";
import { ProcessedTraceEvent, TraceSinkParams } from "./types";
import { buildInternalTraceEventInputs } from "./internalTraceEvents";
import { writeInternalTraceViaOtelIngestion } from "../otel/internalTraceOtelWriter";
import { logger } from "../logger";
import { traceException } from "../instrumentation";

export function prepareInternalTraceEvents(params: {
  events: Array<{
    type: string;
    timestamp: string;
    body: Record<string, unknown>;
  }>;
  environment: string;
  prompt?: TraceSinkParams["prompt"];
}): ProcessedTraceEvent[] {
  const { events, environment, prompt } = params;

  const blockedSpanIds = new Set();
  const blockedSpanNameSubstrings = ["RunnableLambda", "OutputParser"];

  for (const event of events) {
    const eventName = "name" in event.body ? event.body.name : "";

    if (typeof eventName !== "string" || eventName.length === 0) {
      continue;
    }

    if (
      blockedSpanNameSubstrings.some((blockedSubstring) =>
        eventName.includes(blockedSubstring),
      ) &&
      "id" in event.body &&
      event.type !== "trace-create"
    ) {
      blockedSpanIds.add(event.body.id);
    }
  }

  return events
    .filter((event) => {
      if ("id" in event.body) {
        return !blockedSpanIds.has(event.body.id);
      }

      return true;
    })
    .map((event) => {
      // Inject environment into all events
      return {
        ...event,
        body: {
          ...event.body,
          environment,
        },
      };
    })
    .map((event) => {
      if (event.type === "generation-create" && prompt) {
        return {
          ...event,
          body: {
            ...event.body,
            promptName: prompt.name,
            promptVersion: prompt.version,
          },
        };
      }

      return event;
    });
}

export function getInternalTracingHandler(traceSinkParams: TraceSinkParams): {
  handler: CallbackHandler;
  processTracedEvents: () => Promise<void>;
} {
  const { prompt, targetProjectId, environment, userId } = traceSinkParams;
  const handler = new CallbackHandler({
    _projectId: targetProjectId,
    _isLocalEventExportEnabled: true,
    environment: environment,
    userId: userId,
  });

  const processTracedEvents = async () => {
    try {
      const events = await handler.langfuse._exportLocalEvents(
        traceSinkParams.targetProjectId,
      );
      const processedEvents = prepareInternalTraceEvents({
        events,
        environment,
        prompt,
      });

      try {
        const { rootSpanId, eventInputs } = buildInternalTraceEventInputs({
          processedEvents,
          traceId: traceSinkParams.traceId,
          projectId: targetProjectId,
        });
        if (eventInputs.length > 0) {
          await writeInternalTraceViaOtelIngestion({
            rootSpanId,
            eventInputs,
          });
        }
      } catch (writeError) {
        traceException(writeError);
        logger.error("Failed to publish internal traced events", {
          error: writeError,
        });
      }
    } catch (e) {
      logger.error("Failed to process traced events", { error: e });
    }
  };

  return { handler, processTracedEvents };
}
