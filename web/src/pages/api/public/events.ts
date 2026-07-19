import {
  PostEventsV1Body,
  PostEventsV1Response,
} from "@/src/features/public-api/types/events";
import { withMiddlewares } from "@/src/features/public-api/server/withMiddlewares";
import { createAuthedProjectAPIRoute } from "@/src/features/public-api/server/createAuthedProjectAPIRoute";
import {
  createIngestionAttribution,
  eventTypes,
  logger,
  processEventBatch,
} from "@langfuse/shared/src/server";
import { v4 } from "uuid";

export default withMiddlewares({
  POST: createAuthedProjectAPIRoute({
    name: "Create Event",
    bodySchema: PostEventsV1Body,
    responseSchema: PostEventsV1Response,
    // The compatibility route remains registered but processEventBatch
    // returns a structured 501; R1A tracing ingestion uses OTLP.
    fn: async ({ body, auth, req, res }) => {
      const event = {
        id: v4(),
        type: eventTypes.OBSERVATION_CREATE,
        timestamp: new Date().toISOString(),
        body: {
          ...body,
          type: "EVENT",
        },
      };
      if (!event.body.id) {
        event.body.id = v4();
      }
      const result = await processEventBatch([event], auth, {
        attribution: createIngestionAttribution({
          headers: req.headers,
          authCheck: auth,
        }),
      });
      if (result.errors.length > 0) {
        const error = result.errors[0];
        res.status(error.status).json({
          error: error.error,
          code: error.code,
          message: error.message,
          recovery: error.recovery,
        });
        return { id: "" }; // dummy return
      }
      if (result.successes.length !== 1) {
        logger.error("Failed to create event", { result });
        throw new Error("Failed to create event");
      }
      return { id: event.body.id };
    },
  }),
});
