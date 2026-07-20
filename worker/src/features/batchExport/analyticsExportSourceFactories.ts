import { getEventsStream } from "../database-read-stream/event-stream";
import { getDatabaseReadStreamPaginated } from "../database-read-stream/getDatabaseReadStream";
import { getObservationStream } from "../database-read-stream/observation-stream";
import { getTraceStream } from "../database-read-stream/trace-stream";
import { ClickHouseAnalyticsExportSource } from "./ClickHouseAnalyticsExportSource";
import { DorisAnalyticsExportSource } from "./DorisAnalyticsExportSource";

export function createClickHouseAnalyticsExportSource() {
  return new ClickHouseAnalyticsExportSource({
    observations: getObservationStream,
    traces: getTraceStream,
    events: getEventsStream,
    paginated: getDatabaseReadStreamPaginated,
  });
}

export function createDorisAnalyticsExportSource() {
  return new DorisAnalyticsExportSource({
    paginated: getDatabaseReadStreamPaginated,
  });
}
