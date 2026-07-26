import { describe, expect, it } from "vitest";
import {
  BATCH_EXPORT_QUEUE_ATTEMPTS,
  BATCH_EXPORT_QUEUE_BACKOFF_DELAY_MS,
} from "@langfuse/shared/src/server";

import {
  BATCH_EXPORT_EXECUTION_LEASE_MS,
  BATCH_EXPORT_LEASE_HEARTBEAT_MS,
  BATCH_EXPORT_MANIFEST_LEASE_MS,
} from "./BatchExportLeasePolicy";

describe("batch export lease policy", () => {
  it("allows a BullMQ retry to recover a crashed execution before attempts exhaust", () => {
    const retryStartOffsets = Array.from(
      { length: BATCH_EXPORT_QUEUE_ATTEMPTS - 1 },
      (_, index) =>
        Array.from(
          { length: index + 1 },
          (_value, delayIndex) =>
            BATCH_EXPORT_QUEUE_BACKOFF_DELAY_MS * 2 ** delayIndex,
        ).reduce((total, delay) => total + delay, 0),
    );

    expect(
      retryStartOffsets.some(
        (offset) =>
          offset > BATCH_EXPORT_EXECUTION_LEASE_MS &&
          offset < retryStartOffsets.at(-1)!,
      ),
    ).toBe(true);
    expect(BATCH_EXPORT_LEASE_HEARTBEAT_MS * 2).toBeLessThan(
      BATCH_EXPORT_EXECUTION_LEASE_MS,
    );
    expect(BATCH_EXPORT_MANIFEST_LEASE_MS).toBeLessThan(
      retryStartOffsets.at(-1)!,
    );
  });
});
