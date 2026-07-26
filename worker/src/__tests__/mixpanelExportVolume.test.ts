import { describe, it, expect, vi, beforeEach, afterEach } from "vitest";
import { gzipSync } from "zlib";
import { logger } from "@langfuse/shared/src/server";
import { MixpanelClient } from "../features/mixpanel/mixpanelClient";
import type { MixpanelEvent } from "../features/mixpanel/transformers";

describe("MixpanelClient export volume", () => {
  const fetchMock = vi.fn();

  beforeEach(() => {
    fetchMock.mockReset();
    fetchMock.mockResolvedValue({ ok: true, json: async () => ({}) });
    vi.stubGlobal("fetch", fetchMock);
  });

  afterEach(() => {
    vi.unstubAllGlobals();
    vi.restoreAllMocks();
  });

  it("accumulates gzipped on-wire bytes across sendBatch chunks", async () => {
    const client = new MixpanelClient({ projectToken: "t", region: "api" });

    // > batchSize (1000) so flush() splits into two sendBatch chunks.
    const total = 1500;
    const events: MixpanelEvent[] = Array.from(
      { length: total },
      (_, i) =>
        ({
          event: "trace",
          properties: { token: "t", distinct_id: String(i), $insert_id: i },
        }) as unknown as MixpanelEvent,
    );
    events.forEach((e) => client.addEvent(e));

    await client.flush();

    const expected =
      gzipSync(JSON.stringify(events.slice(0, 1000))).length +
      gzipSync(JSON.stringify(events.slice(1000))).length;

    expect(fetchMock).toHaveBeenCalledTimes(2);
    expect(client.getSerializedBytes()).toBe(expected);
  });

  it("preserves the legacy partial-import behavior by default", async () => {
    fetchMock.mockResolvedValue({
      ok: false,
      status: 400,
      statusText: "Bad Request",
      text: async () =>
        JSON.stringify({
          num_records_imported: 1,
          failed_records: [
            {
              index: 1,
              insert_id: "failed-id",
              field: "event",
              message: "fixture rejection",
            },
          ],
        }),
    });
    const client = new MixpanelClient({ projectToken: "t", region: "api" });
    client.addEvent({
      event: "trace",
      properties: {
        time: 0,
        distinct_id: "user-1",
        $insert_id: "event-1",
      },
    });

    await expect(client.flush()).resolves.toBeUndefined();
    expect(client.getBatchSize()).toBe(0);
  });

  it("does not log a remote response body in managed redaction mode", async () => {
    const secret = "Authorization=Bearer remote-secret";
    fetchMock.mockResolvedValue({
      ok: false,
      status: 400,
      statusText: "Bad Request",
      text: async () => secret,
    });
    const errorLog = vi.spyOn(logger, "error");
    const client = new MixpanelClient({
      projectToken: "t",
      region: "api",
      allowPartialSuccess: false,
      redactErrors: true,
    });
    client.addEvent({
      event: "trace",
      properties: {
        time: 0,
        distinct_id: "user-1",
        $insert_id: "event-1",
      },
    });

    await expect(client.flush()).rejects.toThrow("Mixpanel API error: 400");
    expect(JSON.stringify(errorLog.mock.calls)).not.toContain(secret);
  });
});
