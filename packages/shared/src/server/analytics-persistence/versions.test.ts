import { describe, expect, it } from "vitest";

import {
  ANALYTICS_CONTRACT_COMPATIBILITY,
  CURRENT_ANALYTICS_CANONICALIZER_VERSION,
  CURRENT_ANALYTICS_SCHEMA_VERSION,
  defineAdjacentAnalyticsContractWindow,
  NEXT_ANALYTICS_SCHEMA_VERSION,
  PREVIOUS_ANALYTICS_CANONICALIZER_VERSION,
} from "./versions";

describe("analytics contract compatibility window", () => {
  it("keeps schema 1 as writer default while Release A reads schema 2", () => {
    expect(ANALYTICS_CONTRACT_COMPATIBILITY).toMatchObject({
      writerSchemaVersion: CURRENT_ANALYTICS_SCHEMA_VERSION,
      readableSchemaVersions: [
        CURRENT_ANALYTICS_SCHEMA_VERSION,
        NEXT_ANALYTICS_SCHEMA_VERSION,
      ],
    });
  });

  it("fences canonicalizer v2 writes from v1-only workers while retaining v1 replay", () => {
    expect(ANALYTICS_CONTRACT_COMPATIBILITY).toMatchObject({
      writerCanonicalizerVersion: CURRENT_ANALYTICS_CANONICALIZER_VERSION,
      readableCanonicalizerVersions: [
        PREVIOUS_ANALYTICS_CANONICALIZER_VERSION,
        CURRENT_ANALYTICS_CANONICALIZER_VERSION,
      ],
    });
  });

  it("keeps the writer on the old contract while Release A reads one adjacent version", () => {
    expect(
      defineAdjacentAnalyticsContractWindow({
        writerSchemaVersion: 2,
        readableSchemaVersions: [2, 3],
        writerCanonicalizerVersion: "4",
        readableCanonicalizerVersions: ["4", "5"],
      }),
    ).toEqual({
      writerSchemaVersion: 2,
      readableSchemaVersions: [2, 3],
      writerCanonicalizerVersion: "4",
      readableCanonicalizerVersions: ["4", "5"],
    });

    expect(
      defineAdjacentAnalyticsContractWindow({
        writerSchemaVersion: 3,
        readableSchemaVersions: [2, 3],
        writerCanonicalizerVersion: "5",
        readableCanonicalizerVersions: ["4", "5"],
      }),
    ).toMatchObject({
      writerSchemaVersion: 3,
      writerCanonicalizerVersion: "5",
    });

    expect(() =>
      defineAdjacentAnalyticsContractWindow({
        writerSchemaVersion: 3,
        readableSchemaVersions: [1, 3],
        writerCanonicalizerVersion: "5",
        readableCanonicalizerVersions: ["4", "5"],
      }),
    ).toThrow(/adjacent/i);
    expect(() =>
      defineAdjacentAnalyticsContractWindow({
        writerSchemaVersion: 3,
        readableSchemaVersions: [2, 3],
        writerCanonicalizerVersion: "6",
        readableCanonicalizerVersions: ["4", "5"],
      }),
    ).toThrow(/writer/i);
  });
});
