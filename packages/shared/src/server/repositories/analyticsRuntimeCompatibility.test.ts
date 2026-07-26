import { describe, expect, it } from "vitest";

import { evaluateAnalyticsContractRollout } from "./analyticsRuntimeLeases";

const now = new Date("2026-07-21T12:00:00.000Z");

function lease(
  overrides: Partial<
    Parameters<typeof evaluateAnalyticsContractRollout>[0]["leases"][number]
  > = {},
) {
  return {
    component: "WEB" as const,
    instanceId: "web-1",
    buildId: "release-a",
    acceptedSchemaVersionMin: 1,
    acceptedSchemaVersionMax: 2,
    acceptedCanonicalVersionMin: 1,
    acceptedCanonicalVersionMax: 2,
    state: "ACTIVE" as const,
    leaseExpiresAt: new Date(now.getTime() + 60_000),
    supersededAt: null,
    quiescedAt: null,
    ...overrides,
  };
}

describe("analytics contract rollout gate", () => {
  it("requires an exact compatible live inventory and a quiesced rollback drill", () => {
    const compatible = [
      lease(),
      lease({ component: "WORKER", instanceId: "worker-1" }),
      lease({
        instanceId: "rollback-web",
        buildId: "rollback-a",
        state: "QUIESCED",
        leaseExpiresAt: new Date(now.getTime() - 1),
        quiescedAt: new Date(now.getTime() - 1_000),
      }),
      lease({
        component: "WORKER",
        instanceId: "rollback-worker",
        buildId: "rollback-a",
        state: "QUIESCED",
        leaseExpiresAt: new Date(now.getTime() - 1),
        quiescedAt: new Date(now.getTime() - 1_000),
      }),
    ];
    const input = {
      leases: compatible,
      now,
      expectedRuntimeInstanceIds: ["web-1", "worker-1"],
      requiredSchemaVersions: [1, 2],
      requiredCanonicalVersions: [1, 2],
      rollbackBuildId: "rollback-a",
    } as const;

    expect(evaluateAnalyticsContractRollout(input)).toEqual({
      ready: true,
      reasonCode: null,
    });
    expect(
      evaluateAnalyticsContractRollout({
        ...input,
        leases: compatible.map((item) =>
          item.instanceId === "worker-1"
            ? { ...item, acceptedSchemaVersionMax: 1 }
            : item,
        ),
      }),
    ).toEqual({ ready: false, reasonCode: "LIVE_RUNTIME_INCOMPATIBLE" });
    expect(
      evaluateAnalyticsContractRollout({
        ...input,
        leases: compatible.map((item) =>
          item.instanceId === "web-1" ? { ...item, leaseExpiresAt: now } : item,
        ),
      }),
    ).toEqual({
      ready: false,
      reasonCode: "LIVE_RUNTIME_INVENTORY_MISMATCH",
    });
    expect(
      evaluateAnalyticsContractRollout({
        ...input,
        leases: compatible.filter(
          ({ instanceId }) => instanceId !== "rollback-worker",
        ),
      }),
    ).toEqual({ ready: false, reasonCode: "ROLLBACK_NOT_ATTESTED" });
    expect(
      evaluateAnalyticsContractRollout({
        ...input,
        leases: compatible.map((item) =>
          item.buildId === "rollback-a"
            ? { ...item, quiescedAt: new Date(now.getTime() + 1) }
            : item,
        ),
      }),
    ).toEqual({ ready: false, reasonCode: "ROLLBACK_NOT_ATTESTED" });
    expect(
      evaluateAnalyticsContractRollout({
        ...input,
        rollbackBuildId: "untested-rollback",
      }),
    ).toEqual({ ready: false, reasonCode: "ROLLBACK_NOT_ATTESTED" });
  });
});
