import { describe, expect, it, vi } from "vitest";

import {
  getActiveDorisCommunityCapabilities,
  isInternalDorisCapabilityActive,
  isCommunityCapabilityRuntimeAvailable,
} from "./communityCapabilityRuntime";

function client(input: {
  backend?: "DORIS" | "CLICKHOUSE" | null;
  deploymentGeneration?: bigint;
  activeCapabilities?: readonly ("EVALUATIONS" | "EXPERIMENTS")[];
  internalActivation?: {
    backend: "DORIS" | "CLICKHOUSE";
    deploymentGeneration: bigint;
    status: "ACTIVE" | "DARK";
  } | null;
}) {
  return {
    analyticsBackendDeploymentState: {
      findUnique: vi.fn().mockResolvedValue(
        input.backend
          ? {
              backend: input.backend,
              generation: input.deploymentGeneration ?? 1n,
            }
          : null,
      ),
    },
    analyticsCapabilityActivation: {
      findMany: vi.fn().mockResolvedValue(
        (input.activeCapabilities ?? []).map((capability) => ({
          capability,
        })),
      ),
      findUnique: vi.fn().mockResolvedValue(input.internalActivation ?? null),
    },
  };
}

describe("community capability runtime availability", () => {
  it("requires the current Doris deployment activation row", async () => {
    const activeClient = client({
      backend: "DORIS",
      activeCapabilities: ["EVALUATIONS", "EXPERIMENTS"],
    });
    await expect(
      getActiveDorisCommunityCapabilities(activeClient as never),
    ).resolves.toEqual(["evaluations", "experiments"]);
    expect(
      activeClient.analyticsCapabilityActivation.findMany,
    ).toHaveBeenCalledWith({
      where: {
        capability: { in: ["EVALUATIONS", "EXPERIMENTS"] },
        backend: "DORIS",
        deploymentGeneration: 1n,
        status: "ACTIVE",
      },
      select: { capability: true },
    });
  });

  it("fails closed when the marker is absent or not Doris", async () => {
    await expect(
      getActiveDorisCommunityCapabilities(client({ backend: null }) as never),
    ).resolves.toEqual([]);
    await expect(
      getActiveDorisCommunityCapabilities(
        client({
          backend: "CLICKHOUSE",
          activeCapabilities: ["EVALUATIONS"],
        }) as never,
      ),
    ).resolves.toEqual([]);
  });

  it("preserves ClickHouse and static capability behavior", async () => {
    await expect(
      isCommunityCapabilityRuntimeAvailable(
        "evaluations",
        "clickhouse",
        client({ backend: null }) as never,
      ),
    ).resolves.toBe(true);
    await expect(
      isCommunityCapabilityRuntimeAvailable(
        "experiments",
        "doris",
        client({
          backend: "DORIS",
          activeCapabilities: ["EXPERIMENTS"],
        }) as never,
      ),
    ).resolves.toBe(true);
    await expect(
      isCommunityCapabilityRuntimeAvailable(
        "monitors",
        "doris",
        client({ backend: "DORIS" }) as never,
      ),
    ).resolves.toBe(true);
  });

  it("checks dataset-run ingestion against the current Doris generation", async () => {
    await expect(
      isInternalDorisCapabilityActive(
        "datasetRunIngestion",
        client({
          backend: "DORIS",
          deploymentGeneration: 5n,
          internalActivation: {
            backend: "DORIS",
            deploymentGeneration: 5n,
            status: "ACTIVE",
          },
        }) as never,
      ),
    ).resolves.toBe(true);
    await expect(
      isInternalDorisCapabilityActive(
        "datasetRunIngestion",
        client({
          backend: "DORIS",
          deploymentGeneration: 5n,
          internalActivation: {
            backend: "DORIS",
            deploymentGeneration: 4n,
            status: "ACTIVE",
          },
        }) as never,
      ),
    ).resolves.toBe(false);
  });
});
