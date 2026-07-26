import { describe, expect, it } from "vitest";

import {
  ANALYTICS_CAPABILITY_CATALOG,
  ANALYTICS_CAPABILITY_NAMES,
  STATIC_SYNCHRONOUS_ANALYTICS_FEATURES,
} from "./analyticsCapabilities";

describe("analytics capability activation catalog", () => {
  it("contains exactly the six cross-process capabilities", () => {
    expect(ANALYTICS_CAPABILITY_NAMES).toEqual([
      "coreBatchExports",
      "evaluations",
      "experiments",
      "datasetRunExports",
      "datasetRunIngestion",
      "analyticsIntegrations",
    ]);
    expect(Object.keys(ANALYTICS_CAPABILITY_CATALOG)).toEqual(
      ANALYTICS_CAPABILITY_NAMES,
    );
  });

  it("keeps every Doris capability disabled in the foundation rollout", () => {
    expect(
      Object.values(ANALYTICS_CAPABILITY_CATALOG).map(
        ({ initialDorisStatus }) => initialDorisStatus,
      ),
    ).toEqual(Array(6).fill("DISABLED"));
  });

  it("assigns one owner unit and explicit web/worker contracts", () => {
    expect(
      Object.fromEntries(
        Object.entries(ANALYTICS_CAPABILITY_CATALOG).map(([name, value]) => [
          name,
          value.ownerUnit,
        ]),
      ),
    ).toEqual({
      coreBatchExports: "U2",
      evaluations: "U5",
      experiments: "U6",
      datasetRunExports: "U6",
      datasetRunIngestion: "U6",
      analyticsIntegrations: "U7",
    });

    for (const contract of Object.values(ANALYTICS_CAPABILITY_CATALOG)) {
      expect(contract.contractVersion).toBe(1);
      expect(contract.minimumRuntimeContract).toBe(1);
      expect(contract.components.web.length).toBeGreaterThan(0);
      expect(contract.components.worker.length).toBeGreaterThan(0);
    }
  });

  it("records the U6 activation dependencies and worker-side ingestion producer", () => {
    expect(
      Object.fromEntries(
        Object.entries(ANALYTICS_CAPABILITY_CATALOG).map(([name, value]) => [
          name,
          value.dependencies,
        ]),
      ),
    ).toEqual({
      coreBatchExports: [],
      evaluations: [],
      experiments: ["datasetRunIngestion"],
      datasetRunExports: ["coreBatchExports"],
      datasetRunIngestion: [],
      analyticsIntegrations: [],
    });
    expect(
      ANALYTICS_CAPABILITY_CATALOG.datasetRunIngestion.components.worker,
    ).toContain("producer");
    expect(
      ANALYTICS_CAPABILITY_CATALOG.coreBatchExports.components.worker,
    ).toEqual(["consumer", "recovery"]);
  });

  it("does not put static synchronous features behind activation rows", () => {
    expect(STATIC_SYNCHRONOUS_ANALYTICS_FEATURES).toEqual([
      "coreIngestion",
      "coreReads",
      "queryEngine",
      "monitors",
      "customDashboards",
    ]);
    expect(
      STATIC_SYNCHRONOUS_ANALYTICS_FEATURES.filter((feature) =>
        ANALYTICS_CAPABILITY_NAMES.includes(feature as never),
      ),
    ).toEqual([]);
  });
});
