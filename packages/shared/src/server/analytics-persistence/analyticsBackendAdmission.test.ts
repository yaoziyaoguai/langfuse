import { describe, expect, it } from "vitest";

import {
  analyticsDurableProvenanceMatches,
  analyticsProducerProvenanceFromAdmission,
  deserializeAnalyticsDurableProvenance,
  serializeAnalyticsDurableProvenance,
} from "./analyticsDurableProvenance";
import { runtimeLeaseAcceptsAnalyticsContract } from "./analyticsBackendAdmission";

const epoch = "a".repeat(64);

describe("analytics durable provenance", () => {
  it("freezes the original producer identity from producer admission", () => {
    expect(
      analyticsProducerProvenanceFromAdmission({
        analyticsBackend: "DORIS",
        deploymentGeneration: 3n,
        workloadEpochFingerprint: epoch,
        runtimeContractVersion: 2,
        admittingRuntimeLeaseId: "web-producer-lease",
        admittedAt: new Date("2026-07-21T18:00:00.000Z"),
      }),
    ).toEqual({
      analyticsBackend: "DORIS",
      deploymentGeneration: 3n,
      workloadEpochFingerprint: epoch,
      runtimeContractVersion: 2,
      producerRuntimeLeaseId: "web-producer-lease",
    });
  });

  it("round-trips a positive generation without losing bigint precision", () => {
    const provenance = {
      analyticsBackend: "CLICKHOUSE" as const,
      deploymentGeneration: 9_007_199_254_740_993n,
      workloadEpochFingerprint: epoch,
      runtimeContractVersion: 1,
      producerRuntimeLeaseId: "producer-lease",
    };

    const serialized = serializeAnalyticsDurableProvenance(provenance);
    expect(serialized.deploymentGeneration).toBe("9007199254740993");
    expect(deserializeAnalyticsDurableProvenance(serialized)).toEqual(
      provenance,
    );
  });

  it("round-trips capability activation provenance for durable feature work", () => {
    const provenance = analyticsProducerProvenanceFromAdmission(
      {
        analyticsBackend: "DORIS",
        deploymentGeneration: 7n,
        workloadEpochFingerprint: epoch,
        runtimeContractVersion: 2,
        admittingRuntimeLeaseId: "web-producer-lease",
        admittedAt: new Date("2026-07-21T18:00:00.000Z"),
        capabilityActivationGeneration: 3n,
        capabilityContractVersion: 1,
      },
      "datasetRunIngestion",
    );

    expect(
      deserializeAnalyticsDurableProvenance(
        serializeAnalyticsDurableProvenance(provenance),
      ),
    ).toEqual(provenance);
    expect(
      analyticsDurableProvenanceMatches(provenance, {
        ...provenance,
        capabilityActivationGeneration: 4n,
      }),
    ).toBe(false);
  });

  it("rejects malformed serialized provenance", () => {
    expect(() =>
      deserializeAnalyticsDurableProvenance({
        analyticsBackend: "DORIS",
        deploymentGeneration: "0",
        workloadEpochFingerprint: epoch,
        runtimeContractVersion: 1,
        producerRuntimeLeaseId: "producer-lease",
      }),
    ).toThrow(/provenance/i);
    expect(() =>
      deserializeAnalyticsDurableProvenance({
        analyticsBackend: "DORIS",
        deploymentGeneration: "01",
        workloadEpochFingerprint: epoch,
        runtimeContractVersion: 1,
        producerRuntimeLeaseId: "producer-lease",
      }),
    ).toThrow(/provenance/i);
    expect(() =>
      deserializeAnalyticsDurableProvenance({
        analyticsBackend: "DORIS",
        capability: "datasetRunIngestion",
        deploymentGeneration: "1",
        workloadEpochFingerprint: epoch,
        runtimeContractVersion: 1,
        producerRuntimeLeaseId: "producer-lease",
      }),
    ).toThrow(/provenance/i);
  });

  it("compares the original producer as part of the durable fence", () => {
    const left = {
      analyticsBackend: "DORIS" as const,
      deploymentGeneration: 1n,
      workloadEpochFingerprint: epoch,
      runtimeContractVersion: 1,
      producerRuntimeLeaseId: "producer-a",
    };
    expect(
      analyticsDurableProvenanceMatches(left, {
        ...left,
        producerRuntimeLeaseId: "producer-b",
      }),
    ).toBe(false);
  });
});

describe("analytics runtime contract admission", () => {
  const lease = {
    acceptedSchemaVersionMin: 2,
    acceptedSchemaVersionMax: 3,
    acceptedCanonicalVersionMin: 4,
    acceptedCanonicalVersionMax: 5,
  };

  it("accepts only versions advertised by the runtime lease", () => {
    expect(
      runtimeLeaseAcceptsAnalyticsContract(lease, {
        schemaVersion: 2,
        canonicalizerVersion: "5",
      }),
    ).toBe(true);
    expect(
      runtimeLeaseAcceptsAnalyticsContract(lease, {
        schemaVersion: 1,
        canonicalizerVersion: "5",
      }),
    ).toBe(false);
    expect(
      runtimeLeaseAcceptsAnalyticsContract(lease, {
        schemaVersion: 2,
        canonicalizerVersion: "r1a-v5",
      }),
    ).toBe(false);
  });
});
