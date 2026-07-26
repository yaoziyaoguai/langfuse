import type { AnalyticsCapability } from "@prisma/client";

import type { AnalyticsAdmissionStamp } from "./analyticsBackendAdmission";
import {
  ANALYTICS_CAPABILITY_NAMES,
  type AnalyticsCapabilityName,
} from "./analyticsCapabilities";
import { fromPrismaAnalyticsCapability } from "./analyticsBackendMapping";

type AnalyticsFoundationProvenance = {
  readonly analyticsBackend: "CLICKHOUSE" | "DORIS";
  readonly deploymentGeneration: bigint;
  readonly workloadEpochFingerprint: string;
  readonly runtimeContractVersion: number;
  readonly producerRuntimeLeaseId: string;
};

export type AnalyticsDurableProvenance = AnalyticsFoundationProvenance & {
  readonly capability?: AnalyticsCapabilityName;
  readonly capabilityActivationGeneration?: bigint;
  readonly capabilityContractVersion?: number;
};

export type SerializedAnalyticsDurableProvenance = Omit<
  AnalyticsFoundationProvenance,
  "deploymentGeneration"
> & {
  readonly deploymentGeneration: string;
  readonly capability?: AnalyticsCapabilityName;
  readonly capabilityActivationGeneration?: string;
  readonly capabilityContractVersion?: number;
};

const SHA256_HEX = /^[a-f0-9]{64}$/;
const POSITIVE_INTEGER = /^[1-9][0-9]*$/;
const SERIALIZED_FOUNDATION_PROVENANCE_KEYS = [
  "analyticsBackend",
  "deploymentGeneration",
  "producerRuntimeLeaseId",
  "runtimeContractVersion",
  "workloadEpochFingerprint",
] as const;
const SERIALIZED_CAPABILITY_PROVENANCE_KEYS = [
  "analyticsBackend",
  "capability",
  "capabilityActivationGeneration",
  "capabilityContractVersion",
  "deploymentGeneration",
  "producerRuntimeLeaseId",
  "runtimeContractVersion",
  "workloadEpochFingerprint",
] as const;

function isAnalyticsCapabilityName(
  value: unknown,
): value is AnalyticsCapabilityName {
  return (
    typeof value === "string" &&
    ANALYTICS_CAPABILITY_NAMES.some((capability) => capability === value)
  );
}

function assertAnalyticsDurableProvenance(
  provenance: AnalyticsDurableProvenance,
): void {
  const hasCapability = provenance.capability !== undefined;
  if (
    (provenance.analyticsBackend !== "CLICKHOUSE" &&
      provenance.analyticsBackend !== "DORIS") ||
    provenance.deploymentGeneration < 1n ||
    !SHA256_HEX.test(provenance.workloadEpochFingerprint) ||
    !Number.isSafeInteger(provenance.runtimeContractVersion) ||
    provenance.runtimeContractVersion < 1 ||
    !provenance.producerRuntimeLeaseId ||
    hasCapability !==
      (provenance.capabilityActivationGeneration !== undefined) ||
    hasCapability !== (provenance.capabilityContractVersion !== undefined) ||
    (hasCapability &&
      (!isAnalyticsCapabilityName(provenance.capability) ||
        provenance.analyticsBackend !== "DORIS" ||
        provenance.capabilityActivationGeneration! < 1n ||
        !Number.isSafeInteger(provenance.capabilityContractVersion) ||
        provenance.capabilityContractVersion! < 1))
  ) {
    throw new TypeError("Invalid analytics durable provenance");
  }
}

export function analyticsProducerProvenanceFromAdmission(
  admission: AnalyticsAdmissionStamp,
  capability?: AnalyticsCapabilityName,
): AnalyticsDurableProvenance {
  const hasCapabilityStamp =
    admission.capabilityActivationGeneration !== undefined &&
    admission.capabilityContractVersion !== undefined;
  if ((capability !== undefined) !== hasCapabilityStamp) {
    throw new TypeError("Invalid analytics capability provenance");
  }
  const provenance: AnalyticsDurableProvenance = {
    analyticsBackend: admission.analyticsBackend,
    deploymentGeneration: admission.deploymentGeneration,
    workloadEpochFingerprint: admission.workloadEpochFingerprint,
    runtimeContractVersion: admission.runtimeContractVersion,
    producerRuntimeLeaseId: admission.admittingRuntimeLeaseId,
    ...(capability && hasCapabilityStamp
      ? {
          capability,
          capabilityActivationGeneration:
            admission.capabilityActivationGeneration!,
          capabilityContractVersion: admission.capabilityContractVersion!,
        }
      : {}),
  };
  assertAnalyticsDurableProvenance(provenance);
  return provenance;
}

export function serializeAnalyticsDurableProvenance(
  provenance: AnalyticsDurableProvenance,
): SerializedAnalyticsDurableProvenance {
  assertAnalyticsDurableProvenance(provenance);
  const {
    deploymentGeneration,
    capabilityActivationGeneration,
    ...serialized
  } = provenance;
  return {
    ...serialized,
    deploymentGeneration: deploymentGeneration.toString(),
    ...(capabilityActivationGeneration !== undefined
      ? {
          capabilityActivationGeneration:
            capabilityActivationGeneration.toString(),
        }
      : {}),
  };
}

export function deserializeAnalyticsDurableProvenance(
  value: unknown,
): AnalyticsDurableProvenance {
  if (typeof value !== "object" || value === null || Array.isArray(value)) {
    throw new TypeError("Invalid analytics durable provenance");
  }
  const keys = Object.keys(value).sort();
  const hasCapability = "capability" in value;
  const expectedKeys = hasCapability
    ? SERIALIZED_CAPABILITY_PROVENANCE_KEYS
    : SERIALIZED_FOUNDATION_PROVENANCE_KEYS;
  if (
    keys.length !== expectedKeys.length ||
    keys.some((key, index) => key !== expectedKeys[index])
  ) {
    throw new TypeError("Invalid analytics durable provenance");
  }
  const provenance = value as Record<string, unknown>;
  if (
    (provenance.analyticsBackend !== "CLICKHOUSE" &&
      provenance.analyticsBackend !== "DORIS") ||
    typeof provenance.deploymentGeneration !== "string" ||
    typeof provenance.workloadEpochFingerprint !== "string" ||
    typeof provenance.runtimeContractVersion !== "number" ||
    typeof provenance.producerRuntimeLeaseId !== "string" ||
    !POSITIVE_INTEGER.test(provenance.deploymentGeneration) ||
    (hasCapability &&
      (!isAnalyticsCapabilityName(provenance.capability) ||
        typeof provenance.capabilityActivationGeneration !== "string" ||
        !POSITIVE_INTEGER.test(provenance.capabilityActivationGeneration) ||
        typeof provenance.capabilityContractVersion !== "number"))
  ) {
    throw new TypeError("Invalid analytics durable provenance");
  }
  const decoded: AnalyticsDurableProvenance = {
    analyticsBackend: provenance.analyticsBackend,
    deploymentGeneration: BigInt(provenance.deploymentGeneration),
    workloadEpochFingerprint: provenance.workloadEpochFingerprint,
    runtimeContractVersion: provenance.runtimeContractVersion,
    producerRuntimeLeaseId: provenance.producerRuntimeLeaseId,
    ...(hasCapability
      ? {
          capability: provenance.capability as AnalyticsCapabilityName,
          capabilityActivationGeneration: BigInt(
            provenance.capabilityActivationGeneration as string,
          ),
          capabilityContractVersion:
            provenance.capabilityContractVersion as number,
        }
      : {}),
  };
  assertAnalyticsDurableProvenance(decoded);
  return decoded;
}

type AnalyticsDurableProvenanceRecord = {
  readonly analyticsBackend: "CLICKHOUSE" | "DORIS" | null;
  readonly deploymentGeneration: bigint | null;
  readonly workloadEpochFingerprint: string | null;
  readonly runtimeContractVersion: number | null;
  readonly producerRuntimeLeaseId: string | null;
  readonly capability?: AnalyticsCapability | null;
  readonly capabilityActivationGeneration?: bigint | null;
  readonly capabilityContractVersion?: number | null;
};

export function analyticsDurableProvenanceFromRecord(
  record: AnalyticsDurableProvenanceRecord,
): AnalyticsDurableProvenance | null {
  const foundationValues = [
    record.analyticsBackend,
    record.deploymentGeneration,
    record.workloadEpochFingerprint,
    record.runtimeContractVersion,
    record.producerRuntimeLeaseId,
  ];
  if (foundationValues.every((value) => value === null)) return null;
  if (foundationValues.some((value) => value === null)) {
    throw new TypeError("Analytics durable provenance is only partially set");
  }
  const capabilityValues = [
    record.capability ?? null,
    record.capabilityActivationGeneration ?? null,
    record.capabilityContractVersion ?? null,
  ];
  if (
    capabilityValues.some((value) => value !== null) &&
    capabilityValues.some((value) => value === null)
  ) {
    throw new TypeError(
      "Analytics capability provenance is only partially set",
    );
  }
  const provenance: AnalyticsDurableProvenance = {
    analyticsBackend: record.analyticsBackend!,
    deploymentGeneration: record.deploymentGeneration!,
    workloadEpochFingerprint: record.workloadEpochFingerprint!,
    runtimeContractVersion: record.runtimeContractVersion!,
    producerRuntimeLeaseId: record.producerRuntimeLeaseId!,
    ...(record.capability
      ? {
          capability: fromPrismaAnalyticsCapability(record.capability),
          capabilityActivationGeneration:
            record.capabilityActivationGeneration!,
          capabilityContractVersion: record.capabilityContractVersion!,
        }
      : {}),
  };
  assertAnalyticsDurableProvenance(provenance);
  return provenance;
}

export function analyticsDurableProvenanceMatches(
  left: AnalyticsDurableProvenance,
  right: AnalyticsDurableProvenance,
): boolean {
  assertAnalyticsDurableProvenance(left);
  assertAnalyticsDurableProvenance(right);
  return (
    left.analyticsBackend === right.analyticsBackend &&
    left.deploymentGeneration === right.deploymentGeneration &&
    left.workloadEpochFingerprint === right.workloadEpochFingerprint &&
    left.runtimeContractVersion === right.runtimeContractVersion &&
    left.producerRuntimeLeaseId === right.producerRuntimeLeaseId &&
    left.capability === right.capability &&
    left.capabilityActivationGeneration ===
      right.capabilityActivationGeneration &&
    left.capabilityContractVersion === right.capabilityContractVersion
  );
}
