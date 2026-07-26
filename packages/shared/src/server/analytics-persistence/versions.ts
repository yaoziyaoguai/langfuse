export const CURRENT_ANALYTICS_CANONICALIZER_VERSION = "1";
export const CURRENT_ANALYTICS_SCHEMA_VERSION = 1;
export const NEXT_ANALYTICS_SCHEMA_VERSION = 2;

export type AnalyticsContractCompatibilityWindow = {
  readonly writerSchemaVersion: number;
  readonly readableSchemaVersions: readonly number[];
  readonly writerCanonicalizerVersion: string;
  readonly readableCanonicalizerVersions: readonly string[];
};

function assertAdjacentNumericWindow(input: {
  readonly writer: number;
  readonly readable: readonly number[];
  readonly label: string;
}): void {
  if (
    !Number.isSafeInteger(input.writer) ||
    input.writer < 1 ||
    input.readable.length < 1 ||
    input.readable.length > 2 ||
    input.readable.some(
      (version) => !Number.isSafeInteger(version) || version < 1,
    ) ||
    new Set(input.readable).size !== input.readable.length
  ) {
    throw new TypeError(`Invalid ${input.label} compatibility window`);
  }
  const ordered = input.readable.every(
    (version, index) => index === 0 || version > input.readable[index - 1]!,
  );
  if (!ordered) {
    throw new TypeError(`${input.label} versions must be ordered`);
  }
  if (!input.readable.includes(input.writer)) {
    throw new TypeError(`Writer ${input.label} version must be readable`);
  }
  if (
    input.readable.length === 2 &&
    input.readable[1]! !== input.readable[0]! + 1
  ) {
    throw new TypeError(`${input.label} versions must be adjacent`);
  }
}

export function defineAdjacentAnalyticsContractWindow(
  input: AnalyticsContractCompatibilityWindow,
): AnalyticsContractCompatibilityWindow {
  assertAdjacentNumericWindow({
    writer: input.writerSchemaVersion,
    readable: input.readableSchemaVersions,
    label: "schema",
  });
  if (
    !/^[1-9][0-9]*$/.test(input.writerCanonicalizerVersion) ||
    input.readableCanonicalizerVersions.some(
      (version) => !/^[1-9][0-9]*$/.test(version),
    )
  ) {
    throw new TypeError("Invalid canonicalizer compatibility window");
  }
  assertAdjacentNumericWindow({
    writer: Number(input.writerCanonicalizerVersion),
    readable: input.readableCanonicalizerVersions.map(Number),
    label: "canonicalizer",
  });
  return {
    writerSchemaVersion: input.writerSchemaVersion,
    readableSchemaVersions: [...input.readableSchemaVersions],
    writerCanonicalizerVersion: input.writerCanonicalizerVersion,
    readableCanonicalizerVersions: [...input.readableCanonicalizerVersions],
  };
}

// Release A 只会把 reader 显式扩大到一个相邻版本。
// writer 版本独立固定，在 Release B 门禁通过前不会随 reader 窗口推进。
export const ANALYTICS_CONTRACT_COMPATIBILITY =
  defineAdjacentAnalyticsContractWindow({
    writerSchemaVersion: CURRENT_ANALYTICS_SCHEMA_VERSION,
    readableSchemaVersions: [
      CURRENT_ANALYTICS_SCHEMA_VERSION,
      NEXT_ANALYTICS_SCHEMA_VERSION,
    ],
    writerCanonicalizerVersion: CURRENT_ANALYTICS_CANONICALIZER_VERSION,
    readableCanonicalizerVersions: [CURRENT_ANALYTICS_CANONICALIZER_VERSION],
  });
