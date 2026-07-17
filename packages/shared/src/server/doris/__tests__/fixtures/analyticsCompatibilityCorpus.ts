// U1 frozen compatibility corpus for the Langfuse Doris analytics storage PoC.
//
// Every Source Version Contract token/conflict, identity edge case, timestamp
// normalization rule, content/filter/time-boundary compatibility fixture lives
// here. This is deterministic input to DorisPoC.integration.test.ts and
// querySemantics.integration.test.ts. It is NOT application code: it captures
// the frozen baseline (langfuse@3.218.0 / 85d233edc) behavior that Doris must
// reproduce, derived from worker/src/services/IngestionService and the v4
// events_full/events_core design (characterization, not a template).
//
// R1B fixture links (dataset run item projection, evaluator score closure) are
// declared as future contract seeds only and add NO R1A launch DDL or PASS gate.

// -----------------------------------------------------------------------------
// Source Version Contract: timestamp token normalization (pure logic, no Doris).
// -----------------------------------------------------------------------------
// The version/ordering token is UTC Unix epoch NANOSECONDS as a checked signed
// BIGINT. Equivalent RFC3339 / protobuf Timestamp expressions MUST yield the
// same token. TypeScript MUST NOT round through `number` (loses precision beyond
// 2^53). Ordinary sequence < INT64_MAX; a terminal delete uses INT64_MAX.
export const INT64_MAX = 9_223_372_036_854_775_807n;

export interface TimestampTokenFixture {
  readonly name: string;
  /** Diverse raw expressions that MUST normalize to the same token. */
  readonly equivalentRaw: readonly string[];
  readonly expectedToken: bigint;
}

export const timestampTokenFixtures: readonly TimestampTokenFixture[] = [
  {
    name: "rfc3339-and-protobuf-epoch-nanos-agree",
    // 2026-07-17T10:00:00.123456789Z == 1784263200123456789 ns since epoch.
    equivalentRaw: [
      "2026-07-17T10:00:00.123456789Z",
      "1784263200123456789",
      "2026-07-17T10:00:00.123456Z + 456789ns",
    ],
    expectedToken: 1_784_263_200_123_456_789n,
  },
  {
    name: "protobuf-struct-high-low-agrees-with-rfc3339",
    equivalentRaw: [
      "2026-07-17T10:00:00Z",
      // protobuf Timestamp { seconds: 1784263200, nanos: 0 }
      "{seconds:1784263200,nanos:0}",
    ],
    expectedToken: 1_784_263_200_000_000_000n,
  },
];

// -----------------------------------------------------------------------------
// Entity identity: collision-free typed composite.
// -----------------------------------------------------------------------------
// Event/span identity is the (trace_id, span_id) PAIR, stored as separate typed
// key columns. Cross-trace equal span IDs MUST NOT collide. OTLP span_id alone
// is never the identity, and ambiguous string concatenation is forbidden.
export interface IdentityFixture {
  readonly name: string;
  readonly project_id: string;
  readonly partition_date: string;
  readonly trace_id: string;
  readonly span_id: string;
  readonly expectCollidesWith?: IdentityFixture;
  readonly expectDistinctFrom?: IdentityFixture;
}

const traceA_spanX: IdentityFixture = {
  name: "trace-a-span-x",
  project_id: "p1",
  partition_date: "2026-07-17",
  trace_id: "traceA",
  span_id: "0000000000000001",
};
const traceB_spanX: IdentityFixture = {
  name: "trace-b-span-x-same-spanid-different-trace",
  project_id: "p1",
  partition_date: "2026-07-17",
  trace_id: "traceB",
  span_id: "0000000000000001", // identical span_id, different trace_id -> distinct
  expectDistinctFrom: traceA_spanX,
};

export const identityFixtures: readonly IdentityFixture[] = [
  traceA_spanX,
  traceB_spanX,
  // Same trace, two distinct spans must not collide either.
  {
    name: "trace-a-span-y",
    project_id: "p1",
    partition_date: "2026-07-17",
    trace_id: "traceA",
    span_id: "0000000000000002",
    expectDistinctFrom: traceA_spanX,
  },
];

// -----------------------------------------------------------------------------
// Source Version Contract: current-state winner + quarantine cases (engine).
// -----------------------------------------------------------------------------
// Each fixture is a sequence of writes for ONE entity key. The PoC asserts the
// final current row matches `expectedWinner` and that quarantine/no-op outcomes
// hold. version_token is UTC epoch nanoseconds; higher wins; same-token+same-hash
// is no-op; same-token+different-hash quarantines; cross-day partition mutation
// quarantines. A terminal delete (INT64_MAX + DELETE_SIGN) beats every ordinary
// version and cannot be resurrected by a later lower/equal write.

export interface VersionEvent {
  readonly version_token: bigint;
  readonly name: string;
  /** Marks this event as a row-level terminal delete (DELETE_SIGN=1). */
  readonly isDelete?: boolean;
  /**
   * Canonical payload hash this event would carry. same token + same hash =>
   * no-op; same token + different hash => quarantine (loser), not a winner.
   */
  readonly canonicalHash: string;
}

export interface CurrentStateFixture {
  readonly name: string;
  readonly project_id: string;
  readonly partition_date: string;
  readonly trace_id: string;
  readonly span_id: string;
  readonly events: readonly VersionEvent[];
  readonly expectedWinnerName: string | null; // null => entity invisible (deleted)
  /** Indices (into events) that should be quarantined, not published as winner. */
  readonly expectedQuarantinedIndices: readonly number[];
}

export const currentStateFixtures: readonly CurrentStateFixture[] = [
  {
    name: "higher-token-wins-regardless-of-arrival-order",
    project_id: "p1",
    partition_date: "2026-07-17",
    trace_id: "t-order",
    span_id: "s1",
    // Arrive out of order: v3(1500) last but lowest; v2(2000) must win.
    events: [
      { version_token: 1000n, name: "gen-v1", canonicalHash: "h1" },
      { version_token: 2000n, name: "gen-v2", canonicalHash: "h2" },
      { version_token: 1500n, name: "gen-v3-late", canonicalHash: "h3" },
    ],
    expectedWinnerName: "gen-v2",
    expectedQuarantinedIndices: [],
  },
  {
    name: "same-token-same-hash-is-noop",
    project_id: "p1",
    partition_date: "2026-07-17",
    trace_id: "t-noop",
    span_id: "s1",
    events: [
      { version_token: 5000n, name: "gen", canonicalHash: "h-same" },
      { version_token: 5000n, name: "gen", canonicalHash: "h-same" },
    ],
    expectedWinnerName: "gen",
    expectedQuarantinedIndices: [],
  },
  {
    name: "same-token-different-hash-quarantines-loser",
    project_id: "p1",
    partition_date: "2026-07-17",
    trace_id: "t-conflict",
    span_id: "s1",
    events: [
      { version_token: 7000n, name: "winner-payload", canonicalHash: "h-a" },
      { version_token: 7000n, name: "loser-payload", canonicalHash: "h-b" },
    ],
    // Unique-key MoW keeps one row by sequence; the conflicting payload that
    // lost the entity-head CAS is quarantined at the application ledger (U4).
    // Engine-level: only one current row exists.
    expectedWinnerName: "winner-payload",
    expectedQuarantinedIndices: [1],
  },
  {
    name: "terminal-delete-beats-every-ordinary-version",
    project_id: "p1",
    partition_date: "2026-07-17",
    trace_id: "t-del",
    span_id: "s1",
    events: [
      { version_token: 1000n, name: "gen-v1", canonicalHash: "h1" },
      { version_token: 2000n, name: "gen-v2", canonicalHash: "h2" },
      {
        version_token: INT64_MAX,
        name: "del",
        canonicalHash: "h-del",
        isDelete: true,
      },
    ],
    expectedWinnerName: null,
    expectedQuarantinedIndices: [],
  },
  {
    name: "anti-resurrection-late-lower-write-after-delete",
    project_id: "p1",
    partition_date: "2026-07-17",
    trace_id: "t-resurrect",
    span_id: "s1",
    events: [
      { version_token: 1000n, name: "gen", canonicalHash: "h1" },
      {
        version_token: INT64_MAX,
        name: "del",
        canonicalHash: "h-del",
        isDelete: true,
      },
      // Late retry with an EQUAL ordinary token must NOT resurrect.
      { version_token: 1000n, name: "resurrect-attempt", canonicalHash: "h1" },
    ],
    expectedWinnerName: null,
    expectedQuarantinedIndices: [],
  },
];

// Cross-day partition mutation: the first canonical start fixes partition_date
// (UTC). A later mutation whose start would land on a different UTC date MUST
// quarantine rather than write a second current row. Engine-level: the PoC
// proves the partition_date is part of the unique key, so a cross-day re-key is
// a distinct key (caught and quarantined by the entity-head CAS in U4).
export const crossDayPartitionFixture = {
  name: "cross-day-partition-mutation-is-distinct-key",
  firstPublish: {
    project_id: "p1",
    partition_date: "2026-07-17",
    trace_id: "t-xday",
    span_id: "s1",
    version_token: 1000n,
    start_time: "2026-07-17 23:59:00.000000",
  },
  rejectedMutation: {
    // Same trace/span but a start_time crossing UTC midnight -> different
    // partition_date -> distinct unique key -> quarantine, not an update.
    partition_date: "2026-07-18",
    start_time: "2026-07-18 00:01:00.000000",
  },
} as const;

// -----------------------------------------------------------------------------
// v4 source-time tokens (envelope timestamp vs body startTime/endTime).
// -----------------------------------------------------------------------------
// Frozen baseline behavior captured from IngestionService:
//   - v4 observation mutation version = mandatory top-level ingestion envelope
//     `timestamp`. body startTime/endTime MAY be absent; when canonical
//     start_time is missing it is derived from the SAME envelope timestamp
//     deterministically (never receipt/processing time). endTime stays nullable.
//   - Multiple updates MAY share one endTime; they order by envelope timestamp.
export interface V4SourceTimeFixture {
  readonly name: string;
  /** Envelope timestamp (ms) is the ordering/version token source. */
  readonly envelopeTimestampMs: number;
  readonly bodyStartTime: string | null;
  readonly bodyEndTime: string | null;
  readonly expectedCanonicalStart: string;
  readonly expectedEndNullable: boolean;
}

export const v4SourceTimeFixtures: readonly V4SourceTimeFixture[] = [
  {
    name: "both-body-times-absent-uses-envelope-for-start",
    envelopeTimestampMs: Date.UTC(2026, 6, 17, 10, 0, 0), // 2026-07-17T10:00:00Z
    bodyStartTime: null,
    bodyEndTime: null,
    // Deterministic canonical start = envelope timestamp; not processing clock.
    expectedCanonicalStart: "2026-07-17 10:00:00.000000",
    expectedEndNullable: true,
  },
  {
    name: "shared-endTime-across-updates-orders-by-envelope",
    // Two updates sharing endTime but distinct envelope timestamps: the higher
    // envelope token wins. Captured as two events in currentStateFixtures
    // semantics; here we record the per-event shape.
    envelopeTimestampMs: Date.UTC(2026, 6, 17, 10, 5, 0),
    bodyStartTime: "2026-07-17 10:00:00.000000",
    bodyEndTime: "2026-07-17 10:10:00.000000",
    expectedCanonicalStart: "2026-07-17 10:00:00.000000",
    expectedEndNullable: false,
  },
];

// OTLP snapshot time tokens: version = valid source end_time; ONLY for a
// protocol-valid incomplete snapshot does it fall back to valid source
// start_time. Missing the protocol-required source time => durable validation/
// quarantine (NOT receipt/processing time). The PoC proves these map to the
// right canonical token or quarantine via the canonicalizer unit (U3); U1
// freezes the token taxonomy here.
export const otlpSnapshotTimeTokens = [
  "timestamp_missing",
  "timestamp_invalid_empty_string",
  "timestamp_invalid_number",
  "timestamp_invalid_string",
  "timestamp_inferred_start_from_end",
  "timestamp_inferred_end_from_start",
  "timestamp_inferred_both_missing",
] as const;
export type OtlpSnapshotTimeToken = (typeof otlpSnapshotTimeTokens)[number];

// -----------------------------------------------------------------------------
// Full-content search corpus (multilingual / escaped) for the U1-frozen index.
// -----------------------------------------------------------------------------
// parser=unicode inverted index must find CJK, Arabic, astral-plane, emoji, raw
// JSON and \\uXXXX-escaped content. No silent full-scan fallback; full-content
// search requires an explicit <=30-day range (R1A-new contract).
export interface ContentSearchFixture {
  readonly name: string;
  readonly input: string;
  readonly output: string;
  readonly query: string;
  readonly expectMatch: boolean;
}

export const contentSearchFixtures: readonly ContentSearchFixture[] = [
  {
    name: "cjk-chinese",
    input: "用户询问了关于模型价格的问题",
    output: "回答包含 token 计费说明",
    query: "模型价格",
    expectMatch: true,
  },
  {
    name: "korean",
    input: "사용자가 모델 비용에 대해 질문했습니다",
    output: "토큰 요금 설명을 포함한 답변",
    query: "비용",
    expectMatch: true,
  },
  {
    name: "arabic",
    input: "سأل المستخدم عن تكلفة النموذج",
    output: "تتضمن الإجابة شرح فوترة الرموز",
    query: "تكلفة",
    expectMatch: true,
  },
  {
    name: "emoji-and-astral",
    input: "🚀 rocket launch trace 😀 face",
    output: "result 𝕳𝖊𝖑𝖑𝖔 astral-plane",
    query: "🚀",
    expectMatch: true,
  },
  {
    name: "raw-json-content",
    input: '{"tool":"search","q":"alpha beta"}',
    output: '{"hits":[1,2,3]}',
    query: "alpha beta",
    expectMatch: true,
  },
  {
    name: "escaped-unicode-on-disk",
    // Python SDK serializes non-ASCII as \uXXXX; search must still find it.
    input: "\\u4ef7\\u683c means price",
    output: "ok",
    query: "价格",
    expectMatch: false, // escaped ASCII form does not contain the literal CJK
  },
];

// -----------------------------------------------------------------------------
// Filter / null / empty / array / negative-score compatibility fixtures.
// -----------------------------------------------------------------------------
export interface FilterFixture {
  readonly name: string;
  readonly description: string;
  readonly rowCount: number;
  readonly expectMatched: number;
}

export const filterBehaviorFixtures: readonly FilterFixture[] = [
  {
    name: "null-vs-empty-string",
    description: "'' ≡ NULL compatibility (emptyEqualsNull)",
    rowCount: 0,
    expectMatched: 0,
  },
  {
    name: "array-any-of",
    description: "tags any of [a,b]",
    rowCount: 0,
    expectMatched: 0,
  },
  {
    name: "array-none-of",
    description: "tags none of [x]",
    rowCount: 0,
    expectMatched: 0,
  },
  {
    name: "array-all-of",
    description: "tags all of [a,b]",
    rowCount: 0,
    expectMatched: 0,
  },
  {
    name: "negative-score-filter",
    description: "score < 0 excluded/allowed per contract",
    rowCount: 0,
    expectMatched: 0,
  },
  {
    name: "metadata-missing-key",
    description: "metadata key absent => not matched",
    rowCount: 0,
    expectMatched: 0,
  },
];

// -----------------------------------------------------------------------------
// Time boundary compatibility (UTC, [from, to), midnight crossing).
// -----------------------------------------------------------------------------
export const timeBoundaryFixtures = [
  {
    name: "utc-midnight-crossing-trace",
    // A trace whose events span UTC midnight lives in TWO partition_dates; both
    // halves must be queryable by the same trace_id and reconcile to one trace.
    trace_id: "t-midnight",
    spans: [
      {
        span_id: "pre",
        partition_date: "2026-07-17",
        start_time: "2026-07-17 23:55:00.000000",
      },
      {
        span_id: "post",
        partition_date: "2026-07-18",
        start_time: "2026-07-18 00:05:00.000000",
      },
    ],
  },
] as const;

// R1B future contract seeds (NOT R1A launch DDL / PASS gates):
//   - dataset_run_items_current projection (immutable run date from first
//     captured control-row created_at; replay uses captured token).
//   - evaluator/experiment score closure (bounded retry, idempotent score).
// These are recorded here so a future U9 corpus amendment has a stable anchor.
export const r1bFutureSeeds = [
  "dataset_run_items_current-projection",
  "evaluator-experiment-score-closure",
] as const;
