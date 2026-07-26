import {
  ParquetSchema,
  ParquetWriter,
  type SchemaDefinition,
} from "@dsnp/parquetjs";

const DATE_FIELDS = new Set([
  "timestamp",
  "start_time",
  "end_time",
  "completion_start_time",
  "created_at",
  "updated_at",
]);
const BOOLEAN_FIELDS = new Set(["public", "bookmarked"]);
const NUMBER_FIELDS = new Set([
  "value",
  "total_cost",
  "latency",
  "time_to_first_token",
  "input_price",
  "output_price",
  "total_price",
  "prompt_version",
]);

function schemaForRows(
  rows: readonly Readonly<Record<string, unknown>>[],
): SchemaDefinition {
  const fields = [...new Set(rows.flatMap((row) => Object.keys(row)))].sort();
  return Object.fromEntries(
    fields.map((field) => [
      field,
      DATE_FIELDS.has(field)
        ? { type: "TIMESTAMP_MILLIS", optional: true }
        : BOOLEAN_FIELDS.has(field)
          ? { type: "BOOLEAN", optional: true }
          : NUMBER_FIELDS.has(field)
            ? { type: "DOUBLE", optional: true }
            : {
                type: "UTF8",
                optional: true,
                compression: "SNAPPY",
              },
    ]),
  );
}

function parquetValue(field: string, value: unknown): unknown {
  if (value === null || value === undefined) return undefined;
  if (DATE_FIELDS.has(field)) {
    const date = value instanceof Date ? value : new Date(String(value));
    if (!Number.isFinite(date.getTime())) {
      throw new TypeError(`Invalid Parquet timestamp field: ${field}`);
    }
    return date;
  }
  if (BOOLEAN_FIELDS.has(field)) return Boolean(value);
  if (NUMBER_FIELDS.has(field)) {
    const number = Number(value);
    return Number.isFinite(number) ? number : undefined;
  }
  return typeof value === "string" ? value : JSON.stringify(value);
}

export async function writeDorisParquetFile(input: {
  readonly filePath: string;
  readonly rows: readonly Readonly<Record<string, unknown>>[];
}): Promise<void> {
  if (input.rows.length === 0) {
    throw new TypeError("Cannot write an empty Doris Parquet file");
  }
  const schema = new ParquetSchema(schemaForRows(input.rows));
  const writer = await ParquetWriter.openFile(schema, input.filePath, {
    useDataPageV2: false,
  });
  try {
    for (const row of input.rows) {
      await writer.appendRow(
        Object.fromEntries(
          Object.entries(row).map(([field, value]) => [
            field,
            parquetValue(field, value),
          ]),
        ),
      );
    }
  } finally {
    await writer.close();
  }
}
