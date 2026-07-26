import { promises as fs } from "node:fs";
import { tmpdir } from "node:os";
import path from "node:path";

import { ParquetReader } from "@dsnp/parquetjs";
import { afterEach, describe, expect, it } from "vitest";

import { writeDorisParquetFile } from "./dorisParquetWriter";

const directories: string[] = [];

afterEach(async () => {
  for (const directory of directories.splice(0)) {
    await fs.rm(directory, { recursive: true, force: true });
  }
});

describe("writeDorisParquetFile", () => {
  it("writes typed, externally readable rows without losing structured fields", async () => {
    const directory = await fs.mkdtemp(
      path.join(tmpdir(), "langfuse-parquet-writer-test-"),
    );
    directories.push(directory);
    const filePath = path.join(directory, "export.parquet");
    const timestamp = new Date("2026-07-25T12:34:56.789Z");

    await writeDorisParquetFile({
      filePath,
      rows: [
        {
          id: "trace-1",
          timestamp,
          public: true,
          total_cost: 1.25,
          metadata: { model: "gpt-5", nested: { count: 2 } },
          nullable: null,
        },
        {
          id: "trace-2",
          timestamp: timestamp.toISOString(),
          public: false,
          total_cost: "2.5",
          metadata: ["one", "two"],
          nullable: "present",
        },
      ],
    });

    const reader = await ParquetReader.openFile(filePath);
    try {
      const cursor = reader.getCursor();
      const rows: Record<string, unknown>[] = [];
      for (
        let row = await cursor.next();
        row !== null;
        row = await cursor.next()
      ) {
        rows.push(row as Record<string, unknown>);
      }

      expect(rows).toEqual([
        {
          id: "trace-1",
          metadata: '{"model":"gpt-5","nested":{"count":2}}',
          nullable: null,
          public: true,
          timestamp,
          total_cost: 1.25,
        },
        {
          id: "trace-2",
          metadata: '["one","two"]',
          nullable: "present",
          public: false,
          timestamp,
          total_cost: 2.5,
        },
      ]);
    } finally {
      await reader.close();
    }
  });

  it("rejects empty exports and invalid timestamps", async () => {
    const directory = await fs.mkdtemp(
      path.join(tmpdir(), "langfuse-parquet-writer-test-"),
    );
    directories.push(directory);
    const filePath = path.join(directory, "export.parquet");

    await expect(writeDorisParquetFile({ filePath, rows: [] })).rejects.toThrow(
      /empty Doris Parquet/i,
    );
    await expect(
      writeDorisParquetFile({
        filePath,
        rows: [{ id: "trace-1", timestamp: "not-a-timestamp" }],
      }),
    ).rejects.toThrow(/Invalid Parquet timestamp field: timestamp/);
  });
});
