import { describe, expect, it, vi } from "vitest";

import { DatabaseReadStream } from "./DatabaseReadStream";

async function collect<T>(stream: DatabaseReadStream<T>): Promise<T[]> {
  const rows: T[] = [];
  for await (const row of stream) rows.push(row);
  return rows;
}

describe("DatabaseReadStream", () => {
  it("requests only the exact remaining row limit", async () => {
    const rows = Array.from({ length: 12 }, (_, index) => index);
    const query = vi.fn(async (limit: number, offset: number) =>
      rows.slice(offset, offset + limit),
    );

    await expect(collect(new DatabaseReadStream(query, 5, 7))).resolves.toEqual(
      rows.slice(0, 7),
    );
    expect(query.mock.calls).toEqual([
      [5, 0],
      [2, 5],
    ]);
  });

  it("advances by emitted rows and stops after a short page", async () => {
    const query = vi
      .fn<(limit: number, offset: number) => Promise<number[]>>()
      .mockResolvedValueOnce([1, 2])
      .mockResolvedValueOnce([3]);

    await expect(
      collect(new DatabaseReadStream(query, 3, 10)),
    ).resolves.toEqual([1, 2]);
    expect(query).toHaveBeenCalledTimes(1);
  });

  it("destroys the stream when the delegate rejects", async () => {
    const error = new Error("query failed");
    const stream = new DatabaseReadStream(
      vi.fn().mockRejectedValue(error),
      5,
      10,
    );

    await expect(collect(stream)).rejects.toBe(error);
  });
});
