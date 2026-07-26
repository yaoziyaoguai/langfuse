import { Readable } from "stream";

/**
 * DatabaseReadStream fetches and streams database records in paginated batches,
 * simulating a streaming behavior. This class is designed for efficient, memory-optimized chunking of
 * database queries, ideal for processing large datasets with minimal memory overhead. It operates in
 * object mode, directly streaming database entity objects.
 *
 * Note: Due to Prisma's lack of direct streaming support, this class implements a chunk-based approach
 * rather than true database streaming. It fetches data in paginated batches determined by the pageSize.
 * GitHub issue: https://github.com/prisma/prisma/issues/5055
 *
 * @param prisma - The PrismaClient instance for database queries.
 * @param rawSqlQuery - A Prisma.Sql object representing the base SQL query, excluding OFFSET and LIMIT.
 * @param pageSize - Number of records per batch, defining the chunk size.
 *
 * The class extends Node.js's Readable stream, using async iteration and Prisma's pagination for scalable
 * data processing. It's suitable for applications requiring large dataset processing with a low memory footprint.
 */
export class DatabaseReadStream<EntityType> extends Readable {
  private hasNextPage: boolean;
  private offset: number;
  private isReading: boolean;
  private ended: boolean;

  constructor(
    // the delegate function takes care of querying the database in a paginated manner

    private queryDelegate: (
      pageSize: number,
      offset: number,
    ) => Promise<Array<EntityType>>,
    private pageSize: number,
    private maxRecords?: number,
  ) {
    super({ objectMode: true }); // Set object mode to true to allow pushing objects to the stream rather than strings or buffers

    this.isReading = false; // Prevent concurrent read executions
    this.hasNextPage = true;
    this.offset = 0;
    this.ended = false;
  }

  _read() {
    if (this.isReading || this.ended) return;
    this.isReading = true;
    this.readPages().catch((error: unknown) => {
      this.destroy(error instanceof Error ? error : new Error(String(error)));
    });
  }

  private async readPages(): Promise<void> {
    try {
      while (this.hasNextPage) {
        const remaining =
          this.maxRecords === undefined
            ? this.pageSize
            : this.maxRecords - this.offset;
        if (remaining <= 0) {
          this.hasNextPage = false;
          break;
        }

        const requested = Math.min(this.pageSize, remaining);
        const rows = await this.queryDelegate(requested, this.offset);
        this.offset += rows.length;
        if (
          rows.length < requested ||
          this.offset >= (this.maxRecords ?? Infinity)
        ) {
          this.hasNextPage = false;
        }

        for (const row of rows) {
          if (!this.push(row)) return;
        }
      }

      this.ended = true;
      this.push(null);
    } catch (error) {
      this.destroy(error instanceof Error ? error : new Error(String(error)));
    } finally {
      this.isReading = false;
    }
  }
}
