import { BatchExportTableName } from "@langfuse/shared";
import { Readable } from "node:stream";

import type { getDatabaseReadStreamPaginated } from "../database-read-stream/getDatabaseReadStream";
import {
  type AnalyticsExportOpenOptions,
  type AnalyticsExportRequest,
  type AnalyticsExportSource,
} from "./AnalyticsExportSource";
import type { BatchExportIdentity } from "./BatchExportIdentityManifest";
import type { DorisBatchExportIdentitySource } from "./DorisBatchExportIdentitySource";

type DorisAnalyticsExportDependencies = {
  readonly paginated: typeof getDatabaseReadStreamPaginated;
  readonly identities: Pick<DorisBatchExportIdentitySource, "scan">;
  readonly pageSize: number;
};

export class DorisAnalyticsExportSource implements AnalyticsExportSource {
  constructor(
    private readonly dependencies: DorisAnalyticsExportDependencies,
  ) {}

  scanIdentities(
    request: AnalyticsExportRequest,
    hardLimit: number,
    signal?: AbortSignal,
  ): AsyncIterable<BatchExportIdentity> {
    return this.dependencies.identities.scan(request, hardLimit, signal);
  }

  async open(
    request: AnalyticsExportRequest,
    options?: AnalyticsExportOpenOptions,
  ): Promise<Readable> {
    if (!options?.identities) {
      throw new Error(
        "A sealed identity manifest is required for Doris export",
      );
    }
    const identities = options.identities;
    if (
      !Number.isSafeInteger(this.dependencies.pageSize) ||
      this.dependencies.pageSize < 1
    ) {
      throw new TypeError("Invalid Doris export page size");
    }

    const composite =
      request.tableName === BatchExportTableName.Observations ||
      request.tableName === BatchExportTableName.Events;
    const paginated = this.dependencies.paginated;
    const pageSize = this.dependencies.pageSize;
    const rows = async function* () {
      let batch: BatchExportIdentity[] = [];
      let traceId: string | undefined;

      const flush = async function* () {
        if (batch.length === 0) return;
        await options.revalidate?.();
        if (options.signal?.aborted)
          throw new Error("Batch export was aborted");
        const expected = batch;
        batch = [];
        const {
          fileFormat: _fileFormat,
          searchQuery: _searchQuery,
          searchType: _searchType,
          ...query
        } = request;
        const source = await paginated({
          ...query,
          filter: [],
          exactIdentityIds: expected.map(({ id }) => id),
          ...(traceId ? { exactTraceId: traceId } : {}),
          rowLimit: expected.length,
        });
        const current = new Map<string, unknown>();
        for await (const candidate of source) {
          if (options.signal?.aborted) {
            source.destroy(new Error("Batch export was aborted"));
            throw new Error("Batch export was aborted");
          }
          if (
            typeof candidate !== "object" ||
            candidate === null ||
            !("id" in candidate) ||
            typeof candidate.id !== "string"
          ) {
            throw new Error("Doris export reader returned a row without an id");
          }
          if (current.has(candidate.id)) {
            throw new Error(
              "Doris export reader returned duplicate identities",
            );
          }
          current.set(candidate.id, candidate);
        }
        for (const identity of expected) {
          const row = current.get(identity.id);
          if (row !== undefined) yield row;
        }
      };

      for await (const identity of identities) {
        if (composite) {
          if (!identity.traceId) {
            throw new Error("Doris event manifest identity is missing traceId");
          }
          if (batch.length > 0 && identity.traceId !== traceId) yield* flush();
          traceId = identity.traceId;
        } else if (identity.traceId !== undefined) {
          throw new Error("Doris manifest identity has an unexpected traceId");
        }
        batch.push(identity);
        if (batch.length >= pageSize) yield* flush();
      }
      yield* flush();
    };

    return Readable.from(rows());
  }
}
