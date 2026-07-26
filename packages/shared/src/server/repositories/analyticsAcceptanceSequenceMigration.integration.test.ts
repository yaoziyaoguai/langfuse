import { readFileSync } from "node:fs";
import { resolve } from "node:path";

import { PrismaClient } from "@prisma/client";
import { afterAll, describe, expect, it } from "vitest";

const databaseUrl = process.env.DATABASE_URL;

const migrationSql = readFileSync(
  resolve(
    __dirname,
    "../../../prisma/migrations/20260722070000_add_analytics_ingestion_barrier/migration.sql",
  ),
  "utf8",
);

function splitStatements(sql: string): string[] {
  return sql
    .split(";")
    .map((statement) => statement.trim())
    .filter(Boolean);
}

describe.skipIf(!databaseUrl)("analytics acceptance sequence migration", () => {
  const client = new PrismaClient({ datasourceUrl: databaseUrl });

  afterAll(async () => {
    await client.$disconnect();
  });

  it("keeps pre-migration ACTIVE rows with NULL sequence drainable", async () => {
    const schema = `acceptance_migration_${Date.now()}_${Math.random()
      .toString(16)
      .slice(2)}`;
    const quotedSchema = `"${schema}"`;

    await client.$executeRawUnsafe(`CREATE SCHEMA ${quotedSchema}`);
    try {
      await client.$transaction(async (transaction) => {
        await transaction.$executeRawUnsafe(
          `SET LOCAL search_path TO ${quotedSchema}`,
        );
        await transaction.$executeRawUnsafe(
          'CREATE TABLE "analytics_deletion_operations" ("id" TEXT PRIMARY KEY)',
        );
        await transaction.$executeRawUnsafe(
          'CREATE TABLE "analytics_ingestion_operations" ("id" TEXT PRIMARY KEY, "status" TEXT NOT NULL)',
        );
        await transaction.$executeRawUnsafe(
          'INSERT INTO "analytics_ingestion_operations" ("id", "status") VALUES (\'legacy-active\', \'ACTIVE\')',
        );

        for (const statement of splitStatements(migrationSql)) {
          await transaction.$executeRawUnsafe(statement);
        }

        await transaction.$executeRawUnsafe(
          'UPDATE "analytics_ingestion_operations" SET "status" = \'COMPLETED\' WHERE "id" = \'legacy-active\'',
        );
        const rows = await transaction.$queryRawUnsafe<
          Array<{ status: string; acceptance_sequence: bigint | null }>
        >(
          'SELECT "status", "acceptance_sequence" FROM "analytics_ingestion_operations" WHERE "id" = \'legacy-active\'',
        );
        expect(rows).toEqual([
          { status: "COMPLETED", acceptance_sequence: null },
        ]);
      });
    } finally {
      await client.$executeRawUnsafe(`DROP SCHEMA ${quotedSchema} CASCADE`);
    }
  });
});
