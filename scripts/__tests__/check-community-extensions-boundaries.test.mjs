import assert from "node:assert/strict";
import { mkdtemp, mkdir, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import path from "node:path";
import test from "node:test";
import { findCommunityExtensionBoundaryViolations } from "../check-community-extensions-boundaries.mjs";

const withFixture = async (contents, callback) => {
  const root = await mkdtemp(
    path.join(tmpdir(), "langfuse-community-boundary-"),
  );
  const featureRoot = path.join(
    root,
    "packages/shared/src/server/community-extensions",
  );
  await mkdir(featureRoot, { recursive: true });
  await writeFile(path.join(featureRoot, "fixture.ts"), contents);

  try {
    await callback(root);
  } finally {
    await rm(root, { recursive: true, force: true });
  }
};

test("accepts community code that only imports MIT modules", async () => {
  await withFixture(
    'import { logger } from "@langfuse/shared/src/server";\n',
    async (root) => {
      assert.deepEqual(
        await findCommunityExtensionBoundaryViolations(root),
        [],
      );
    },
  );
});

test("rejects official EE imports and license spoofing", async () => {
  await withFixture(
    [
      'import { feature } from "@/src/ee/features/example";',
      'const license = "langfuse_ee_fake";',
    ].join("\n"),
    async (root) => {
      const violations = await findCommunityExtensionBoundaryViolations(root);
      assert.equal(violations.length, 2);
      assert.match(violations[0].reason, /official EE import/);
      assert.match(violations[1].reason, /license spoofing/);
    },
  );
});
