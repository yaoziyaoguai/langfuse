import { readdir, readFile } from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";

const communityExtensionRoots = [
  "packages/shared/src/features/community-extensions",
  "packages/shared/src/server/community-extensions",
  "web/src/features/admin-api",
  "web/src/features/community-extensions",
  "web/src/features/ui-customization",
  "worker/src/features/community-extensions",
];

const sourceExtensions = new Set([".js", ".jsx", ".mjs", ".ts", ".tsx"]);
const importSourcePattern =
  /(?:from\s*|import\s*\(\s*|require\(\s*)["']([^"']+)["']/g;
const forbiddenLicenseTokens = [
  "LANGFUSE_EE_LICENSE_KEY",
  "langfuse_ee_",
  "self-hosted:enterprise",
];

const listSourceFiles = async (root) => {
  const files = [];

  const visit = async (directory) => {
    let entries;
    try {
      entries = await readdir(directory, { withFileTypes: true });
    } catch (error) {
      if (error?.code === "ENOENT") return;
      throw error;
    }

    for (const entry of entries) {
      const entryPath = path.join(directory, entry.name);
      if (entry.isDirectory()) {
        await visit(entryPath);
      } else if (sourceExtensions.has(path.extname(entry.name))) {
        files.push(entryPath);
      }
    }
  };

  await visit(root);
  return files;
};

const isOfficialEeImport = (source) =>
  source === "ee" ||
  source.includes("/ee/") ||
  source.endsWith("/ee") ||
  source.startsWith("@/src/ee") ||
  source.startsWith("@langfuse/shared/src/server/ee");

export const findCommunityExtensionBoundaryViolations = async (
  repositoryRoot,
) => {
  const violations = [];

  for (const relativeRoot of communityExtensionRoots) {
    const absoluteRoot = path.join(repositoryRoot, relativeRoot);
    for (const file of await listSourceFiles(absoluteRoot)) {
      const contents = await readFile(file, "utf8");
      const lines = contents.split(/\r?\n/);

      lines.forEach((line, index) => {
        importSourcePattern.lastIndex = 0;
        for (const match of line.matchAll(importSourcePattern)) {
          if (isOfficialEeImport(match[1])) {
            violations.push({
              file: path.relative(repositoryRoot, file),
              line: index + 1,
              reason: `official EE import is forbidden: ${match[1]}`,
            });
          }
        }

        for (const token of forbiddenLicenseTokens) {
          if (line.includes(token)) {
            violations.push({
              file: path.relative(repositoryRoot, file),
              line: index + 1,
              reason: `license spoofing token is forbidden: ${token}`,
            });
          }
        }
      });
    }
  }

  return violations;
};

const currentFile = fileURLToPath(import.meta.url);
if (process.argv[1] && path.resolve(process.argv[1]) === currentFile) {
  const repositoryRoot = path.resolve(path.dirname(currentFile), "..");
  const violations =
    await findCommunityExtensionBoundaryViolations(repositoryRoot);

  if (violations.length > 0) {
    for (const violation of violations) {
      process.stderr.write(
        `${violation.file}:${violation.line} ${violation.reason}\n`,
      );
    }
    process.exitCode = 1;
  } else {
    process.stdout.write("Community Extensions boundaries: valid\n");
  }
}
