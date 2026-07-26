/**
 * Bootstrap for the Langfuse seed CLI.
 *
 * Deliberately imports nothing from src/: importing the server barrel parses
 * the shared env schema at module load, which crashes with a raw ZodError
 * when the repo-root .env is missing — exactly the fresh-clone situation
 * where the CLI must instead print the fix. This file stays env-independent
 * and dynamically loads ./cli-main, whose preflight is backend-aware.
 */
// `list`, help, unsupported-scenario checks, and dry-runs are deliberately
// available without database credentials. Runtime preflight reports the
// selected backend's missing variables before any backend client is loaded.
if (process.argv.includes("--json")) {
  // Legacy ClickHouse scenarios import winston later; preserve JSON-only stdout.
  // eslint-disable-next-line turbo/no-undeclared-env-vars
  process.env.LANGFUSE_LOG_LEVEL = "error";
}
import("./cli-main.js")
  .then((cliMain) => cliMain.run())
  .catch((error: unknown) => {
    const message =
      error instanceof Error ? error.message : String(error ?? "unknown");
    console.error(`error: failed to start the seed CLI: ${message}`);
    process.exitCode = 1;
  });
