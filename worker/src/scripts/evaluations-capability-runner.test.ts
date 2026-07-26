import { describe, expect, it, vi } from "vitest";

import {
  normalizePnpmRunArgs,
  runEvaluationCapabilityOperator,
} from "./evaluations-capability-runner";

describe("normalizePnpmRunArgs", () => {
  it("removes the separator forwarded by pnpm run", () => {
    expect(
      normalizePnpmRunArgs(["--", "begin-dark", "--expected-generation", "1"]),
    ).toEqual(["begin-dark", "--expected-generation", "1"]);
  });

  it("preserves direct CLI arguments", () => {
    expect(normalizePnpmRunArgs(["status"])).toEqual(["status"]);
  });
});

describe("runEvaluationCapabilityOperator", () => {
  it("disconnects before exiting successfully", async () => {
    const events: string[] = [];

    await runEvaluationCapabilityOperator({
      execute: async () => {
        events.push("execute");
      },
      disconnect: async () => {
        events.push("disconnect");
      },
      writeFailure: vi.fn(),
      exit: (code) => {
        events.push(`exit:${code}`);
      },
    });

    expect(events).toEqual(["execute", "disconnect", "exit:0"]);
  });

  it("redacts command failures, disconnects, and exits unsuccessfully", async () => {
    const events: string[] = [];
    const writeFailure = vi.fn();

    await runEvaluationCapabilityOperator({
      execute: async () => {
        events.push("execute");
        throw new Error("postgresql://secret@db");
      },
      disconnect: async () => {
        events.push("disconnect");
      },
      writeFailure,
      exit: (code) => {
        events.push(`exit:${code}`);
      },
    });

    expect(writeFailure).toHaveBeenCalledWith(
      "Evaluations capability operator command failed (details redacted)\n",
    );
    expect(JSON.stringify(writeFailure.mock.calls)).not.toContain("secret");
    expect(events).toEqual(["execute", "disconnect", "exit:1"]);
  });

  it("reports cleanup failures without exposing their details", async () => {
    const writeFailure = vi.fn();
    const exit = vi.fn();

    await runEvaluationCapabilityOperator({
      execute: async () => undefined,
      disconnect: async () => {
        throw new Error("redis://secret@queue");
      },
      writeFailure,
      exit,
    });

    expect(writeFailure).toHaveBeenCalledWith(
      "Evaluations capability operator cleanup failed (details redacted)\n",
    );
    expect(JSON.stringify(writeFailure.mock.calls)).not.toContain("secret");
    expect(exit).toHaveBeenCalledWith(1);
  });
});
