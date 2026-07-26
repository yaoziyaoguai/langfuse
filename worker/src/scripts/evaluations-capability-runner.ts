type EvaluationCapabilityOperatorRunner = {
  readonly execute: () => Promise<void>;
  readonly disconnect: () => Promise<void>;
  readonly writeFailure: (message: string) => void;
  readonly exit: (code: number) => void;
};

export function normalizePnpmRunArgs(args: readonly string[]): string[] {
  return args[0] === "--" ? args.slice(1) : [...args];
}

export async function runEvaluationCapabilityOperator(
  runner: EvaluationCapabilityOperatorRunner,
): Promise<void> {
  let exitCode = 0;
  try {
    await runner.execute();
  } catch {
    runner.writeFailure(
      "Evaluations capability operator command failed (details redacted)\n",
    );
    exitCode = 1;
  }

  try {
    await runner.disconnect();
  } catch {
    runner.writeFailure(
      "Evaluations capability operator cleanup failed (details redacted)\n",
    );
    exitCode = 1;
  }

  runner.exit(exitCode);
}
