import { readFile } from "node:fs/promises";
import { isAbsolute } from "node:path";

const MAX_WORKLOAD_EPOCH_BYTES = 4_096;

type ReadTextFile = (path: string) => Promise<string>;

export async function resolveAnalyticsRuntimeWorkloadEpoch(input: {
  readonly value?: string;
  readonly file?: string;
  readonly readTextFile?: ReadTextFile;
}): Promise<string | undefined> {
  if (input.value === undefined && input.file === undefined) return undefined;
  if (input.value !== undefined && input.file !== undefined) {
    throw new TypeError(
      "Configure only one analytics workload epoch source: direct value or mounted file",
    );
  }
  const readTextFile =
    input.readTextFile ??
    ((path: string) => readFile(path, { encoding: "utf8" }));
  let raw: string;
  if (input.value !== undefined) {
    raw = input.value;
  } else {
    if (!input.file || !isAbsolute(input.file)) {
      throw new TypeError(
        "Analytics workload epoch file must use an absolute path",
      );
    }
    raw = await readTextFile(input.file);
  }
  if (Buffer.byteLength(raw, "utf8") > MAX_WORKLOAD_EPOCH_BYTES) {
    throw new TypeError("Workload epoch exceeds the maximum supported size");
  }
  const epoch = raw.trim();
  if (!epoch) throw new TypeError("Workload epoch must not be empty");
  return epoch;
}
