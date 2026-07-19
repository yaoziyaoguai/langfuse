import type { NextApiRequest, NextApiResponse } from "next";

export type CommunityCapability =
  | "evaluations"
  | "experiments"
  | "monitors"
  | "batchExports";

export type UnsupportedFeatureBody = {
  readonly error: "UnsupportedFeature";
  readonly code:
    | "R1B_EVALUATIONS_UNAVAILABLE"
    | "R1B_EXPERIMENTS_UNAVAILABLE"
    | "R2_MONITORS_UNAVAILABLE"
    | "R2_BATCH_EXPORTS_UNAVAILABLE";
  readonly message: string;
  readonly recovery: string;
};

export const COMMUNITY_CAPABILITIES: Readonly<
  Record<CommunityCapability, UnsupportedFeatureBody>
> = {
  evaluations: {
    error: "UnsupportedFeature",
    code: "R1B_EVALUATIONS_UNAVAILABLE",
    message: "Evaluator execution is not available in the Doris R1A release.",
    recovery:
      "Adopt the R1B evaluation capability only after its owner, usage evidence, Doris implementation, and backlog policy are approved.",
  },
  experiments: {
    error: "UnsupportedFeature",
    code: "R1B_EXPERIMENTS_UNAVAILABLE",
    message:
      "Experiment execution and analytics are not available in the Doris R1A release.",
    recovery:
      "Adopt the R1B experiment capability only after its owner, usage evidence, Doris implementation, and backlog policy are approved.",
  },
  monitors: {
    error: "UnsupportedFeature",
    code: "R2_MONITORS_UNAVAILABLE",
    message: "Product monitors are not available in the Doris R1A release.",
    recovery:
      "Create a separately reviewed Doris monitor implementation before enabling this capability.",
  },
  batchExports: {
    error: "UnsupportedFeature",
    code: "R2_BATCH_EXPORTS_UNAVAILABLE",
    message:
      "Analytics batch exports are not available in the Doris R1A release.",
    recovery:
      "Create a separately reviewed Doris export implementation before enabling this capability.",
  },
};

export const isCommunityCapabilityAvailable = (
  _capability: CommunityCapability,
): boolean => false;

export class CommunityCapabilityUnavailableError extends Error {
  readonly body: UnsupportedFeatureBody;

  constructor(capability: CommunityCapability) {
    const body = COMMUNITY_CAPABILITIES[capability];
    super(body.message);
    this.name = "CommunityCapabilityUnavailableError";
    this.body = body;
  }
}

export function assertCommunityCapability(
  capability: CommunityCapability,
): void {
  if (!isCommunityCapabilityAvailable(capability)) {
    throw new CommunityCapabilityUnavailableError(capability);
  }
}

export function capabilityForTrpcPath(
  path: string,
): CommunityCapability | null {
  if (
    path.startsWith("evals.") ||
    path.startsWith("defaultLlmModel.") ||
    path.startsWith("batchAction.runEvaluation.")
  ) {
    return "evaluations";
  }
  if (path.startsWith("experiments.")) return "experiments";
  if (path.startsWith("monitors.")) return "monitors";
  if (path.startsWith("batchExport.")) return "batchExports";
  return null;
}

export function capabilityForMcpFeature(
  featureName: string,
): CommunityCapability | null {
  if (featureName === "evals") return "evaluations";
  if (featureName === "experiments") return "experiments";
  if (featureName === "monitors") return "monitors";
  return null;
}

export function createUnsupportedFeatureApiHandler(
  capability: CommunityCapability,
): (req: NextApiRequest, res: NextApiResponse) => void {
  return (_req, res) => {
    res.status(501).json(COMMUNITY_CAPABILITIES[capability]);
  };
}
