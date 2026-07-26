import { beforeEach, describe, expect, it, vi } from "vitest";

const { publicationAdmission } = vi.hoisted(() => ({
  publicationAdmission: vi.fn(),
}));

vi.mock("@/src/server/analyticsQueuePublicationAdmission", () => ({
  withAnalyticsQueuePublicationAdmission: publicationAdmission,
}));

import { withAnalyticsEvaluationMutationAdmission } from "@/src/features/evals/server/evaluationMutationAdmission";

describe("withAnalyticsEvaluationMutationAdmission", () => {
  beforeEach(() => {
    publicationAdmission.mockReset();
  });

  it("requires the evaluations capability for both analytics backends", async () => {
    const guard = { assertActive: vi.fn(), withIoFence: vi.fn() };
    const mutate = vi.fn().mockResolvedValue("saved");
    publicationAdmission.mockImplementationOnce(({ publish }) =>
      publish(guard),
    );

    await expect(
      withAnalyticsEvaluationMutationAdmission({
        resourceIdentity: "rule-1",
        mutate,
      }),
    ).resolves.toBe("saved");

    expect(publicationAdmission).toHaveBeenCalledWith({
      claimKind: "evaluation-config-mutation",
      resourceIdentity: "rule-1",
      supportedBackends: ["clickhouse", "doris"],
      unsupportedMessage: "",
      capabilities: ["evaluations"],
      publish: mutate,
    });
    expect(mutate).toHaveBeenCalledWith(guard);
  });

  it("does not execute a mutation when admission is rejected", async () => {
    const mutate = vi.fn();
    publicationAdmission.mockRejectedValueOnce(new Error("draining"));

    await expect(
      withAnalyticsEvaluationMutationAdmission({
        resourceIdentity: "rule-2",
        mutate,
      }),
    ).rejects.toThrow("draining");

    expect(mutate).not.toHaveBeenCalled();
  });
});
