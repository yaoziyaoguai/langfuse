import { beforeEach, describe, expect, it, vi } from "vitest";

const { admissionMock, mutationAdmissionMock } = vi.hoisted(() => ({
  admissionMock: vi.fn(),
  mutationAdmissionMock: vi.fn(),
}));

vi.mock("@/src/server/analyticsQueuePublicationAdmission", () => ({
  withAnalyticsBatchActionPublicationAdmission: admissionMock,
}));
vi.mock("@/src/features/evals/server/evaluationMutationAdmission", () => ({
  withAnalyticsEvaluationMutationAdmission: mutationAdmissionMock,
}));

import { runHistoricalEvaluationMutation } from "@/src/features/evals/server/historicalEvaluationAdmission";

describe("runHistoricalEvaluationMutation", () => {
  beforeEach(() => {
    admissionMock.mockReset();
    mutationAdmissionMock.mockReset();
  });

  it("does not start the mutation when historical publication admission is rejected", async () => {
    const mutate = vi.fn();
    admissionMock.mockRejectedValueOnce(new Error("admission rejected"));

    await expect(
      runHistoricalEvaluationMutation({
        scheduleHistoricalEvaluation: true,
        resourceIdentity: "event-id",
        mutate,
      }),
    ).rejects.toThrow("admission rejected");

    expect(mutate).not.toHaveBeenCalled();
    expect(admissionMock).toHaveBeenCalledWith({
      actionId: "eval-create",
      resourceIdentity: "event-id",
      publish: mutate,
    });
  });

  it("runs non-historical mutations behind evaluation mutation admission", async () => {
    const guard = { assertActive: vi.fn(), withIoFence: vi.fn() };
    const mutate = vi.fn().mockResolvedValueOnce("updated");
    mutationAdmissionMock.mockImplementationOnce(({ mutate: run }) =>
      run(guard),
    );

    await expect(
      runHistoricalEvaluationMutation({
        scheduleHistoricalEvaluation: false,
        resourceIdentity: "event-id",
        mutate,
      }),
    ).resolves.toBe("updated");

    expect(admissionMock).not.toHaveBeenCalled();
    expect(mutationAdmissionMock).toHaveBeenCalledWith({
      resourceIdentity: "event-id",
      mutate,
    });
    expect(mutate).toHaveBeenCalledWith(guard);
  });

  it("passes the admitted publication guard to historical mutations", async () => {
    const guard = { assertActive: vi.fn(), withIoFence: vi.fn() };
    admissionMock.mockImplementationOnce(({ publish }) => publish(guard));
    const mutate = vi.fn().mockResolvedValueOnce("created");

    await expect(
      runHistoricalEvaluationMutation({
        scheduleHistoricalEvaluation: true,
        resourceIdentity: "event-id",
        mutate,
      }),
    ).resolves.toBe("created");

    expect(mutate).toHaveBeenCalledWith(guard);
  });
});
