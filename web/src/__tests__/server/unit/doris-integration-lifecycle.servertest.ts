import { beforeEach, describe, expect, it, vi } from "vitest";
import type * as SharedServer from "@langfuse/shared/src/server";

const mocks = vi.hoisted(() => ({
  env: { LANGFUSE_ANALYTICS_BACKEND: "clickhouse" },
  admission: vi.fn(),
  fenced: vi.fn(),
  sync: vi.fn(),
}));

vi.mock("@/src/env.mjs", () => ({ env: mocks.env }));
vi.mock("@/src/server/analyticsRuntime", () => ({
  getWebAnalyticsAdmissionContext: mocks.admission,
  isWebAnalyticsRuntimeFenced: mocks.fenced,
}));
vi.mock("@langfuse/shared/src/server", async (importOriginal) => ({
  ...(await importOriginal<typeof SharedServer>()),
  syncDorisAnalyticsIntegrationConfigState: mocks.sync,
}));

import {
  getDorisIntegrationMutationAdmission,
  syncDorisIntegrationMutation,
} from "@/src/features/analytics-integrations/server/dorisIntegrationLifecycle";

describe("Doris analytics integration configuration lifecycle", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mocks.env.LANGFUSE_ANALYTICS_BACKEND = "clickhouse";
    mocks.fenced.mockReturnValue(false);
  });

  it("leaves the existing ClickHouse configuration path unchanged", async () => {
    const admission = getDorisIntegrationMutationAdmission();
    expect(admission).toBeNull();

    await syncDorisIntegrationMutation({
      transaction: {} as never,
      admissionContext: admission,
      projectId: "project-1",
      integrationType: "POSTHOG",
      enabled: true,
    });
    expect(mocks.sync).not.toHaveBeenCalled();
  });

  it("fails before mutation when the Doris runtime or capability is fenced", () => {
    mocks.env.LANGFUSE_ANALYTICS_BACKEND = "doris";
    mocks.admission.mockReturnValue(null);

    expect(() => getDorisIntegrationMutationAdmission()).toThrow(
      /not admitted/i,
    );
    mocks.admission.mockReturnValue({
      runtimeLeaseId: "web-1",
      backend: "doris",
      deploymentGeneration: 1n,
    });
    mocks.fenced.mockReturnValue(true);
    expect(() => getDorisIntegrationMutationAdmission()).toThrow(
      /not admitted/i,
    );
  });

  it("syncs the durable config state inside the caller transaction", async () => {
    const admission = {
      runtimeLeaseId: "web-1",
      backend: "doris" as const,
      deploymentGeneration: 1n,
    };
    const transaction = {};

    await syncDorisIntegrationMutation({
      transaction: transaction as never,
      admissionContext: admission,
      projectId: "project-1",
      integrationType: "BLOB_STORAGE",
      enabled: false,
    });
    expect(mocks.sync).toHaveBeenCalledWith({
      transaction,
      admissionContext: admission,
      projectId: "project-1",
      integrationType: "BLOB_STORAGE",
      enabled: false,
    });
  });
});
