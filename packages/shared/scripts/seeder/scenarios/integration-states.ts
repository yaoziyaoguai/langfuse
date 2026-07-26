import { createHash } from "node:crypto";

import { DEFAULT_SEED_PROJECT_ID } from "../defaults";
import {
  SeedError,
  type ScenarioContext,
  type ScenarioDefinition,
  type SeedSummary,
} from "./types";

const SEEDED_AT = new Date("2026-07-17T12:00:00.000Z");

const stableHash = (value: unknown): string =>
  createHash("sha256").update(JSON.stringify(value)).digest("hex");

const run = async (
  ctx: ScenarioContext,
  params: Record<string, string | number | boolean>,
): Promise<SeedSummary> => {
  const startedAt = Date.now();
  if (ctx.projectId !== DEFAULT_SEED_PROJECT_ID) {
    throw new SeedError(
      `integration-states only supports the default seed project ${DEFAULT_SEED_PROJECT_ID}`,
      "omit --project or use the default seed project",
    );
  }

  const counts = {
    posthogIntegrations: 1,
    mixpanelIntegrations: 1,
    blobStorageIntegrations: 1,
  };
  const blobExportSource =
    ctx.backend === "doris" ? "EVENTS" : "TRACES_OBSERVATIONS";
  const links = [
    `${ctx.baseUrl}/project/${ctx.projectId}/settings/integrations/posthog`,
    `${ctx.baseUrl}/project/${ctx.projectId}/settings/integrations/mixpanel`,
    `${ctx.baseUrl}/project/${ctx.projectId}/settings/integrations/blobstorage`,
  ];
  const summary = (verified: Record<string, number>): SeedSummary => ({
    scenario: "integration-states",
    target: ctx.backend,
    params,
    projectId: ctx.projectId,
    environment: ctx.environment,
    traceIds: [],
    sessionIds: [],
    counts,
    verified,
    links: ctx.dryRun ? [] : links,
    dryRun: ctx.dryRun,
    durationMs: Date.now() - startedAt,
    evidenceContractVersion: 1,
    fixtureHash: stableHash({
      projectId: ctx.projectId,
      idPrefix: ctx.idPrefix,
      posthog: {
        hostname: "https://example.invalid",
        enabled: false,
        exportSource: "EVENTS",
      },
      mixpanel: {
        region: "api",
        enabled: false,
        exportSource: "EVENTS",
      },
      blobStorage: {
        endpoint: "https://example.invalid",
        enabled: false,
        fileType: "PARQUET",
        exportMode: "FULL_HISTORY",
        exportSource: blobExportSource,
        lastError: "Seeded export failure; no outbound request was sent.",
      },
    }),
    semanticHash: null,
    operationId: null,
  });

  if (ctx.dryRun) return summary({});

  const [{ prisma }, { encrypt }] = await Promise.all([
    import("../../../src/db.js"),
    import("../../../src/encryption/index.js"),
  ]);
  const project = await prisma.project.findUnique({
    where: { id: ctx.projectId },
    select: { id: true },
  });
  if (!project) {
    throw new SeedError(
      "integration-state seed project does not exist",
      "run the standard Postgres seed first, then retry",
    );
  }

  const encryptedPosthogApiKey = encrypt("phc_seed_integration_states");
  const encryptedMixpanelProjectToken = encrypt(
    "seed-integration-states-token",
  );
  const encryptedBlobSecret = encrypt("seed-secret-access-key");

  await prisma.$transaction(async (tx) => {
    await tx.posthogIntegration.upsert({
      where: { projectId: ctx.projectId },
      create: {
        projectId: ctx.projectId,
        encryptedPosthogApiKey,
        posthogHostName: "https://example.invalid",
        lastSyncAt: SEEDED_AT,
        enabled: false,
        exportSource: "EVENTS",
      },
      update: {
        encryptedPosthogApiKey,
        posthogHostName: "https://example.invalid",
        lastSyncAt: SEEDED_AT,
        enabled: false,
        exportSource: "EVENTS",
      },
    });
    await tx.mixpanelIntegration.upsert({
      where: { projectId: ctx.projectId },
      create: {
        projectId: ctx.projectId,
        encryptedMixpanelProjectToken,
        mixpanelRegion: "api",
        lastSyncAt: SEEDED_AT,
        enabled: false,
        exportSource: "EVENTS",
      },
      update: {
        encryptedMixpanelProjectToken,
        mixpanelRegion: "api",
        lastSyncAt: SEEDED_AT,
        enabled: false,
        exportSource: "EVENTS",
      },
    });
    await tx.blobStorageIntegration.upsert({
      where: { projectId: ctx.projectId },
      create: {
        projectId: ctx.projectId,
        type: "S3_COMPATIBLE",
        bucketName: "seed-integration-states",
        prefix: "langfuse-seed/",
        accessKeyId: "seed-access-key",
        secretAccessKey: encryptedBlobSecret,
        region: "us-east-1",
        endpoint: "https://example.invalid",
        forcePathStyle: true,
        nextSyncAt: null,
        lastSyncAt: SEEDED_AT,
        enabled: false,
        exportFrequency: "daily",
        fileType: "PARQUET",
        exportMode: "FULL_HISTORY",
        exportStartDate: null,
        exportSource: blobExportSource,
        compressed: true,
        runStartedAt: null,
        lastError: "Seeded export failure; no outbound request was sent.",
        lastErrorAt: SEEDED_AT,
      },
      update: {
        type: "S3_COMPATIBLE",
        bucketName: "seed-integration-states",
        prefix: "langfuse-seed/",
        accessKeyId: "seed-access-key",
        secretAccessKey: encryptedBlobSecret,
        region: "us-east-1",
        endpoint: "https://example.invalid",
        forcePathStyle: true,
        nextSyncAt: null,
        lastSyncAt: SEEDED_AT,
        enabled: false,
        exportFrequency: "daily",
        fileType: "PARQUET",
        exportMode: "FULL_HISTORY",
        exportStartDate: null,
        exportSource: blobExportSource,
        compressed: true,
        runStartedAt: null,
        lastError: "Seeded export failure; no outbound request was sent.",
        lastErrorAt: SEEDED_AT,
      },
    });
  });

  const [posthogIntegrations, mixpanelIntegrations, blobStorageIntegrations] =
    await Promise.all([
      prisma.posthogIntegration.count({
        where: { projectId: ctx.projectId, enabled: false },
      }),
      prisma.mixpanelIntegration.count({
        where: { projectId: ctx.projectId, enabled: false },
      }),
      prisma.blobStorageIntegration.count({
        where: {
          projectId: ctx.projectId,
          enabled: false,
          lastError: { not: null },
        },
      }),
    ]);
  const verified = {
    posthogIntegrations,
    mixpanelIntegrations,
    blobStorageIntegrations,
  };
  if (
    (Object.keys(counts) as Array<keyof typeof counts>).some(
      (key) => verified[key] !== counts[key],
    )
  ) {
    throw new SeedError(
      "integration-state Postgres readback did not match the fixture",
      "confirm the local Postgres schema is current and retry",
    );
  }

  return summary(verified);
};

export const integrationStatesScenario: ScenarioDefinition = {
  name: "integration-states",
  description:
    "Backend-neutral PostHog, Mixpanel, and Blob Storage settings fixture. All integrations are disabled to guarantee zero third-party egress; Blob Storage includes a visible terminal-error state.",
  flags: [],
  supportsV4: false,
  run,
};
