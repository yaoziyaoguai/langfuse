import { randomUUID } from "node:crypto";

import { Prisma, type PrismaClient } from "@prisma/client";

import { prisma } from "../../db";
import type { AnalyticsBackend } from "./analyticsBackend";
import type { AnalyticsDurableProvenance } from "./analyticsDurableProvenance";
import type { AnalyticsRuntimeCapabilityContractInput } from "../repositories/analyticsRuntimeLeases";
import {
  fingerprintAnalyticsWorkloadEpoch,
  getAnalyticsBackendDeploymentState,
  resolveAnalyticsBackendStartup,
} from "../repositories/analyticsBackendDeployment";
import {
  markAnalyticsRuntimeQuiesced,
  registerAnalyticsRuntimeLease,
  renewAnalyticsRuntimeLease,
} from "../repositories/analyticsRuntimeLeases";
import {
  toPrismaAnalyticsBackend,
  type AnalyticsRuntimeComponentName,
} from "./analyticsBackendMapping";
import {
  armAnalyticsRuntimeIoLease,
  assertAnalyticsRuntimeIoAllowed,
  fenceAnalyticsRuntimeIo,
  isAnalyticsRuntimeIoFenced,
  onAnalyticsRuntimeIoFenced,
} from "./analyticsRuntimeIoFence";

type VersionRange = { readonly min: number; readonly max: number };

type AnalyticsRuntimeControllerConfig = {
  readonly client?: PrismaClient;
  readonly component: AnalyticsRuntimeComponentName;
  readonly instanceId: string;
  readonly backend: AnalyticsBackend;
  readonly workloadEpoch: string | undefined;
  readonly queueNamespaceFingerprint: string;
  readonly allowFreshInitialization?: boolean;
  readonly buildId: string;
  readonly foundationContractVersion: number;
  readonly acceptedSchemaVersion: VersionRange;
  readonly acceptedCanonicalVersion: VersionRange;
  readonly capabilityContracts: readonly AnalyticsRuntimeCapabilityContractInput[];
  readonly leaseMs: number;
  readonly onFenced?: () => void | Promise<void>;
};

type AnalyticsRuntimeInitialization =
  | { readonly mode: "LEGACY_COMPATIBILITY" }
  | {
      readonly mode: "ADOPTION_REQUIRED" | "READY";
      readonly deploymentGeneration: bigint;
      readonly runtimeLeaseId: string;
      readonly initialized: boolean;
    };

export type AnalyticsRuntimeDurableWorkState =
  | {
      readonly mode: "MANAGED";
      readonly provenance: AnalyticsDurableProvenance;
    }
  | {
      readonly mode: "LEGACY_COMPATIBILITY";
      readonly backend: AnalyticsBackend;
    }
  | { readonly mode: "UNAVAILABLE" };

async function databaseClock(
  transaction: Prisma.TransactionClient,
): Promise<Date> {
  const [row] = await transaction.$queryRaw<readonly { now: Date }[]>(
    Prisma.sql`SELECT clock_timestamp() AS now`,
  );
  if (!row || !Number.isFinite(row.now.getTime())) {
    throw new Error("Postgres did not return an analytics runtime clock");
  }
  return row.now;
}

export class AnalyticsRuntimeController {
  private readonly client: PrismaClient;
  private readonly config: AnalyticsRuntimeControllerConfig;
  private readonly runtimeLeaseIncarnationId = randomUUID();
  private runtimeLeaseId: string | null = null;
  private mode:
    | "UNINITIALIZED"
    | "LEGACY_COMPATIBILITY"
    | "ADOPTION_REQUIRED"
    | "READY"
    | "FENCED"
    | "QUIESCED" = "UNINITIALIZED";
  private deploymentGeneration: bigint | null = null;
  private heartbeat: NodeJS.Timeout | null = null;
  private renewalInFlight: Promise<boolean> | null = null;
  private fenceHandlerInvoked = false;
  private removeIoFenceListener: (() => void) | null = null;

  constructor(config: AnalyticsRuntimeControllerConfig) {
    this.client = config.client ?? prisma;
    this.config = config;
  }

  async initialize(input: {
    readonly selectedBackendEmpty: boolean;
    readonly evidenceDigest: string;
    readonly now?: Date;
  }): Promise<AnalyticsRuntimeInitialization> {
    if (this.mode !== "UNINITIALIZED") {
      throw new Error("Analytics runtime controller is already initialized");
    }
    const now = input.now;
    if (!this.config.workloadEpoch) {
      if (await getAnalyticsBackendDeploymentState({ client: this.client })) {
        throw new Error(
          "LANGFUSE_ANALYTICS_WORKLOAD_EPOCH is required by the deployment marker",
        );
      }
      this.mode = "LEGACY_COMPATIBILITY";
      return { mode: this.mode };
    }

    const workloadEpochFingerprint = fingerprintAnalyticsWorkloadEpoch(
      this.config.workloadEpoch,
    );
    const resolution = await resolveAnalyticsBackendStartup({
      client: this.client,
      backend: this.config.backend,
      workloadEpochFingerprint,
      queueNamespaceFingerprint: this.config.queueNamespaceFingerprint,
      foundationContractVersion: this.config.foundationContractVersion,
      allowFreshInitialization: this.config.allowFreshInitialization === true,
      freshDeploymentEvidence: {
        selectedBackendEmpty: input.selectedBackendEmpty,
        evidenceDigest: input.evidenceDigest,
      },
      now,
    });
    if (resolution.mode === "MISMATCH") {
      throw new Error(
        `Analytics deployment mismatch: ${resolution.reasonCode}`,
      );
    }
    const deploymentGeneration =
      resolution.mode === "READY" ? resolution.marker.generation : 0n;
    const leaseRegistrationStartedAt = performance.now();
    const registered = await registerAnalyticsRuntimeLease({
      client: this.client,
      runtimeLeaseId: this.runtimeLeaseIncarnationId,
      component: this.config.component,
      instanceId: this.config.instanceId,
      backend: this.config.backend,
      deploymentGeneration,
      workloadEpochFingerprint,
      queueNamespaceFingerprint: this.config.queueNamespaceFingerprint,
      buildId: this.config.buildId,
      foundationContractVersion: this.config.foundationContractVersion,
      acceptedSchemaVersion: this.config.acceptedSchemaVersion,
      acceptedCanonicalVersion: this.config.acceptedCanonicalVersion,
      capabilityContracts: this.config.capabilityContracts,
      leaseMs: this.config.leaseMs,
      now,
    });
    this.runtimeLeaseId = registered.lease.id;
    this.deploymentGeneration = deploymentGeneration;
    this.mode = registered.mode;
    this.removeIoFenceListener = onAnalyticsRuntimeIoFenced(() => this.fence());
    armAnalyticsRuntimeIoLease({
      startedAtMonotonicMs: leaseRegistrationStartedAt,
      leaseMs: this.config.leaseMs,
    });
    assertAnalyticsRuntimeIoAllowed();
    return {
      mode: registered.mode,
      deploymentGeneration,
      runtimeLeaseId: registered.lease.id,
      initialized: resolution.mode === "READY" && resolution.initialized,
    };
  }

  getRuntimeLeaseId(): string | null {
    return this.runtimeLeaseId;
  }

  getAdmissionContext(): {
    readonly runtimeLeaseId: string;
    readonly backend: AnalyticsBackend;
    readonly deploymentGeneration: bigint;
  } {
    assertAnalyticsRuntimeIoAllowed();
    if (
      !this.runtimeLeaseId ||
      this.deploymentGeneration === null ||
      this.mode !== "READY"
    ) {
      throw new Error(
        "Analytics runtime is not managed by a deployment marker",
      );
    }
    return {
      runtimeLeaseId: this.runtimeLeaseId,
      backend: this.config.backend,
      deploymentGeneration: this.deploymentGeneration,
    };
  }

  getDurableProvenance(): AnalyticsDurableProvenance {
    const admission = this.getAdmissionContext();
    if (!this.config.workloadEpoch) {
      throw new Error(
        "Analytics runtime is not managed by a deployment marker",
      );
    }
    return {
      analyticsBackend: admission.backend === "doris" ? "DORIS" : "CLICKHOUSE",
      deploymentGeneration: admission.deploymentGeneration,
      workloadEpochFingerprint: fingerprintAnalyticsWorkloadEpoch(
        this.config.workloadEpoch,
      ),
      runtimeContractVersion: this.config.foundationContractVersion,
      producerRuntimeLeaseId: admission.runtimeLeaseId,
    };
  }

  getDurableWorkState(): AnalyticsRuntimeDurableWorkState {
    if (this.mode === "READY") {
      return { mode: "MANAGED", provenance: this.getDurableProvenance() };
    }
    if (this.mode === "LEGACY_COMPATIBILITY") {
      return { mode: "LEGACY_COMPATIBILITY", backend: this.config.backend };
    }
    return { mode: "UNAVAILABLE" };
  }

  async renew(input?: { readonly now?: Date }): Promise<boolean> {
    if (this.renewalInFlight) return this.renewalInFlight;
    const renewal = this.performRenew(input);
    this.renewalInFlight = renewal;
    try {
      return await renewal;
    } finally {
      if (this.renewalInFlight === renewal) this.renewalInFlight = null;
    }
  }

  private async performRenew(input?: {
    readonly now?: Date;
  }): Promise<boolean> {
    if (
      !this.runtimeLeaseId ||
      this.mode === "QUIESCED" ||
      this.mode === "FENCED"
    ) {
      return false;
    }
    try {
      const renewalStartedAt = performance.now();
      const renewed = await renewAnalyticsRuntimeLease({
        client: this.client,
        runtimeLeaseId: this.runtimeLeaseId,
        leaseMs: this.config.leaseMs,
        now: input?.now,
      });
      if (renewed) {
        armAnalyticsRuntimeIoLease({
          startedAtMonotonicMs: renewalStartedAt,
          leaseMs: this.config.leaseMs,
        });
      } else {
        this.fence();
      }
      return renewed;
    } catch (error) {
      this.fence();
      throw error;
    }
  }

  private fence(): void {
    this.stopHeartbeat();
    this.mode = "FENCED";
    this.removeIoFenceListener?.();
    this.removeIoFenceListener = null;
    fenceAnalyticsRuntimeIo();
    if (this.fenceHandlerInvoked) return;
    this.fenceHandlerInvoked = true;
    try {
      const notification = this.config.onFenced?.();
      notification?.catch(() => undefined);
    } catch {
      // 即使进程内清理失败，也必须保持租约已熔断的状态。
    }
  }

  startHeartbeat(intervalMs: number): void {
    if (
      !Number.isSafeInteger(intervalMs) ||
      intervalMs < 1_000 ||
      intervalMs >= this.config.leaseMs
    ) {
      throw new TypeError("Invalid analytics runtime heartbeat interval");
    }
    if (
      !this.runtimeLeaseId ||
      this.mode === "QUIESCED" ||
      this.mode === "FENCED"
    ) {
      return;
    }
    if (this.heartbeat) throw new Error("Analytics heartbeat already started");
    this.heartbeat = setInterval(() => {
      this.renew().catch(() => false);
    }, intervalMs);
    this.heartbeat.unref();
  }

  stopHeartbeat(): void {
    if (this.heartbeat) clearInterval(this.heartbeat);
    this.heartbeat = null;
  }

  async checkReadiness(input?: { readonly now?: Date }): Promise<boolean> {
    if (this.mode === "LEGACY_COMPATIBILITY") {
      return (
        (await getAnalyticsBackendDeploymentState({ client: this.client })) ===
        null
      );
    }
    if (this.mode === "FENCED" || isAnalyticsRuntimeIoFenced()) return false;
    if (
      !this.runtimeLeaseId ||
      this.deploymentGeneration === null ||
      this.mode === "QUIESCED"
    ) {
      return false;
    }
    return this.client.$transaction(async (transaction) => {
      const [marker, lease, now] = await Promise.all([
        transaction.analyticsBackendDeploymentState.findUnique({
          where: { id: "global" },
        }),
        transaction.analyticsRuntimeLease.findUnique({
          where: { id: this.runtimeLeaseId! },
        }),
        input?.now ? Promise.resolve(input.now) : databaseClock(transaction),
      ]);
      const expectedWorkloadEpochFingerprint = this.config.workloadEpoch
        ? fingerprintAnalyticsWorkloadEpoch(this.config.workloadEpoch)
        : null;
      const leaseReady = Boolean(
        lease &&
        lease.backend === toPrismaAnalyticsBackend(this.config.backend) &&
        lease.deploymentGeneration === this.deploymentGeneration &&
        lease.workloadEpochFingerprint === expectedWorkloadEpochFingerprint &&
        lease.queueNamespaceFingerprint ===
          this.config.queueNamespaceFingerprint &&
        lease.foundationContractVersion ===
          this.config.foundationContractVersion &&
        lease.state === "ACTIVE" &&
        lease.supersededAt === null &&
        lease.leaseExpiresAt > now,
      );
      if (this.mode === "ADOPTION_REQUIRED") {
        return (
          marker === null && this.deploymentGeneration === 0n && leaseReady
        );
      }
      return Boolean(
        marker &&
        leaseReady &&
        lease &&
        marker.generation === this.deploymentGeneration &&
        marker.backend === lease.backend &&
        marker.workloadEpochFingerprint === lease.workloadEpochFingerprint &&
        marker.queueNamespaceFingerprint === lease.queueNamespaceFingerprint &&
        marker.foundationContractVersion === lease.foundationContractVersion,
      );
    });
  }

  async quiesce(input?: { readonly now?: Date }): Promise<boolean> {
    if (this.mode === "LEGACY_COMPATIBILITY" || this.mode === "QUIESCED") {
      return true;
    }
    if (!this.runtimeLeaseId) return false;
    this.fence();
    const quiesced = await markAnalyticsRuntimeQuiesced({
      client: this.client,
      runtimeLeaseId: this.runtimeLeaseId,
      now: input?.now,
    });
    if (quiesced) this.mode = "QUIESCED";
    return quiesced;
  }
}
