import dns from "node:dns/promises";
import { readFileSync } from "node:fs";
import http from "node:http";
import https from "node:https";
import net from "node:net";
import type { LookupFunction } from "node:net";

import {
  assertAnalyticsRuntimeIoAllowed,
  onAnalyticsRuntimeIoFenced,
} from "../analytics-persistence/analyticsRuntimeIoFence";
import { DorisError, toDorisError } from "./errors";

export interface DorisStreamLoadConfig {
  readonly feOrigin: string;
  readonly database: string;
  readonly user: string;
  readonly password: string;
  readonly requireTls: boolean;
  readonly allowedFeAddresses?: readonly string[];
  readonly allowedRedirectOrigins: readonly string[];
  readonly allowedRedirectAddresses?: readonly string[];
  readonly redirectOriginRewriteMap?: Readonly<Record<string, string>>;
  readonly allowedRewriteAddresses?: readonly string[];
  readonly tlsCaPath?: string;
  readonly requestTimeoutMs?: number;
  readonly maxBodyBytes?: number;
  readonly resolveAddresses?: (hostname: string) => Promise<readonly string[]>;
  readonly reconcileLabelStatus?: (
    label: string,
  ) => Promise<DorisStreamLoadReconciliation>;
}

export interface DorisStreamLoadReconciliation {
  readonly status: string;
  readonly visible: boolean;
}

export interface DorisStreamLoadRequest {
  readonly table: string;
  readonly database?: string;
  readonly label: string;
  readonly ndjsonBody: string | Buffer;
  readonly columns?: readonly string[];
  readonly mergeType?: "APPEND" | "DELETE";
}

export interface DorisStreamLoadResult {
  readonly status: string;
  readonly label: string;
  readonly numberTotalRows: number;
  readonly numberFilteredRows: number;
  readonly committed: boolean;
  readonly requiresReconciliation: boolean;
}

interface RawResponse {
  readonly statusCode: number;
  readonly headers: http.IncomingHttpHeaders;
  readonly bodyText: string;
}

const IDENTIFIER = /^[A-Za-z_][A-Za-z0-9_]*$/;
const LABEL = /^[A-Za-z0-9_-]{1,128}$/;
const UNKNOWN_OUTCOMES = new Set([
  "PUBLISH TIMEOUT",
  "LABEL ALREADY EXISTS",
  "UNKNOWN",
  "",
]);
const RETRYABLE_REJECTION_MARKERS = ["MEM_LIMIT_EXCEEDED"] as const;
const RECONCILIATION_STATES = new Set([
  "UNKNOWN",
  "PREPARE",
  "COMMITTED",
  "VISIBLE",
  "ABORTED",
]);

function basicAuth(user: string, password: string): string {
  return `Basic ${Buffer.from(`${user}:${password}`).toString("base64")}`;
}

function pinnedLookup(address: string): LookupFunction {
  const family = net.isIP(address);
  return (_hostname, options, callback) => {
    callback(
      null,
      options.all ? [{ address, family }] : address,
      options.all ? undefined : family,
    );
  };
}

function asNonNegativeInteger(value: unknown): number {
  const number = Number(value ?? 0);
  if (!Number.isSafeInteger(number) || number < 0) {
    throw new DorisError("LOAD_REJECTED", false);
  }
  return number;
}

function isRetryableLoadRejection(parsed: Record<string, unknown>): boolean {
  const message = parsed.Message ?? parsed.message;
  return (
    typeof message === "string" &&
    RETRYABLE_REJECTION_MARKERS.some((marker) => message.includes(marker))
  );
}

async function defaultResolveAddresses(hostname: string): Promise<string[]> {
  if (net.isIP(hostname)) return [hostname];
  return (await dns.lookup(hostname, { all: true, verbatim: true })).map(
    ({ address }) => address,
  );
}

export class DorisStreamLoadClient {
  private readonly timeoutMs: number;
  private readonly maxBodyBytes: number;
  private readonly resolveAddresses: (
    hostname: string,
  ) => Promise<readonly string[]>;
  private readonly tlsCa?: string;

  constructor(private readonly config: DorisStreamLoadConfig) {
    this.timeoutMs = config.requestTimeoutMs ?? 30_000;
    this.maxBodyBytes = config.maxBodyBytes ?? 100 * 1024 * 1024;
    this.resolveAddresses = config.resolveAddresses ?? defaultResolveAddresses;
    try {
      this.tlsCa = config.tlsCaPath
        ? readFileSync(config.tlsCaPath, "utf8")
        : undefined;
    } catch (error) {
      throw toDorisError(error);
    }
  }

  async load(request: DorisStreamLoadRequest): Promise<DorisStreamLoadResult> {
    assertAnalyticsRuntimeIoAllowed();
    const database = request.database ?? this.config.database;
    if (
      !IDENTIFIER.test(database) ||
      !IDENTIFIER.test(request.table) ||
      !LABEL.test(request.label) ||
      request.columns?.some((column) => !IDENTIFIER.test(column))
    ) {
      throw new DorisError("INVALID_REQUEST", false);
    }
    const body = Buffer.isBuffer(request.ndjsonBody)
      ? request.ndjsonBody
      : Buffer.from(request.ndjsonBody, "utf8");
    if (body.byteLength === 0 || body.byteLength > this.maxBodyBytes) {
      throw new DorisError("INVALID_REQUEST", false);
    }

    const initialUrl = new URL(
      `/api/${database}/${request.table}/_stream_load`,
      this.config.feOrigin,
    );
    const pinnedFeAddress = await this.resolvePinnedAddress(
      initialUrl.hostname,
      this.config.allowedFeAddresses ?? [],
      this.config.requireTls,
    );
    assertAnalyticsRuntimeIoAllowed();
    const response = await this.put(
      initialUrl,
      request,
      body,
      0,
      pinnedFeAddress,
    );
    assertAnalyticsRuntimeIoAllowed();
    return this.parseResponse(response, request.label);
  }

  async reconcile(input: {
    readonly label: string;
    readonly database?: string;
  }): Promise<DorisStreamLoadReconciliation> {
    assertAnalyticsRuntimeIoAllowed();
    const database = input.database ?? this.config.database;
    if (!LABEL.test(input.label) || !IDENTIFIER.test(database)) {
      throw new DorisError("INVALID_REQUEST", false);
    }
    if (this.config.reconcileLabelStatus) {
      try {
        const reconciliation = await this.config.reconcileLabelStatus(
          input.label,
        );
        assertAnalyticsRuntimeIoAllowed();
        return reconciliation;
      } catch (error) {
        throw toDorisError(error);
      }
    }

    try {
      const url = new URL(
        `/api/${database}/get_load_state`,
        this.config.feOrigin,
      );
      url.searchParams.set("label", input.label);
      const pinnedAddress = await this.resolvePinnedAddress(
        url.hostname,
        this.config.allowedFeAddresses ?? [],
        this.config.requireTls,
      );
      assertAnalyticsRuntimeIoAllowed();
      const response = await this.getOnce(url, pinnedAddress);
      assertAnalyticsRuntimeIoAllowed();
      if (response.statusCode < 200 || response.statusCode >= 300) {
        throw new DorisError("ANALYTICS_UNAVAILABLE", true);
      }
      const parsed = JSON.parse(response.bodyText) as Record<string, unknown>;
      const status = String(parsed.data ?? "").toUpperCase();
      if (Number(parsed.code) !== 0 || !RECONCILIATION_STATES.has(status)) {
        throw new DorisError("LOAD_REJECTED", false);
      }
      return { status, visible: status === "VISIBLE" };
    } catch (error) {
      throw toDorisError(error);
    }
  }

  private async resolvePinnedAddress(
    hostname: string,
    allowedAddresses: readonly string[],
    required: boolean,
  ): Promise<string | undefined> {
    if (allowedAddresses.length === 0 && !required) return undefined;
    if (allowedAddresses.length === 0) {
      throw new DorisError("REDIRECT_REJECTED", false);
    }
    try {
      const addresses = await this.resolveAddresses(hostname);
      if (
        addresses.length === 0 ||
        addresses.some((address) => !allowedAddresses.includes(address))
      ) {
        throw new DorisError("REDIRECT_REJECTED", false);
      }
      return addresses[0];
    } catch (error) {
      if (error instanceof DorisError) throw error;
      throw new DorisError("REDIRECT_REJECTED", false);
    }
  }

  private async put(
    url: URL,
    request: DorisStreamLoadRequest,
    body: Buffer,
    redirectDepth = 0,
    pinnedAddress?: string,
  ): Promise<RawResponse> {
    assertAnalyticsRuntimeIoAllowed();
    if (this.config.requireTls && url.protocol !== "https:") {
      throw new DorisError("REDIRECT_REJECTED", false);
    }
    const response = await this.putOnce(url, request, body, pinnedAddress);
    assertAnalyticsRuntimeIoAllowed();
    if (response.statusCode !== 307) return response;
    if (redirectDepth > 0) {
      throw new DorisError("REDIRECT_REJECTED", false);
    }

    const location = response.headers.location;
    if (!location) throw new DorisError("REDIRECT_REJECTED", false);
    let redirect: URL;
    try {
      redirect = new URL(location, url);
    } catch (_error) {
      throw new DorisError("REDIRECT_REJECTED", false);
    }
    // Doris may advertise `user:@host`; never trust redirect credentials.
    // The client supplies its configured load identity after origin/IP checks.
    redirect.username = "";
    redirect.password = "";
    if (
      !["http:", "https:"].includes(redirect.protocol) ||
      !redirect.hostname ||
      redirect.hostname.includes("*") ||
      redirect.hash ||
      (this.config.requireTls && redirect.protocol !== "https:") ||
      !this.config.allowedRedirectOrigins.includes(redirect.origin)
    ) {
      throw new DorisError("REDIRECT_REJECTED", false);
    }

    const rewriteOrigin =
      this.config.redirectOriginRewriteMap?.[redirect.origin];
    const allowedAddresses = this.config.allowedRedirectAddresses ?? [];
    const pinnedOriginalRedirectAddress = await this.resolvePinnedAddress(
      redirect.hostname,
      allowedAddresses,
      this.config.requireTls || rewriteOrigin !== undefined,
    );

    let requestUrl = redirect;
    let pinnedRequestAddress = pinnedOriginalRedirectAddress;
    if (rewriteOrigin !== undefined) {
      let targetOrigin: URL;
      try {
        targetOrigin = new URL(rewriteOrigin);
      } catch (_error) {
        throw new DorisError("REDIRECT_REJECTED", false);
      }
      if (
        !["http:", "https:"].includes(targetOrigin.protocol) ||
        !targetOrigin.hostname ||
        targetOrigin.hostname.includes("*") ||
        targetOrigin.username ||
        targetOrigin.password ||
        (targetOrigin.pathname !== "/" && targetOrigin.pathname !== "") ||
        targetOrigin.search ||
        targetOrigin.hash ||
        targetOrigin.origin !== rewriteOrigin ||
        (redirect.protocol === "https:" &&
          targetOrigin.protocol !== "https:") ||
        (this.config.requireTls && targetOrigin.protocol !== "https:") ||
        Object.hasOwn(
          this.config.redirectOriginRewriteMap ?? {},
          targetOrigin.origin,
        )
      ) {
        throw new DorisError("REDIRECT_REJECTED", false);
      }
      pinnedRequestAddress = await this.resolvePinnedAddress(
        targetOrigin.hostname,
        this.config.allowedRewriteAddresses ?? [],
        true,
      );
      requestUrl = new URL(
        `${redirect.pathname}${redirect.search}`,
        targetOrigin.origin,
      );
    }

    return this.put(
      requestUrl,
      request,
      body,
      redirectDepth + 1,
      pinnedRequestAddress,
    );
  }

  private putOnce(
    url: URL,
    request: DorisStreamLoadRequest,
    body: Buffer,
    pinnedAddress?: string,
  ): Promise<RawResponse> {
    assertAnalyticsRuntimeIoAllowed();
    return new Promise((resolve, reject) => {
      let settled = false;
      let removeFenceListener: () => void = () => undefined;
      const finish = (callback: () => void) => {
        if (settled) return;
        settled = true;
        removeFenceListener();
        callback();
      };
      const transport = url.protocol === "https:" ? https : http;
      const headers: Record<string, string> = {
        Authorization: basicAuth(this.config.user, this.config.password),
        "Content-Length": String(body.byteLength),
        "Content-Type": "application/json",
        Expect: "100-continue",
        format: "json",
        label: request.label,
        max_filter_ratio: "0",
        read_json_by_line: "true",
      };
      if (request.columns?.length) headers.columns = request.columns.join(",");
      if (request.mergeType) headers.merge_type = request.mergeType;

      const req = transport.request(
        url,
        {
          method: "PUT",
          headers,
          rejectUnauthorized: true,
          ca: this.tlsCa,
          servername: url.hostname,
          lookup: pinnedAddress ? pinnedLookup(pinnedAddress) : undefined,
        },
        (res) => {
          let responseBody = "";
          res.setEncoding("utf8");
          res.on("data", (chunk: string) => {
            responseBody += chunk;
            if (Buffer.byteLength(responseBody, "utf8") > 65_536) {
              req.destroy(new Error("Doris response exceeded safe limit"));
            }
          });
          res.on("end", () =>
            finish(() =>
              resolve({
                statusCode: res.statusCode ?? 0,
                headers: res.headers,
                bodyText: responseBody,
              }),
            ),
          );
        },
      );
      req.setTimeout(this.timeoutMs, () =>
        req.destroy(new Error("Doris Stream Load timed out")),
      );
      req.on("continue", () => req.end(body));
      req.on("error", (error) => finish(() => reject(toDorisError(error))));
      removeFenceListener = onAnalyticsRuntimeIoFenced(() =>
        req.destroy(new DorisError("ANALYTICS_UNAVAILABLE", true)),
      );
      req.flushHeaders();
    });
  }

  private getOnce(url: URL, pinnedAddress?: string): Promise<RawResponse> {
    assertAnalyticsRuntimeIoAllowed();
    return new Promise((resolve, reject) => {
      let settled = false;
      let removeFenceListener: () => void = () => undefined;
      const finish = (callback: () => void) => {
        if (settled) return;
        settled = true;
        removeFenceListener();
        callback();
      };
      const transport = url.protocol === "https:" ? https : http;
      const req = transport.request(
        url,
        {
          method: "GET",
          headers: {
            Authorization: basicAuth(this.config.user, this.config.password),
          },
          rejectUnauthorized: true,
          ca: this.tlsCa,
          servername: url.hostname,
          lookup: pinnedAddress ? pinnedLookup(pinnedAddress) : undefined,
        },
        (res) => {
          let responseBody = "";
          res.setEncoding("utf8");
          res.on("data", (chunk: string) => {
            responseBody += chunk;
            if (Buffer.byteLength(responseBody, "utf8") > 65_536) {
              req.destroy(new Error("Doris response exceeded safe limit"));
            }
          });
          res.on("end", () =>
            finish(() =>
              resolve({
                statusCode: res.statusCode ?? 0,
                headers: res.headers,
                bodyText: responseBody,
              }),
            ),
          );
        },
      );
      req.setTimeout(this.timeoutMs, () =>
        req.destroy(new Error("Doris load reconciliation timed out")),
      );
      req.on("error", (error) => finish(() => reject(toDorisError(error))));
      removeFenceListener = onAnalyticsRuntimeIoFenced(() =>
        req.destroy(new DorisError("ANALYTICS_UNAVAILABLE", true)),
      );
      req.end();
    });
  }

  private parseResponse(
    response: RawResponse,
    requestedLabel: string,
  ): DorisStreamLoadResult {
    let parsed: Record<string, unknown>;
    try {
      parsed = JSON.parse(response.bodyText) as Record<string, unknown>;
    } catch (_error) {
      throw new DorisError("LOAD_REJECTED", false);
    }
    const status = String(parsed.Status ?? parsed.status ?? "");
    const responseLabel = String(
      parsed.Label ?? parsed.label ?? requestedLabel,
    );
    if (responseLabel !== requestedLabel) {
      throw new DorisError("LOAD_REJECTED", false);
    }
    const numberTotalRows = asNonNegativeInteger(parsed.NumberTotalRows);
    const numberFilteredRows = asNonNegativeInteger(parsed.NumberFilteredRows);
    if (numberFilteredRows > 0) {
      throw new DorisError("FILTERED_ROWS", false);
    }
    const httpSuccess = response.statusCode >= 200 && response.statusCode < 300;
    const requiresReconciliation =
      UNKNOWN_OUTCOMES.has(status.toUpperCase()) ||
      (status === "Success" && !httpSuccess);
    if (status !== "Success" && !requiresReconciliation) {
      if (isRetryableLoadRejection(parsed)) {
        throw new DorisError("ANALYTICS_UNAVAILABLE", true);
      }
      throw new DorisError("LOAD_REJECTED", false);
    }
    return {
      status,
      label: responseLabel,
      numberTotalRows,
      numberFilteredRows,
      committed: status === "Success" && httpSuccess,
      requiresReconciliation,
    };
  }
}
