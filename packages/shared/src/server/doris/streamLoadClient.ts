import dns from "node:dns/promises";
import { readFileSync } from "node:fs";
import http from "node:http";
import https from "node:https";
import net from "node:net";

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

function asNonNegativeInteger(value: unknown): number {
  const number = Number(value ?? 0);
  if (!Number.isSafeInteger(number) || number < 0) {
    throw new DorisError("LOAD_REJECTED", false);
  }
  return number;
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
    const response = await this.put(
      initialUrl,
      request,
      body,
      0,
      pinnedFeAddress,
    );
    return this.parseResponse(response, request.label);
  }

  async reconcile(input: {
    readonly label: string;
    readonly database?: string;
  }): Promise<DorisStreamLoadReconciliation> {
    const database = input.database ?? this.config.database;
    if (!LABEL.test(input.label) || !IDENTIFIER.test(database)) {
      throw new DorisError("INVALID_REQUEST", false);
    }
    if (this.config.reconcileLabelStatus) {
      try {
        return await this.config.reconcileLabelStatus(input.label);
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
      const response = await this.getOnce(url, pinnedAddress);
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
    if (this.config.requireTls && url.protocol !== "https:") {
      throw new DorisError("REDIRECT_REJECTED", false);
    }
    const response = await this.putOnce(url, request, body, pinnedAddress);
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
    redirect.username = "";
    redirect.password = "";
    if (
      (this.config.requireTls && redirect.protocol !== "https:") ||
      !this.config.allowedRedirectOrigins.includes(redirect.origin)
    ) {
      throw new DorisError("REDIRECT_REJECTED", false);
    }

    const allowedAddresses = this.config.allowedRedirectAddresses ?? [];
    const pinnedRedirectAddress = await this.resolvePinnedAddress(
      redirect.hostname,
      allowedAddresses,
      this.config.requireTls,
    );

    return this.put(
      redirect,
      request,
      body,
      redirectDepth + 1,
      pinnedRedirectAddress,
    );
  }

  private putOnce(
    url: URL,
    request: DorisStreamLoadRequest,
    body: Buffer,
    pinnedAddress?: string,
  ): Promise<RawResponse> {
    return new Promise((resolve, reject) => {
      let settled = false;
      const finish = (callback: () => void) => {
        if (settled) return;
        settled = true;
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

      const req = transport.request(
        url,
        {
          method: "PUT",
          headers,
          rejectUnauthorized: true,
          ca: this.tlsCa,
          servername: url.hostname,
          lookup: pinnedAddress
            ? (_hostname, _options, callback) =>
                callback(null, pinnedAddress, net.isIP(pinnedAddress) as 4 | 6)
            : undefined,
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
      req.flushHeaders();
    });
  }

  private getOnce(url: URL, pinnedAddress?: string): Promise<RawResponse> {
    return new Promise((resolve, reject) => {
      let settled = false;
      const finish = (callback: () => void) => {
        if (settled) return;
        settled = true;
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
          lookup: pinnedAddress
            ? (_hostname, _options, callback) =>
                callback(null, pinnedAddress, net.isIP(pinnedAddress) as 4 | 6)
            : undefined,
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
