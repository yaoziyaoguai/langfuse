// Test-only Doris Stream Load client for the U1 PoC.
//
// Proves the Load State and Retry Contract transport requirements against the
// pinned real Doris target: Expect: 100-continue handling, allowlisted body-
// preserving FE->BE 307 redirect (rejecting downgrade or any non-allowlisted
// origin WITHOUT forwarding auth/body), max_filter_ratio=0 strict parsing,
// duplicate-label idempotency, and unknown-response reconciliation via label.
//
// The HTTP layer uses node:http rather than undici/fetch because Doris FE
// Stream Load requires the literal `Expect: 100-continue` header, which
// undici/fetch refuse to set manually. node:http gives full header control and
// the correct 100-continue handshake (body is sent only after the server's
// 100 response) without honoring ambient HTTP_PROXY.
//
// U2 promotes a production version to packages/shared/src/server/doris/streamLoadClient.ts.

import http from "node:http";

export interface DorisPoCStreamLoadConfig {
  /** FE HTTP endpoint (Stream Load redirect origin), e.g. http://127.0.0.1:8031. */
  readonly feHttpOrigin: string;
  readonly user: string;
  readonly password?: string;
  readonly defaultDatabase: string;
  /**
   * Allowlisted FE->BE redirect targets. Key = BE origin as it appears in the
   * FE 307 Location (host:port inside the cluster network), value = the
   * reachable URL prefix the PoC rewrites it to. A Location whose origin is NOT
   * a key here is rejected before any auth/body is forwarded.
   */
  readonly beRedirectAllowlist: Readonly<Record<string, string>>;
  /** Optional injected label reconciler (wired to mysqlClient by the test). */
  readonly reconcileLabelStatus?: (
    label: string,
  ) => Promise<DorisPoCTransactionStatus>;
}

export interface DorisPoCTransactionStatus {
  readonly status: string; // PREPARE | COMMITTED | VISIBLE | ABORTED | UNKNOWN
  readonly visible: boolean;
}

export interface DorisPoCStreamLoadResult {
  /** Doris top-level Status: Success | Publish Timeout | Fail | ... */
  readonly status: string;
  readonly label: string;
  readonly message: string;
  readonly numberTotalRows: number;
  readonly numberFilteredRows: number;
  /** For a duplicate label, Doris reports the existing job's status. */
  readonly existingJobStatus?: string;
  /** True only when the load is committed AND visible. */
  readonly committed: boolean;
}

export interface DorisPoCStreamLoadRequest {
  readonly table: string;
  readonly database?: string;
  /** Newline-delimited JSON body (read_json_by_line=true). */
  readonly ndjsonBody: string;
  /** Globally collision-safe deterministic label; reuse converges idempotently. */
  readonly label: string;
  /** Column list, needed when marking __DORIS_DELETE_SIGN__ for a delete. */
  readonly columns?: readonly string[];
  readonly mergeType?: "APPEND" | "DELETE";
}

const UNKNOWN_RESPONSE_STATUSES = new Set(["Publish Timeout", "unknown", ""]);

function basicAuth(user: string, password?: string): string {
  return `Basic ${Buffer.from(`${user}:${password ?? ""}`).toString("base64")}`;
}

function originOf(url: string): string {
  const match = /^https?:\/\/([^/]+)/.exec(url);
  if (!match) {
    return "";
  }
  const authority = match[1];
  // Doris embeds the load credential as URL userinfo in the 307 Location
  // (e.g. http://root:@host:port/...). The host:port for allowlist matching is
  // the part after the last '@' in the authority.
  const at = authority.lastIndexOf("@");
  return at >= 0 ? authority.slice(at + 1) : authority;
}

/** Rebuild a Location URL against a reachable origin, dropping embedded userinfo. */
function rebuildLocation(location: string, reachableOrigin: string): string {
  const match = /^https?:\/\/[^/]+(\/.*)?$/.exec(location);
  const path = match?.[1] ?? "/";
  return `${reachableOrigin}${path}`;
}

interface RawResponse {
  readonly statusCode: number;
  readonly headers: http.IncomingHttpHeaders;
  readonly bodyText: string;
}

/** Single PUT with Expect: 100-continue via node:http. */
function putExpectContinue(
  url: string,
  headers: Record<string, string>,
  body: Buffer,
): Promise<RawResponse> {
  return new Promise((resolve, reject) => {
    const req = http.request(
      url,
      {
        method: "PUT",
        headers: { ...headers, Expect: "100-continue" },
      },
      (res) => {
        let data = "";
        res.setEncoding("utf8");
        res.on("data", (chunk) => {
          data += chunk;
        });
        res.on("end", () =>
          resolve({
            statusCode: res.statusCode ?? 0,
            headers: res.headers,
            bodyText: data,
          }),
        );
      },
    );
    req.on("error", reject);
    // Send the body only after the server signals 100 Continue. If the server
    // answers with a final response without 100, no body is sent.
    req.on("continue", () => {
      req.end(body);
    });
    // Safety: if the server neither sends 100 nor errors within a deadline, end
    // the request so the call does not hang.
    setTimeout(() => {
      if (!req.writableEnded) {
        req.end();
      }
    }, 15_000);
  });
}

export class DorisPoCStreamLoadClient {
  private readonly auth: string;
  private readonly maxRedirects = 1;

  constructor(private readonly config: DorisPoCStreamLoadConfig) {
    this.auth = basicAuth(config.user, config.password);
  }

  /**
   * Execute a Stream Load. Follows at most one allowlisted FE->BE 307 redirect,
   * preserving method/body and the load credential, and rejecting any other
   * redirect origin or downgrade without forwarding auth or body.
   */
  async streamLoad(
    req: DorisPoCStreamLoadRequest,
  ): Promise<DorisPoCStreamLoadResult> {
    const database = req.database ?? this.config.defaultDatabase;
    const url = `${this.config.feHttpOrigin}/api/${database}/${req.table}/_stream_load`;
    return this.putWithAllowlistedRedirect(url, req);
  }

  private async putWithAllowlistedRedirect(
    url: string,
    req: DorisPoCStreamLoadRequest,
    redirectDepth = 0,
  ): Promise<DorisPoCStreamLoadResult> {
    const body = Buffer.from(req.ndjsonBody, "utf8");
    const headers: Record<string, string> = {
      // Doris Stream Load contract: strict JSON-by-line parse, zero filtered rows.
      format: "json",
      read_json_by_line: "true",
      max_filter_ratio: "0",
      label: req.label,
      "Content-Length": String(body.byteLength),
      "Content-Type": "application/json",
      Authorization: this.auth,
    };
    if (req.columns && req.columns.length > 0) {
      headers.columns = req.columns.join(",");
    }
    if (req.mergeType) headers.merge_type = req.mergeType;

    const resp = await putExpectContinue(url, headers, body);

    // Allowlisted FE->BE redirect: FE returns 307 to a BE Stream Load endpoint.
    if (resp.statusCode === 307 || resp.statusCode === 308) {
      const location = resp.headers["location"];
      if (redirectDepth >= this.maxRedirects) {
        throw new Error(
          `Doris Stream Load exceeded redirect depth for label ${req.label}`,
        );
      }
      if (typeof location !== "string" || location.length === 0) {
        throw new Error(
          `Doris Stream Load 307 without Location for label ${req.label}`,
        );
      }
      // SECURITY: only forward to an allowlisted same-cluster BE origin. Any
      // other origin or a downgrade is rejected BEFORE auth/body is forwarded.
      const beOrigin = originOf(location);
      const reachable = this.config.beRedirectAllowlist[beOrigin];
      if (!reachable) {
        throw new Error(
          `Doris Stream Load redirect to non-allowlisted origin '${beOrigin}' rejected for label ${req.label}`,
        );
      }
      const rewrittenUrl = rebuildLocation(location, reachable);
      return this.putWithAllowlistedRedirect(
        rewrittenUrl,
        req,
        redirectDepth + 1,
      );
    }

    let parsed: Record<string, unknown>;
    try {
      parsed = JSON.parse(resp.bodyText) as Record<string, unknown>;
    } catch {
      throw new Error(
        `Doris Stream Load non-JSON response (HTTP ${resp.statusCode}) for label ${req.label}: ${resp.bodyText.slice(0, 200)}`,
      );
    }
    return this.toResult(parsed, req.label);
  }

  private toResult(
    parsed: Record<string, unknown>,
    requestedLabel: string,
  ): DorisPoCStreamLoadResult {
    const status = String(parsed.Status ?? parsed.status ?? "");
    return {
      status,
      label: String(parsed.Label ?? parsed.label ?? requestedLabel),
      message: String(parsed.Message ?? parsed.msg ?? ""),
      numberTotalRows: Number(parsed.NumberTotalRows ?? 0),
      numberFilteredRows: Number(parsed.NumberFilteredRows ?? 0),
      existingJobStatus:
        parsed.ExistingJobStatus !== undefined
          ? String(parsed.ExistingJobStatus)
          : undefined,
      // A load is committed only when Doris reports Success (VISIBLE) with no
      // filtered rows; Publish Timeout / Fail / unknown are NOT committed.
      committed: status === "Success",
    };
  }

  /**
   * Reconcile an unknown Stream Load outcome by checking the label's transaction
   * state through the MySQL protocol. An unknown response is NEVER classified as
   * success or failure until this resolves; label-expiry-without-proof is
   * treated as needs_reconcile, never as "absent".
   */
  async reconcile(req: {
    readonly label: string;
  }): Promise<DorisPoCTransactionStatus> {
    if (!this.config.reconcileLabelStatus) {
      throw new Error(
        `Cannot reconcile label ${req.label}: no label-status reconciler configured`,
      );
    }
    return this.config.reconcileLabelStatus(req.label);
  }

  /** True when a Stream Load result is ambiguous and must be reconciled. */
  isUnknownOutcome(result: DorisPoCStreamLoadResult): boolean {
    return UNKNOWN_RESPONSE_STATUSES.has(result.status);
  }
}

/** Convenience export so callers can build collision-safe deterministic labels. */
export function dorisLabel(parts: readonly string[]): string {
  // Doris label charset: letters, digits, _, -. Deterministic and collision-safe
  // across operation/batch/attempt, matching the Load State contract.
  return parts
    .join("-")
    .replace(/[^A-Za-z0-9_-]/g, "_")
    .slice(0, 128);
}
