import net from "node:net";

import { z } from "zod";

export type DorisNodeEnv = "development" | "test" | "production";

/** The bundled single-node compose is intentionally non-TLS and must never be
 * confused with a production Doris deployment. Production remains strict by
 * default; this explicit escape hatch exists only for the local compose. */
export function resolveDorisNodeEnv(
  nodeEnv: DorisNodeEnv,
  localDevMode: string | undefined,
): DorisNodeEnv {
  return localDevMode === "true" ? "development" : nodeEnv;
}

type DorisEnv = Readonly<Record<string, string | undefined>>;

export interface DorisQueryConfig {
  readonly host: string;
  readonly port: number;
  readonly database: string;
  readonly user: string;
  readonly password: string;
  readonly tls: boolean;
  readonly tlsCaPath?: string;
  readonly maxConnections: number;
  readonly connectTimeoutMs: number;
  readonly queryTimeoutMs: number;
}

export interface DorisStreamLoadRuntimeConfig {
  readonly feOrigin: string;
  readonly database: string;
  readonly user: string;
  readonly password: string;
  readonly requireTls: boolean;
  readonly allowedFeAddresses: readonly string[];
  readonly allowedRedirectOrigins: readonly string[];
  readonly allowedRedirectAddresses: readonly string[];
  readonly tlsCaPath?: string;
  readonly requestTimeoutMs: number;
  readonly maxBodyBytes: number;
}

const queryEnvSchema = z.object({
  DORIS_QUERY_URL: z.string().min(1),
  DORIS_QUERY_USER: z.string().min(1),
  DORIS_QUERY_PASSWORD: z.string().default(""),
  DORIS_QUERY_TLS_ENABLED: z.enum(["true", "false"]).default("false"),
  DORIS_QUERY_TLS_CA_PATH: z.string().min(1).optional(),
  DORIS_QUERY_MAX_CONNECTIONS: z.coerce.number().int().positive().default(25),
  DORIS_QUERY_CONNECT_TIMEOUT_MS: z.coerce
    .number()
    .int()
    .positive()
    .default(10_000),
  DORIS_QUERY_TIMEOUT_MS: z.coerce.number().int().positive().default(30_000),
});

const streamLoadEnvSchema = z.object({
  DORIS_STREAM_LOAD_FE_URL: z.string().min(1),
  DORIS_STREAM_LOAD_USER: z.string().min(1),
  DORIS_STREAM_LOAD_PASSWORD: z.string().default(""),
  DORIS_STREAM_LOAD_DATABASE: z.string().min(1).default("langfuse"),
  DORIS_STREAM_LOAD_FE_IP_ALLOWLIST: z.string().default(""),
  DORIS_STREAM_LOAD_BE_ALLOWLIST: z.string().min(1),
  DORIS_STREAM_LOAD_BE_IP_ALLOWLIST: z.string().default(""),
  DORIS_STREAM_LOAD_TLS_CA_PATH: z.string().min(1).optional(),
  DORIS_STREAM_LOAD_REQUEST_TIMEOUT_MS: z.coerce
    .number()
    .int()
    .positive()
    .default(30_000),
  DORIS_STREAM_LOAD_MAX_BODY_BYTES: z.coerce
    .number()
    .int()
    .min(100 * 1024 * 1024)
    .default(100 * 1024 * 1024),
  DORIS_QUERY_USER: z.string().optional(),
});

function parseMysqlEndpoint(value: string): URL {
  const url = new URL(value);
  if (
    url.protocol !== "mysql:" ||
    !url.hostname ||
    !url.pathname.slice(1) ||
    url.username ||
    url.password ||
    url.search ||
    url.hash
  ) {
    throw new Error(
      "DORIS_QUERY_URL must be mysql://host:port/database without embedded credentials",
    );
  }
  return url;
}

function parseOrigin(value: string, variableName: string): URL {
  const url = new URL(value);
  if (
    !["http:", "https:"].includes(url.protocol) ||
    !url.hostname ||
    url.username ||
    url.password ||
    (url.pathname !== "/" && url.pathname !== "") ||
    url.search ||
    url.hash
  ) {
    throw new Error(`${variableName} must contain only an HTTP(S) origin`);
  }
  return url;
}

function commaSeparated(value: string): string[] {
  return [
    ...new Set(
      value
        .split(",")
        .map((item) => item.trim())
        .filter(Boolean),
    ),
  ];
}

function parseIpAllowlist(value: string, variableName: string): string[] {
  const addresses = commaSeparated(value);
  for (const address of addresses) {
    if (!net.isIP(address)) {
      throw new Error(
        `${variableName} accepts only literal IPv4/IPv6 addresses`,
      );
    }
  }
  return addresses;
}

export function parseDorisQueryConfig(
  input: DorisEnv,
  nodeEnv: DorisNodeEnv,
): DorisQueryConfig {
  const parsed = queryEnvSchema.parse(input);
  const endpoint = parseMysqlEndpoint(parsed.DORIS_QUERY_URL);
  const tls = parsed.DORIS_QUERY_TLS_ENABLED === "true";

  if (nodeEnv === "production") {
    if (!tls) {
      throw new Error(
        "Production Doris query connections require verified TLS",
      );
    }
    if (parsed.DORIS_QUERY_USER.toLowerCase() === "root") {
      throw new Error(
        "Production Doris query connections require a least-privilege identity",
      );
    }
    if (!parsed.DORIS_QUERY_PASSWORD) {
      throw new Error("Production Doris query credentials require a password");
    }
    if (net.isIP(endpoint.hostname)) {
      throw new Error(
        "Production Doris query TLS requires a DNS hostname for identity verification",
      );
    }
  }

  return {
    host: endpoint.hostname,
    port: Number(endpoint.port || 9030),
    database: decodeURIComponent(endpoint.pathname.slice(1)),
    user: parsed.DORIS_QUERY_USER,
    password: parsed.DORIS_QUERY_PASSWORD,
    tls,
    tlsCaPath: parsed.DORIS_QUERY_TLS_CA_PATH,
    maxConnections: parsed.DORIS_QUERY_MAX_CONNECTIONS,
    connectTimeoutMs: parsed.DORIS_QUERY_CONNECT_TIMEOUT_MS,
    queryTimeoutMs: parsed.DORIS_QUERY_TIMEOUT_MS,
  };
}

export function parseDorisStreamLoadConfig(
  input: DorisEnv,
  nodeEnv: DorisNodeEnv,
): DorisStreamLoadRuntimeConfig {
  const parsed = streamLoadEnvSchema.parse(input);
  const fe = parseOrigin(
    parsed.DORIS_STREAM_LOAD_FE_URL,
    "DORIS_STREAM_LOAD_FE_URL",
  );
  const allowedRedirectOrigins = commaSeparated(
    parsed.DORIS_STREAM_LOAD_BE_ALLOWLIST,
  ).map((value) => parseOrigin(value, "DORIS_STREAM_LOAD_BE_ALLOWLIST").origin);
  const allowedFeAddresses = parseIpAllowlist(
    parsed.DORIS_STREAM_LOAD_FE_IP_ALLOWLIST,
    "DORIS_STREAM_LOAD_FE_IP_ALLOWLIST",
  );
  const allowedRedirectAddresses = parseIpAllowlist(
    parsed.DORIS_STREAM_LOAD_BE_IP_ALLOWLIST,
    "DORIS_STREAM_LOAD_BE_IP_ALLOWLIST",
  );

  if (nodeEnv === "production") {
    if (fe.protocol !== "https:") {
      throw new Error("Production Doris Stream Load requires verified TLS");
    }
    if (
      allowedRedirectOrigins.some((origin) => !origin.startsWith("https://"))
    ) {
      throw new Error(
        "Production Doris Stream Load redirects require verified TLS",
      );
    }
    if (allowedRedirectOrigins.length === 0) {
      throw new Error(
        "Production Doris Stream Load requires a redirect origin allowlist",
      );
    }
    if (allowedRedirectAddresses.length === 0) {
      throw new Error(
        "Production Doris Stream Load requires a resolved BE IP allowlist",
      );
    }
    if (allowedFeAddresses.length === 0) {
      throw new Error(
        "Production Doris Stream Load requires a resolved FE IP allowlist",
      );
    }
    if (parsed.DORIS_STREAM_LOAD_USER.toLowerCase() === "root") {
      throw new Error(
        "Production Doris Stream Load requires a least-privilege identity",
      );
    }
    if (!parsed.DORIS_STREAM_LOAD_PASSWORD) {
      throw new Error("Production Doris Stream Load requires a password");
    }
  }

  if (
    nodeEnv === "production" &&
    parsed.DORIS_QUERY_USER &&
    parsed.DORIS_QUERY_USER === parsed.DORIS_STREAM_LOAD_USER
  ) {
    throw new Error("Doris query and Stream Load must use separate identities");
  }

  return {
    feOrigin: fe.origin,
    database: parsed.DORIS_STREAM_LOAD_DATABASE,
    user: parsed.DORIS_STREAM_LOAD_USER,
    password: parsed.DORIS_STREAM_LOAD_PASSWORD,
    requireTls: nodeEnv === "production",
    allowedFeAddresses,
    allowedRedirectOrigins,
    allowedRedirectAddresses,
    tlsCaPath: parsed.DORIS_STREAM_LOAD_TLS_CA_PATH,
    requestTimeoutMs: parsed.DORIS_STREAM_LOAD_REQUEST_TIMEOUT_MS,
    maxBodyBytes: parsed.DORIS_STREAM_LOAD_MAX_BODY_BYTES,
  };
}

/** Never echo a driver message: it may contain topology, SQL, auth, or payload. */
export function redactDorisErrorMessage(_message: string): string {
  return "Doris request failed";
}
