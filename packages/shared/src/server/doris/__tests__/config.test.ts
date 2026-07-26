import { describe, expect, it } from "vitest";

import {
  parseDorisQueryConfig,
  parseDorisStreamLoadConfig,
  redactDorisErrorMessage,
  resolveDorisNodeEnv,
} from "../config";
import { toDorisError } from "../errors";

describe("Doris runtime configuration", () => {
  it("never allows local development mode to weaken production policy", () => {
    expect(() => resolveDorisNodeEnv("production", "true")).toThrow(
      /local development mode.*production/i,
    );
    expect(resolveDorisNodeEnv("development", "true")).toBe("development");
    expect(resolveDorisNodeEnv("test", "true")).toBe("development");
  });

  it("accepts explicit local query settings in development", () => {
    expect(
      parseDorisQueryConfig(
        {
          DORIS_QUERY_URL: "mysql://127.0.0.1:9031/langfuse",
          DORIS_QUERY_USER: "root",
          DORIS_QUERY_PASSWORD: "local-only",
          DORIS_QUERY_TLS_ENABLED: "false",
          DORIS_QUERY_TIMEOUT_MS: "15000",
        },
        "development",
      ),
    ).toMatchObject({
      host: "127.0.0.1",
      port: 9031,
      database: "langfuse",
      user: "root",
      tls: false,
      queryTimeoutMs: 15_000,
    });
  });

  it("requires verified TLS and a non-root identity in production", () => {
    expect(() =>
      parseDorisQueryConfig(
        {
          DORIS_QUERY_URL: "mysql://doris-fe.internal:9030/langfuse",
          DORIS_QUERY_USER: "root",
          DORIS_QUERY_PASSWORD: "secret",
          DORIS_QUERY_TLS_ENABLED: "false",
        },
        "production",
      ),
    ).toThrow(/verified TLS/i);

    expect(() =>
      parseDorisQueryConfig(
        {
          DORIS_QUERY_URL: "mysql://doris-fe.internal:9030/langfuse",
          DORIS_QUERY_USER: "root",
          DORIS_QUERY_PASSWORD: "secret",
          DORIS_QUERY_TLS_ENABLED: "true",
        },
        "production",
      ),
    ).toThrow(/least-privilege/i);
  });

  it("requires a verifiable query hostname in production", () => {
    expect(() =>
      parseDorisQueryConfig(
        {
          DORIS_QUERY_URL: "mysql://10.0.0.10:9030/langfuse",
          DORIS_QUERY_USER: "langfuse_web_query",
          DORIS_QUERY_PASSWORD: "secret",
          DORIS_QUERY_TLS_ENABLED: "true",
        },
        "production",
      ),
    ).toThrow(/DNS hostname/i);
  });

  it("requires at least one concrete production redirect origin", () => {
    expect(() =>
      parseDorisStreamLoadConfig(
        {
          DORIS_STREAM_LOAD_FE_URL: "https://doris-fe.internal:8030",
          DORIS_STREAM_LOAD_USER: "langfuse_worker_load",
          DORIS_STREAM_LOAD_PASSWORD: "secret",
          DORIS_STREAM_LOAD_BE_ALLOWLIST: ",",
          DORIS_STREAM_LOAD_FE_IP_ALLOWLIST: "10.0.0.10",
          DORIS_STREAM_LOAD_BE_IP_ALLOWLIST: "10.0.0.11",
        },
        "production",
      ),
    ).toThrow(/redirect origin allowlist/i);
  });

  it("parses an HTTPS Stream Load endpoint and exact BE allowlist", () => {
    expect(
      parseDorisStreamLoadConfig(
        {
          DORIS_STREAM_LOAD_FE_URL: "https://doris-fe.internal:8030",
          DORIS_STREAM_LOAD_USER: "langfuse_worker_load",
          DORIS_STREAM_LOAD_PASSWORD: "secret",
          DORIS_STREAM_LOAD_BE_ALLOWLIST:
            "https://doris-be-1.internal:8040,https://10.0.0.12:8040",
          DORIS_STREAM_LOAD_FE_IP_ALLOWLIST: "10.0.0.10",
          DORIS_STREAM_LOAD_BE_IP_ALLOWLIST: "10.0.0.11,10.0.0.12",
          DORIS_STREAM_LOAD_TLS_CA_PATH: "/run/secrets/doris-ca.pem",
        },
        "production",
      ),
    ).toMatchObject({
      feOrigin: "https://doris-fe.internal:8030",
      user: "langfuse_worker_load",
      requireTls: true,
      allowedRedirectOrigins: [
        "https://doris-be-1.internal:8040",
        "https://10.0.0.12:8040",
      ],
      allowedRedirectAddresses: ["10.0.0.11", "10.0.0.12"],
      allowedFeAddresses: ["10.0.0.10"],
      tlsCaPath: "/run/secrets/doris-ca.pem",
    });
  });

  it("parses an exact local-dev redirect origin rewrite with independent IP pins", () => {
    expect(
      parseDorisStreamLoadConfig(
        {
          DORIS_LOCAL_DEV_MODE: "true",
          DORIS_STREAM_LOAD_FE_URL: "http://127.0.0.1:8031",
          DORIS_STREAM_LOAD_USER: "root",
          DORIS_STREAM_LOAD_PASSWORD: "",
          DORIS_STREAM_LOAD_BE_ALLOWLIST: "http://172.29.0.3:8040",
          DORIS_STREAM_LOAD_FE_IP_ALLOWLIST: "127.0.0.1",
          DORIS_STREAM_LOAD_BE_IP_ALLOWLIST: "172.29.0.3",
          DORIS_STREAM_LOAD_REDIRECT_ORIGIN_REWRITE_MAP:
            '{"http://172.29.0.3:8040":"http://127.0.0.1:8041"}',
          DORIS_STREAM_LOAD_REDIRECT_REWRITE_IP_ALLOWLIST: "127.0.0.1",
        },
        "development",
      ),
    ).toMatchObject({
      redirectOriginRewriteMap: {
        "http://172.29.0.3:8040": "http://127.0.0.1:8041",
      },
      allowedRewriteAddresses: ["127.0.0.1"],
    });
  });

  it("rejects redirect origin rewrites outside explicit local development mode", () => {
    const input = {
      DORIS_STREAM_LOAD_FE_URL: "http://127.0.0.1:8031",
      DORIS_STREAM_LOAD_USER: "root",
      DORIS_STREAM_LOAD_PASSWORD: "",
      DORIS_STREAM_LOAD_BE_ALLOWLIST: "http://172.29.0.3:8040",
      DORIS_STREAM_LOAD_BE_IP_ALLOWLIST: "172.29.0.3",
      DORIS_STREAM_LOAD_REDIRECT_ORIGIN_REWRITE_MAP:
        '{"http://172.29.0.3:8040":"http://127.0.0.1:8041"}',
      DORIS_STREAM_LOAD_REDIRECT_REWRITE_IP_ALLOWLIST: "127.0.0.1",
    };

    expect(() => parseDorisStreamLoadConfig(input, "development")).toThrow(
      /local development mode/i,
    );
    expect(() =>
      parseDorisStreamLoadConfig(
        { ...input, DORIS_LOCAL_DEV_MODE: "true" },
        "production",
      ),
    ).toThrow(/production/i);
  });

  it.each([
    [
      "userinfo",
      '{"http://user:secret@172.29.0.3:8040":"http://127.0.0.1:8041"}',
    ],
    ["path", '{"http://172.29.0.3:8040/private":"http://127.0.0.1:8041"}'],
    ["query", '{"http://172.29.0.3:8040?private=1":"http://127.0.0.1:8041"}'],
    ["hash", '{"http://172.29.0.3:8040#private":"http://127.0.0.1:8041"}'],
    ["wildcard", '{"http://*.internal:8040":"http://127.0.0.1:8041"}'],
    [
      "TLS downgrade",
      '{"https://doris-be.internal:8040":"http://127.0.0.1:8041"}',
    ],
    [
      "target userinfo",
      '{"http://172.29.0.3:8040":"http://user:secret@127.0.0.1:8041"}',
    ],
    [
      "target path",
      '{"http://172.29.0.3:8040":"http://127.0.0.1:8041/private"}',
    ],
    [
      "target query",
      '{"http://172.29.0.3:8040":"http://127.0.0.1:8041?private=1"}',
    ],
    [
      "target hash",
      '{"http://172.29.0.3:8040":"http://127.0.0.1:8041#private"}',
    ],
    ["target wildcard", '{"http://172.29.0.3:8040":"http://*.internal:8041"}'],
  ])("rejects %s in a redirect origin rewrite", (_name, rewriteMap) => {
    expect(() =>
      parseDorisStreamLoadConfig(
        {
          DORIS_LOCAL_DEV_MODE: "true",
          DORIS_STREAM_LOAD_FE_URL: "http://127.0.0.1:8031",
          DORIS_STREAM_LOAD_USER: "root",
          DORIS_STREAM_LOAD_PASSWORD: "",
          DORIS_STREAM_LOAD_BE_ALLOWLIST:
            "http://172.29.0.3:8040,https://doris-be.internal:8040",
          DORIS_STREAM_LOAD_BE_IP_ALLOWLIST: "172.29.0.3",
          DORIS_STREAM_LOAD_REDIRECT_ORIGIN_REWRITE_MAP: rewriteMap,
          DORIS_STREAM_LOAD_REDIRECT_REWRITE_IP_ALLOWLIST: "127.0.0.1",
        },
        "development",
      ),
    ).toThrow();
  });

  it("rejects rewrite chains and incomplete independent pinning", () => {
    const base = {
      DORIS_LOCAL_DEV_MODE: "true",
      DORIS_STREAM_LOAD_FE_URL: "http://127.0.0.1:8031",
      DORIS_STREAM_LOAD_USER: "root",
      DORIS_STREAM_LOAD_PASSWORD: "",
      DORIS_STREAM_LOAD_BE_ALLOWLIST:
        "http://172.29.0.3:8040,http://127.0.0.1:8041",
      DORIS_STREAM_LOAD_BE_IP_ALLOWLIST: "172.29.0.3",
      DORIS_STREAM_LOAD_REDIRECT_REWRITE_IP_ALLOWLIST: "127.0.0.1",
    };

    expect(() =>
      parseDorisStreamLoadConfig(
        {
          ...base,
          DORIS_STREAM_LOAD_REDIRECT_ORIGIN_REWRITE_MAP:
            '{"http://172.29.0.3:8040":"http://127.0.0.1:8041","http://127.0.0.1:8041":"http://127.0.0.1:8042"}',
        },
        "development",
      ),
    ).toThrow(/chain/i);

    expect(() =>
      parseDorisStreamLoadConfig(
        {
          ...base,
          DORIS_STREAM_LOAD_BE_IP_ALLOWLIST: "",
          DORIS_STREAM_LOAD_REDIRECT_ORIGIN_REWRITE_MAP:
            '{"http://172.29.0.3:8040":"http://127.0.0.1:8041"}',
        },
        "development",
      ),
    ).toThrow(/BE IP allowlist/i);

    expect(() =>
      parseDorisStreamLoadConfig(
        {
          ...base,
          DORIS_STREAM_LOAD_REDIRECT_REWRITE_IP_ALLOWLIST: "",
          DORIS_STREAM_LOAD_REDIRECT_ORIGIN_REWRITE_MAP:
            '{"http://172.29.0.3:8040":"http://127.0.0.1:8041"}',
        },
        "development",
      ),
    ).toThrow(/rewrite.*IP allowlist/i);
  });

  it("rejects Stream Load credentials reused by the query role", () => {
    expect(() =>
      parseDorisStreamLoadConfig(
        {
          DORIS_STREAM_LOAD_FE_URL: "https://doris-fe.internal:8030",
          DORIS_STREAM_LOAD_USER: "langfuse_worker_query",
          DORIS_STREAM_LOAD_PASSWORD: "secret",
          DORIS_QUERY_USER: "langfuse_worker_query",
          DORIS_STREAM_LOAD_BE_ALLOWLIST: "https://doris-be-1.internal:8040",
          DORIS_STREAM_LOAD_FE_IP_ALLOWLIST: "10.0.0.10",
          DORIS_STREAM_LOAD_BE_IP_ALLOWLIST: "10.0.0.11",
        },
        "production",
      ),
    ).toThrow(/separate/i);
  });

  it("redacts credentials, URLs, and payload fragments", () => {
    const message = redactDorisErrorMessage(
      "mysql://user:password@db.internal:9030 failed Authorization=Basic abc input=secret-payload",
    );

    expect(message).not.toContain("password");
    expect(message).not.toContain("abc");
    expect(message).not.toContain("secret-payload");
    expect(message).not.toContain("db.internal");

    const error = toDorisError(
      new Error("connect db.internal:9030 password=secret payload=private"),
    );
    expect(error.message).toBe("Analytics storage is unavailable");
    expect(error.cause).toBeUndefined();
  });
});
