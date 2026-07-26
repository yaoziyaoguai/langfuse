import { afterEach, describe, expect, it, vi } from "vitest";

import {
  checkSeederEnv,
  createDefaultDoctorDependencies,
  preflight,
  runDoctor,
  type BackendCheckBundle,
  type DoctorDependencies,
} from "./doctor";

afterEach(() => vi.unstubAllGlobals());

const pass = (name: string) => ({
  name,
  status: "pass" as const,
  detail: "ok",
});

const backendBundle = (name: string): BackendCheckBundle => ({
  checks: [pass(name)],
  v4Tables: pass(`${name}-v4`),
});

const dependencies = (): DoctorDependencies => ({
  checkEnv: vi.fn(() => pass("env")),
  checkPostgres: vi.fn(async () => pass("postgres")),
  checkMigrations: vi.fn(async () => pass("postgres-migrations")),
  checkProject: vi.fn(async () => pass("project")),
  backendProbes: {
    clickhouse: vi.fn(async () => backendBundle("clickhouse")),
    doris: vi.fn(async () => backendBundle("doris")),
  },
  checkRedis: vi.fn(async () => pass("redis")),
  checkBlobStorage: vi.fn(async () => pass("blob-storage")),
  checkWebApp: vi.fn(async () => pass("web-app")),
  checkWebReadiness: vi.fn(async () => pass("web-readiness")),
  close: vi.fn(async () => undefined),
});

describe("backend-aware seeder doctor", () => {
  it("requires no ClickHouse variables for a Doris environment", () => {
    const result = checkSeederEnv("doris", {
      DATABASE_URL: "postgresql://local/test",
    });

    expect(result.status).toBe("pass");
    expect(result.detail).not.toContain("CLICKHOUSE_");
  });

  it("reports only Postgres when a Doris environment is incomplete", () => {
    const result = checkSeederEnv("doris", {});

    expect(result.status).toBe("fail");
    expect(result.detail).toContain("DATABASE_URL");
    expect(result.detail).not.toContain("CLICKHOUSE_");
  });

  it("does not construct or probe ClickHouse for a Doris doctor run", async () => {
    const deps = dependencies();

    const result = await runDoctor(
      "http://localhost:3000",
      "project-id",
      "doris",
      deps,
    );

    expect(result.ok).toBe(true);
    expect(deps.backendProbes.doris).toHaveBeenCalledOnce();
    expect(deps.backendProbes.clickhouse).not.toHaveBeenCalled();
    expect(deps.checkWebReadiness).toHaveBeenCalledWith(
      "http://localhost:3000",
      "doris",
    );
  });

  it("does not construct or probe Doris for a ClickHouse doctor run", async () => {
    const deps = dependencies();

    const result = await runDoctor(
      "http://localhost:3000",
      "project-id",
      "clickhouse",
      deps,
    );

    expect(result.ok).toBe(true);
    expect(deps.backendProbes.clickhouse).toHaveBeenCalledOnce();
    expect(deps.backendProbes.doris).not.toHaveBeenCalled();
    expect(deps.checkWebReadiness).toHaveBeenCalledWith(
      "http://localhost:3000",
      "clickhouse",
    );
  });

  it("keeps the same backend isolation in scenario preflight", async () => {
    const deps = dependencies();

    await preflight(
      {
        projectId: "project-id",
        backend: "doris",
        baseUrl: "http://localhost:3000",
        needV4: false,
        needWeb: true,
        log: vi.fn(),
      },
      deps,
    );

    expect(deps.backendProbes.doris).toHaveBeenCalledOnce();
    expect(deps.backendProbes.clickhouse).not.toHaveBeenCalled();
    expect(deps.checkWebReadiness).toHaveBeenCalledWith(
      "http://localhost:3000",
      "doris",
    );
  });

  it("rejects a ClickHouse web runtime during Doris readiness", async () => {
    const fetchSpy = vi
      .fn()
      .mockResolvedValueOnce(
        Response.json(
          { status: "Expected analytics backend is not selected" },
          { status: 503 },
        ),
      );
    vi.stubGlobal("fetch", fetchSpy);
    const deps = createDefaultDoctorDependencies();

    const result = await deps.checkWebReadiness(
      "http://localhost:3000",
      "doris",
    );

    expect(result).toMatchObject({
      name: "doris-readiness",
      status: "fail",
    });
    expect(result.detail).toContain("HTTP 503");
    expect(fetchSpy).toHaveBeenCalledWith(
      "http://localhost:3000/api/public/ready?analyticsBackend=doris",
      expect.objectContaining({ signal: expect.any(AbortSignal) }),
    );
    await deps.close();
  });

  it.each(["doris", "clickhouse"] as const)(
    "verifies %s identity through the read-only readiness endpoint",
    async (backend) => {
      const fetchSpy = vi
        .fn()
        .mockResolvedValueOnce(Response.json({ status: "OK" }));
      vi.stubGlobal("fetch", fetchSpy);
      const deps = createDefaultDoctorDependencies();

      const result = await deps.checkWebReadiness(
        "http://localhost:3000",
        backend,
      );

      expect(result).toMatchObject({
        name: `${backend}-readiness`,
        status: "pass",
      });
      expect(fetchSpy).toHaveBeenCalledOnce();
      expect(fetchSpy).toHaveBeenCalledWith(
        `http://localhost:3000/api/public/ready?analyticsBackend=${backend}`,
        expect.objectContaining({ signal: expect.any(AbortSignal) }),
      );
      await deps.close();
    },
  );
});
