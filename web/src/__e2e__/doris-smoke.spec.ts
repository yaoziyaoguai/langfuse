import { expect, test, type Page } from "@playwright/test";

const enabled = process.env.DORIS_E2E_ENABLED === "1";
const projectId = process.env.DORIS_E2E_PROJECT_ID;
const email = process.env.DORIS_E2E_EMAIL;
const password = process.env.DORIS_E2E_PASSWORD;
const datasetId = process.env.DORIS_E2E_DATASET_ID;
const datasetRunId = process.env.DORIS_E2E_DATASET_RUN_ID;

type RuntimeFailures = {
  readonly pageErrors: string[];
  readonly serverResponses: string[];
};

function collectRuntimeFailures(page: Page): RuntimeFailures {
  const failures: RuntimeFailures = {
    pageErrors: [],
    serverResponses: [],
  };
  page.on("pageerror", (error) => failures.pageErrors.push(error.message));
  page.on("response", (response) => {
    if (response.status() >= 500 && response.url().includes("/api/trpc/")) {
      failures.serverResponses.push(
        `${response.status()} ${new URL(response.url()).pathname}`,
      );
    }
  });
  return failures;
}

async function signIn(page: Page): Promise<void> {
  if (!email || !password) {
    throw new Error(
      "DORIS_E2E_EMAIL and DORIS_E2E_PASSWORD are required when DORIS_E2E_ENABLED=1",
    );
  }

  await page.goto("/auth/sign-in");
  if (!page.url().includes("/auth/sign-in")) return;

  await page.locator('input[name="email"]').fill(email);
  await page.locator('input[type="password"]').fill(password);
  await page
    .locator('button[data-testid="submit-email-password-sign-in-form"]')
    .click();
  await expect(page).toHaveURL("/");
}

async function expectHealthyPage(
  page: Page,
  path: string,
  heading: string,
): Promise<void> {
  await page.goto(path);
  await expect(
    page.getByRole("heading", { name: heading, exact: true }),
  ).toBeVisible();
  await expect(
    page.getByText("Something went wrong", { exact: true }),
  ).toHaveCount(0);
  await expect(
    page.getByText("Application error", { exact: true }),
  ).toHaveCount(0);
  await expect(
    page.getByRole("heading", { name: "Not found", exact: true }),
  ).toHaveCount(0);
}

test.describe("Doris critical browser smoke", () => {
  test.skip(
    !enabled,
    "Set DORIS_E2E_ENABLED=1 and the Doris E2E credentials to run this suite",
  );

  test.beforeEach(async ({ page }) => {
    if (!projectId) {
      throw new Error(
        "DORIS_E2E_PROJECT_ID is required when DORIS_E2E_ENABLED=1",
      );
    }
    await signIn(page);
  });

  test("loads analytics, integrations, experiment, and monitor surfaces", async ({
    page,
  }) => {
    const failures = collectRuntimeFailures(page);
    const projectBase = `/project/${projectId}`;

    for (const [path, heading] of [
      [`${projectBase}/traces`, "Tracing"],
      [`${projectBase}/scores`, "Scores"],
      [`${projectBase}/sessions`, "Sessions"],
      [`${projectBase}/users`, "Users"],
      [`${projectBase}/settings/integrations`, "Integrations"],
      [`${projectBase}/monitors`, "Monitors"],
    ] as const) {
      await test.step(path, () => expectHealthyPage(page, path, heading));
    }

    await test.step("trace detail", async () => {
      await page.goto(`${projectBase}/traces`);
      const firstTraceRow = page.getByRole("table").getByRole("row").nth(1);
      await expect(firstTraceRow).toBeVisible();
      await firstTraceRow.getByRole("cell").nth(3).click();
      await expect(page.getByRole("dialog")).toBeVisible();
    });

    if (datasetId && datasetRunId) {
      await test.step("versioned experiment run detail", () =>
        expectHealthyPage(
          page,
          `${projectBase}/datasets/${datasetId}/runs/${datasetRunId}`,
          datasetRunId,
        ));
    }

    expect(failures.pageErrors).toEqual([]);
    expect(failures.serverResponses).toEqual([]);
  });

  test("queues a trace batch export without a client or server error", async ({
    page,
  }) => {
    const failures = collectRuntimeFailures(page);
    await page.goto(`/project/${projectId}/traces`);
    await expect(
      page.getByRole("heading", { name: "Tracing", exact: true }),
    ).toBeVisible();

    await page.getByRole("button", { name: "Export", exact: true }).click();
    await page.getByRole("menuitem", { name: "as JSONL", exact: true }).click();

    await expect(
      page.getByText("Export queued", { exact: true }),
    ).toBeVisible();
    await expect(
      page.getByText("Internal Server Error", { exact: true }),
    ).toHaveCount(0);
    expect(failures.pageErrors).toEqual([]);
    expect(failures.serverResponses).toEqual([]);
  });
});
