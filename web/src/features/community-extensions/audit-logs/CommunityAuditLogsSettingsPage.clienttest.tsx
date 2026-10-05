import { render, screen } from "@testing-library/react";

import { CommunityAuditLogsSettingsPage } from "./CommunityAuditLogsSettingsPage";

vi.mock("use-query-params", () => ({
  NumberParam: {},
  withDefault: (value: unknown) => value,
  useQueryParams: () => [{ auditPageIndex: 0, auditPageSize: 20 }, vi.fn()],
}));

vi.mock("@/src/utils/api", () => ({
  api: {
    auditLogs: {
      all: {
        useQuery: () => ({
          isPending: false,
          isError: false,
          data: { data: [], totalCount: 0 },
        }),
      },
      allByOrg: {
        useQuery: () => ({
          isPending: false,
          isError: false,
          data: { data: [], totalCount: 0 },
        }),
      },
    },
  },
}));

vi.mock("@/src/components/table/data-table", () => ({
  DataTable: () => <div data-testid="audit-log-table" />,
}));

vi.mock("@/src/components/layouts/header", () => ({
  default: ({ title }: { title: string }) => <h1>{title}</h1>,
}));

vi.mock("@/src/components/BatchExportTableButton", () => ({
  BatchExportTableButton: ({ projectId }: { projectId: string }) => (
    <button data-testid="audit-log-export">Export {projectId}</button>
  ),
}));

vi.mock("@/src/components/ui/dialog", () => ({
  Dialog: ({ children }: { children: React.ReactNode }) => <>{children}</>,
  DialogContent: ({ children }: { children: React.ReactNode }) => (
    <>{children}</>
  ),
  DialogDescription: ({ children }: { children: React.ReactNode }) => (
    <>{children}</>
  ),
  DialogHeader: ({ children }: { children: React.ReactNode }) => (
    <>{children}</>
  ),
  DialogTitle: ({ children }: { children: React.ReactNode }) => <>{children}</>,
}));

describe("CommunityAuditLogsSettingsPage", () => {
  it("shows project audit export only for project-scoped logs", () => {
    const { rerender } = render(
      <CommunityAuditLogsSettingsPage projectId="project-1" />,
    );

    expect(screen.getByTestId("audit-log-export")).toHaveTextContent(
      "project-1",
    );

    rerender(<CommunityAuditLogsSettingsPage orgId="org-1" />);

    expect(screen.queryByTestId("audit-log-export")).not.toBeInTheDocument();
  });
});
