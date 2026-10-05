import { useState } from "react";
import { type PaginationState, type OnChangeFn } from "@tanstack/react-table";
import { NumberParam, useQueryParams, withDefault } from "use-query-params";
import { type LangfuseColumnDef } from "@/src/components/table/types";
import { DataTable } from "@/src/components/table/data-table";
import { BatchExportTableButton } from "@/src/components/BatchExportTableButton";
import Header from "@/src/components/layouts/header";
import { Badge } from "@/src/components/ui/badge";
import { Button } from "@/src/components/ui/button";
import { JSONView } from "@/src/components/ui/CodeJsonViewer";
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogHeader,
  DialogTitle,
} from "@/src/components/ui/dialog";
import { api, type RouterOutputs } from "@/src/utils/api";
import { BatchTableNames, deepParseJson } from "@langfuse/shared";

type AuditLogRow = RouterOutputs["auditLogs"]["all"]["data"][number];

type CommunityAuditLogsSettingsPageProps =
  | { projectId: string; orgId?: never }
  | { orgId: string; projectId?: never };

const formatActor = (row: AuditLogRow): string => {
  if (row.actor?.type === "USER") {
    return (
      row.actor.body.name ??
      row.actor.body.email ??
      row.actor.body.id ??
      "Deleted user"
    );
  }
  if (row.actor?.type === "API_KEY") {
    return row.actor.body.publicKey ?? row.actor.body.id ?? "Deleted API key";
  }
  return "Unknown";
};

const parseSnapshot = (value: string | null): unknown =>
  value === null ? null : deepParseJson(value);

export const CommunityAuditLogsSettingsPage = (
  props: CommunityAuditLogsSettingsPageProps,
) => {
  const [selectedLog, setSelectedLog] = useState<AuditLogRow | null>(null);
  const [queryPagination, setQueryPagination] = useQueryParams({
    auditPageIndex: withDefault(NumberParam, 0),
    auditPageSize: withDefault(NumberParam, 20),
  });
  const paginationState: PaginationState = {
    pageIndex: queryPagination.auditPageIndex,
    pageSize: queryPagination.auditPageSize,
  };
  const setPaginationState: OnChangeFn<PaginationState> = (updater) => {
    const next =
      typeof updater === "function" ? updater(paginationState) : updater;
    setQueryPagination({
      auditPageIndex: next.pageIndex,
      auditPageSize: next.pageSize,
    });
  };

  const projectLogs = api.auditLogs.all.useQuery(
    {
      projectId: props.projectId ?? "",
      page: paginationState.pageIndex,
      limit: paginationState.pageSize,
    },
    { enabled: props.projectId !== undefined },
  );
  const organizationLogs = api.auditLogs.allByOrg.useQuery(
    {
      orgId: props.orgId ?? "",
      page: paginationState.pageIndex,
      limit: paginationState.pageSize,
    },
    { enabled: props.orgId !== undefined },
  );
  const logs = props.projectId !== undefined ? projectLogs : organizationLogs;

  const columns: LangfuseColumnDef<AuditLogRow>[] = [
    {
      accessorKey: "createdAt",
      id: "createdAt",
      header: "Time",
      size: 180,
      cell: ({ row }) => row.original.createdAt.toLocaleString(),
    },
    {
      accessorKey: "actor",
      id: "actor",
      header: "Actor",
      size: 200,
      cell: ({ row }) => formatActor(row.original),
    },
    {
      accessorKey: "action",
      id: "action",
      header: "Action",
      size: 120,
      cell: ({ row }) => (
        <Badge variant="secondary">{row.original.action}</Badge>
      ),
    },
    {
      accessorKey: "resourceType",
      id: "resourceType",
      header: "Resource",
      size: 160,
    },
    {
      accessorKey: "resourceId",
      id: "resourceId",
      header: "Resource ID",
      size: 220,
      cell: ({ row }) => (
        <span className="font-mono text-xs">{row.original.resourceId}</span>
      ),
    },
    {
      accessorKey: "details",
      id: "details",
      header: "Changes",
      size: 100,
      cell: ({ row }) => (
        <Button
          variant="outline"
          size="sm"
          onClick={() => setSelectedLog(row.original)}
        >
          View
        </Button>
      ),
    },
  ];

  return (
    <div className="flex flex-col gap-4">
      <div className="flex items-start justify-between gap-2">
        <div>
          <Header title="Audit Logs" />
          <p className="text-muted-foreground text-sm">
            Review security-sensitive changes made by users and API keys.
          </p>
        </div>
        {props.projectId !== undefined && (
          <BatchExportTableButton
            projectId={props.projectId}
            tableName={BatchTableNames.AuditLogs}
            filterState={[]}
            orderByState={{ column: "createdAt", order: "DESC" }}
          />
        )}
      </div>
      <DataTable
        tableName={
          props.projectId !== undefined
            ? "communityProjectAuditLogs"
            : "communityOrganizationAuditLogs"
        }
        columns={columns}
        data={
          logs.isPending
            ? { isLoading: true, isError: false }
            : logs.isError
              ? {
                  isLoading: false,
                  isError: true,
                  error: logs.error.message,
                }
              : {
                  isLoading: false,
                  isError: false,
                  data: logs.data.data,
                }
        }
        pagination={{
          totalCount: logs.data?.totalCount ?? null,
          onChange: setPaginationState,
          state: paginationState,
        }}
        cellPadding="comfortable"
      />
      <Dialog
        open={selectedLog !== null}
        onOpenChange={(open) => {
          if (!open) setSelectedLog(null);
        }}
      >
        <DialogContent className="max-h-[85vh] max-w-4xl overflow-y-auto">
          <DialogHeader>
            <DialogTitle>Audit log changes</DialogTitle>
            <DialogDescription>
              {selectedLog
                ? `${selectedLog.resourceType} · ${selectedLog.resourceId}`
                : ""}
            </DialogDescription>
          </DialogHeader>
          {selectedLog && (
            <div className="grid gap-4 md:grid-cols-2">
              <JSONView
                title="Before"
                json={parseSnapshot(selectedLog.before)}
              />
              <JSONView title="After" json={parseSnapshot(selectedLog.after)} />
            </div>
          )}
        </DialogContent>
      </Dialog>
    </div>
  );
};
