import { ErrorPage } from "@/src/components/error-page";
import Page from "@/src/components/layouts/page";
import { StatusBadge } from "@/src/components/layouts/status-badge";
import { Button } from "@/src/components/ui/button";
import {
  Card,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
} from "@/src/components/ui/card";
import { api } from "@/src/utils/api";
import Link from "next/link";
import { useRouter } from "next/router";

const phaseCopy: Record<string, string> = {
  visibility_barrier:
    "The Doris visibility barrier is still being confirmed. Until it is visible, logical invisibility is not claimed.",
  ingestion_drain:
    "The barrier is visible. Pre-barrier ingestion is converging before materialized cleanup continues.",
  materialized_cleanup:
    "The project is logically unavailable while Doris keys and attributable media are being cleaned up.",
  completed:
    "Doris and attributable media cleanup completed. Raw and canonical ingestion objects remain lifecycle-managed and expire within 7 days.",
};

export default function ProjectDeletionStatusPage() {
  const router = useRouter();
  const organizationId =
    typeof router.query.organizationId === "string"
      ? router.query.organizationId
      : undefined;
  const operationId =
    typeof router.query.operationId === "string"
      ? router.query.operationId
      : undefined;
  const status = api.deletionOperations.projectStatus.useQuery(
    {
      orgId: organizationId ?? "",
      deletionOperationId: operationId ?? "",
    },
    {
      enabled: Boolean(organizationId && operationId),
      refetchInterval: (query) =>
        query.state.data?.status === "completed" ? false : 2_000,
    },
  );

  if (!organizationId || !operationId || status.isPending) return null;
  if (status.isError || !status.data) {
    return <ErrorPage title="Not found" message="This page does not exist." />;
  }

  return (
    <Page
      withPadding
      scrollable
      headerProps={{
        title: "Project deletion",
        breadcrumb: [
          { name: "Projects", href: `/organization/${organizationId}` },
        ],
      }}
    >
      <Card className="mx-auto mt-8 w-full max-w-2xl">
        <CardHeader>
          <div className="flex items-center justify-between gap-4">
            <CardTitle>Deletion progress</CardTitle>
            <StatusBadge type={status.data.status} />
          </div>
          <CardDescription>
            Operation {status.data.deletionOperationId}
          </CardDescription>
        </CardHeader>
        <CardContent className="space-y-5">
          <p className="text-sm">
            {phaseCopy[status.data.phase] ??
              "Deletion is retrying from its last durable checkpoint."}
          </p>
          <dl className="grid grid-cols-[auto_1fr] gap-x-4 gap-y-2 text-sm">
            <dt className="text-muted-foreground">Phase</dt>
            <dd>{status.data.phase}</dd>
            <dt className="text-muted-foreground">Logically unavailable</dt>
            <dd>{status.data.logicallyInvisible ? "Yes" : "Not yet"}</dd>
            <dt className="text-muted-foreground">Project ID</dt>
            <dd>{status.data.projectId}</dd>
          </dl>
          <Button asChild variant="outline">
            <Link href={`/organization/${organizationId}`}>
              Back to projects
            </Link>
          </Button>
        </CardContent>
      </Card>
    </Page>
  );
}
