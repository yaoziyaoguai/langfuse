import {
  deleteDatasetMediaLinksByDatasetId,
  type DatasetQueueEventType,
} from "@langfuse/shared/src/server";

/** R1A has no dataset-run analytics projection; only the uncascaded media links remain. */
export async function processDatasetDelete(
  jobPayload: DatasetQueueEventType,
): Promise<void> {
  if (jobPayload.deletionType !== "dataset") return;
  await deleteDatasetMediaLinksByDatasetId({
    projectId: jobPayload.projectId,
    datasetId: jobPayload.datasetId,
  });
}
