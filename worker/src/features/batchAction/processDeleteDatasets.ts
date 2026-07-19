import {
  addToDeleteDatasetQueue,
  deleteDatasetsByIds,
  findDatasetIdsByIds,
} from "@langfuse/shared/src/server";

export async function processDeleteDatasets(
  projectId: string,
  datasetIds: readonly string[],
): Promise<void> {
  const datasets = await findDatasetIdsByIds({
    projectId,
    datasetIds: [...datasetIds],
  });
  if (datasets.length === 0) return;

  await deleteDatasetsByIds({
    projectId,
    datasetIds: datasets.map(({ id }) => id),
  });
  await Promise.all(
    datasets.map(({ id }) =>
      addToDeleteDatasetQueue({
        deletionType: "dataset",
        projectId,
        datasetId: id,
      }),
    ),
  );
}
