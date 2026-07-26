import {
  addToDeleteDatasetQueue,
  analyticsDatasetDeletionQueueReference,
  createAnalyticsDatasetDeletionIntent,
  deleteDatasetsByIds,
} from "@langfuse/shared/src/server";
import { prisma } from "@langfuse/shared/src/db";

export async function processDeleteDatasets(
  projectId: string,
  datasetIds: string[],
) {
  const deletion = await prisma.$transaction(async (transaction) => {
    const datasets = await transaction.dataset.findMany({
      where: { projectId, id: { in: datasetIds } },
      select: { id: true },
    });
    const intents = [];
    for (const dataset of datasets) {
      intents.push(
        await createAnalyticsDatasetDeletionIntent({
          transaction,
          scope: "DATASET",
          projectId,
          datasetId: dataset.id,
        }),
      );
    }
    await deleteDatasetsByIds({
      client: transaction,
      projectId,
      datasetIds: datasets.map(({ id }) => id),
    });
    return { datasets, intents };
  });

  await Promise.all(
    deletion.datasets.map((dataset, index) =>
      addToDeleteDatasetQueue({
        deletionType: "dataset",
        projectId,
        datasetId: dataset.id,
        analyticsDeletion: analyticsDatasetDeletionQueueReference(
          deletion.intents[index]!,
        ),
      }),
    ),
  );
}
