import { AnnotationQueueObjectType, prisma } from "@langfuse/shared/src/db";

export async function processAddToAnnotationQueue(params: {
  projectId: string;
  objectIds: readonly string[];
  objectType: AnnotationQueueObjectType;
  targetId: string;
}): Promise<void> {
  const { projectId, objectIds, objectType, targetId } = params;
  if (objectIds.length === 0) return;

  const existingItems = await prisma.annotationQueueItem.findMany({
    where: {
      projectId,
      queueId: targetId,
      objectId: { in: [...objectIds] },
      objectType,
    },
    select: { objectId: true },
  });
  const existingIds = new Set(existingItems.map(({ objectId }) => objectId));
  const newObjectIds = objectIds.filter((id) => !existingIds.has(id));

  if (newObjectIds.length > 0) {
    await prisma.annotationQueueItem.createMany({
      data: newObjectIds.map((objectId) => ({
        projectId,
        queueId: targetId,
        objectId,
        objectType,
      })),
      skipDuplicates: true,
    });
  }
}
