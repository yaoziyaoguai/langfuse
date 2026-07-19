import { type NextApiRequest, type NextApiResponse } from "next";
import { z } from "zod";
import { getQueue, logger, QueueName } from "@langfuse/shared/src/server";

import { AdminApiAuthService } from "@/src/ee/features/admin-api/server/adminApiAuth";

const BullStatus = z.enum([
  "completed",
  "failed",
  "active",
  "delayed",
  "prioritized",
  "paused",
  "wait",
]);

const ManageBullBody = z.discriminatedUnion("action", [
  z.object({
    action: z.literal("retry"),
    queueNames: z.array(z.string()),
  }),
  z.object({
    action: z.literal("remove"),
    queueNames: z.array(z.string()),
    bullStatus: BullStatus,
  }),
]);

const RETIRED_QUEUES = [
  QueueName.TraceUpsert,
  QueueName.EvaluationExecution,
  QueueName.EvaluationExecutionSecondaryQueue,
  QueueName.LLMAsJudgeExecution,
  QueueName.CodeEvalExecution,
  QueueName.DatasetRunItemUpsert,
  QueueName.BatchExport,
  QueueName.OtelIngestionQueue,
  QueueName.OtelIngestionSecondaryQueue,
  QueueName.IngestionQueue,
  QueueName.IngestionSecondaryQueue,
  QueueName.ExperimentCreate,
  QueueName.BlobStorageIntegrationQueue,
  QueueName.BlobStorageIntegrationProcessingQueue,
  QueueName.CoreDataS3ExportQueue,
  QueueName.MeteringDataPostgresExportQueue,
  QueueName.DataRetentionQueue,
  QueueName.DataRetentionProcessingQueue,
  QueueName.BatchActionQueue,
  QueueName.CreateEvalQueue,
  QueueName.EventPropagationQueue,
  QueueName.MonitorQueue,
] as const;

const isRetiredQueue = (queueName: string) =>
  RETIRED_QUEUES.some(
    (retiredName) =>
      queueName === retiredName || queueName.startsWith(`${retiredName}-`),
  );

function queueFor(queueName: string) {
  if (isRetiredQueue(queueName)) return null;
  return getQueue(queueName as Parameters<typeof getQueue>[0]);
}

export default async function handler(
  req: NextApiRequest,
  res: NextApiResponse,
) {
  if (req.method !== "POST" && req.method !== "GET") {
    res.status(405).json({ error: "Method Not Allowed" });
    return;
  }
  if (
    !AdminApiAuthService.handleAdminAuth(req, res, {
      isAllowedOnLangfuseCloud: true,
    })
  ) {
    return;
  }

  if (req.method === "GET") {
    const queueNames = Object.values(QueueName).filter(
      (queueName) => !isRetiredQueue(queueName),
    );
    const queueCounts = await Promise.all(
      queueNames.map(async (queueName) => {
        try {
          return {
            queueName,
            jobCount: await queueFor(queueName)?.getJobCounts(),
          };
        } catch (error) {
          logger.error(`Failed to get job count for queue ${queueName}`, error);
          return { queueName, jobCount: Number.NaN };
        }
      }),
    );
    res.status(200).json(queueCounts);
    return;
  }

  const body = ManageBullBody.safeParse(req.body);
  if (!body.success) {
    res.status(400).json({ error: body.error });
    return;
  }
  if (body.data.queueNames.some(isRetiredQueue)) {
    res.status(501).json({
      error: "UnsupportedFeature",
      code: "DORIS_RETIRED_QUEUE_UNAVAILABLE",
      message: "The requested queue is not part of the Doris R1A runtime.",
    });
    return;
  }

  for (const queueName of body.data.queueNames) {
    const queue = queueFor(queueName);
    if (!queue) continue;

    if (body.data.action === "remove") {
      let removed: string[] = [];
      do {
        removed = await queue.clean(0, 1_000, body.data.bullStatus);
      } while (removed.length > 0);
      continue;
    }

    let failed = await queue.getJobs(["failed"], 0, 999, true);
    while (failed.length > 0) {
      await Promise.all(failed.map((job) => job.retry()));
      failed = await queue.getJobs(["failed"], 0, 999, true);
    }
  }

  res.status(200).json({
    message:
      body.data.action === "remove"
        ? "Removed all matching jobs"
        : "Retried all failed jobs",
  });
}
