import { pipeline, Transform, type Readable } from "stream";
import { randomUUID } from "node:crypto";
import {
  BatchExportFileFormat,
  BatchExportQuerySchema,
  BatchExportStatus,
  exportOptions,
  LangfuseNotFoundError,
} from "@langfuse/shared";
import { prisma } from "@langfuse/shared/src/db";
import {
  StorageServiceFactory,
  sendBatchExportSuccessEmail,
  streamTransformations,
  type BatchExportJobType,
  logger,
  getCurrentSpan,
  applyCommentFilters,
  type CommentObjectType,
  assertManagedBatchExportJobMatches,
  batchExportManifestAttemptObjectKey,
  claimBatchExportExecution,
  claimBatchExportManifest,
  completeBatchExportExecution,
  failBatchExportExecution,
  failBatchExportManifest,
  quarantineBatchExport,
  renewBatchExportExecutionLease,
  renewBatchExportManifestLease,
  sealBatchExportManifest,
} from "@langfuse/shared/src/server";
import { env } from "../../env";
import { getAnalyticsExportSource } from "./analyticsExportRuntime";
import { getWorkerAnalyticsAdmissionContext } from "../../analyticsRuntime";
import {
  openVerifiedBatchExportIdentityManifest,
  writeBatchExportIdentityManifest,
} from "./BatchExportIdentityManifest";
import { withBatchExportLeaseHeartbeat } from "./BatchExportLeaseHeartbeat";
import {
  BATCH_EXPORT_EXECUTION_LEASE_MS,
  BATCH_EXPORT_LEASE_HEARTBEAT_MS,
  BATCH_EXPORT_MANIFEST_LEASE_MS,
} from "./BatchExportLeasePolicy";

// Map table names to comment object types for preprocessing
const tableToCommentType: Record<string, CommentObjectType | undefined> = {
  traces: "TRACE",
  observations: "OBSERVATION",
  sessions: "SESSION",
};

export const handleBatchExportJob = async (
  batchExportJob: BatchExportJobType,
) => {
  if (env.LANGFUSE_S3_BATCH_EXPORT_ENABLED !== "true") {
    throw new Error(
      "Batch export is not enabled. Configure environment variables to use this feature. See https://langfuse.com/self-hosting/infrastructure/blobstorage#batch-exports for more details.",
    );
  }

  const { projectId, batchExportId } = batchExportJob;

  logger.info(
    `[BATCH EXPORT] Starting batch export for ${projectId} and ${batchExportId}`,
  );

  const span = getCurrentSpan();
  if (span) {
    span.setAttribute(
      "messaging.bullmq.job.input.batchExportId",
      batchExportId,
    );
    span.setAttribute("messaging.bullmq.job.input.projectId", projectId);
  }

  // Get job details from DB
  const jobDetails = await prisma.batchExport.findFirst({
    where: {
      projectId,
      id: batchExportId,
    },
    include: { dispatchOutbox: true },
  });

  if (!jobDetails) {
    throw new LangfuseNotFoundError(
      `Job not found for project: ${projectId} and export ${batchExportId}`,
    );
  }

  const configuredForDoris = env.LANGFUSE_ANALYTICS_BACKEND === "doris";
  const managedDoris = jobDetails.analyticsBackend === "DORIS";
  if (configuredForDoris || managedDoris) {
    try {
      if (!jobDetails.dispatchOutbox) {
        throw new Error("Doris batch export has no durable dispatch outbox");
      }
      assertManagedBatchExportJobMatches({
        batchExport: jobDetails,
        job: batchExportJob,
        dispatchGeneration: jobDetails.dispatchOutbox.generation,
      });
    } catch (error) {
      await quarantineBatchExport({
        client: prisma,
        projectId,
        batchExportId,
        failureCode: "BATCH_EXPORT_PROVENANCE_MISMATCH",
        log:
          error instanceof Error
            ? error.message
            : "Batch export provenance validation failed",
      });
      throw error;
    }
  }

  // Check if the batch export has been cancelled
  if (jobDetails.status === BatchExportStatus.CANCELLED) {
    logger.info(
      `[BATCH EXPORT] Batch export ${batchExportId} has been cancelled. Skipping processing.`,
    );
    return; // Exit early without processing
  }

  // Check if the batch export is older than 30 days
  const thirtyDaysAgo = new Date();
  thirtyDaysAgo.setDate(thirtyDaysAgo.getDate() - 30);

  if (!managedDoris && jobDetails.createdAt < thirtyDaysAgo) {
    // For old exports, mark as failed with an informative message
    const improvedExportMessage =
      "We have improved the batch export feature. Please retry your export to benefit from the latest enhancements.";

    await prisma.batchExport.update({
      where: {
        id: batchExportId,
        projectId,
      },
      data: {
        status: BatchExportStatus.FAILED,
        finishedAt: new Date(),
        log: improvedExportMessage,
      },
    });

    logger.info(
      `[BATCH EXPORT] Batch export ${batchExportId} is older than 30 days. Marked as failed with retry message.`,
    );

    return; // Exit early without processing
  }

  if (jobDetails.status !== BatchExportStatus.QUEUED) {
    logger.warn(
      `[BATCH EXPORT] Job ${batchExportId} has invalid status: ${jobDetails.status}. Retrying anyway.`,
    );
  }

  if (!managedDoris) {
    // ClickHouse keeps its established queue/status lifecycle.
    await prisma.batchExport.update({
      where: {
        id: batchExportId,
        projectId,
      },
      data: {
        status: BatchExportStatus.PROCESSING,
      },
    });
  }

  // Parse query from job
  const parsedQuery = BatchExportQuerySchema.safeParse(jobDetails.query);
  if (!parsedQuery.success) {
    throw new Error(
      `Failed to parse query for ${batchExportId}: ${parsedQuery.error.message}`,
    );
  }

  if (span) {
    span.setAttribute(
      "messaging.bullmq.job.input.query",
      JSON.stringify(parsedQuery.data),
    );
  }

  // Process comment filters before creating stream
  const commentObjectType = tableToCommentType[parsedQuery.data.tableName];
  let processedFilter = parsedQuery.data.filter ?? [];

  if (commentObjectType) {
    const { filterState, hasNoMatches } = await applyCommentFilters({
      filterState: parsedQuery.data.filter ?? [],
      prisma,
      projectId,
      objectType: commentObjectType,
    });

    if (hasNoMatches) {
      // No matching items - complete export with empty results
      logger.info(
        `[BATCH EXPORT] Batch export ${batchExportId}: comment filter matched no items, completing with empty export`,
      );

      // Create an empty stream by using a filter that matches nothing
      processedFilter = [
        {
          type: "stringOptions" as const,
          operator: "any of" as const,
          column: "id",
          value: [],
        },
      ];
    } else {
      processedFilter = filterState;
    }
  }

  const exportRequest = {
    projectId,
    cutoffCreatedAt: jobDetails.createdAt,
    ...parsedQuery.data,
    filter: processedFilter,
    fileFormat: jobDetails.format as BatchExportFileFormat,
  };

  const bucketName = env.LANGFUSE_S3_BATCH_EXPORT_BUCKET;
  if (!bucketName) {
    throw new Error("No S3 bucket configured for exports.");
  }

  const storageParams = {
    bucketName,
    accessKeyId: env.LANGFUSE_S3_BATCH_EXPORT_ACCESS_KEY_ID,
    secretAccessKey: env.LANGFUSE_S3_BATCH_EXPORT_SECRET_ACCESS_KEY,
    endpoint: env.LANGFUSE_S3_BATCH_EXPORT_ENDPOINT,
    externalEndpoint: env.LANGFUSE_S3_BATCH_EXPORT_EXTERNAL_ENDPOINT,
    region: env.LANGFUSE_S3_BATCH_EXPORT_REGION,
    forcePathStyle: env.LANGFUSE_S3_BATCH_EXPORT_FORCE_PATH_STYLE === "true",
    awsSse: env.LANGFUSE_S3_BATCH_EXPORT_SSE,
    awsSseKmsKeyId: env.LANGFUSE_S3_BATCH_EXPORT_SSE_KMS_KEY_ID,
  };
  const storageService = StorageServiceFactory.getInstance(storageParams);
  const source = getAnalyticsExportSource();
  const expiresInSeconds =
    env.BATCH_EXPORT_DOWNLOAD_LINK_EXPIRATION_HOURS * 3600;

  const uploadExport = async (
    dbReadStream: Readable,
    signal?: AbortSignal,
  ): Promise<{ readonly signedUrl: string; readonly expiresAt: Date }> => {
    let rowCount = 0;
    const loggingTransform = new Transform({
      objectMode: true,
      transform(chunk, _encoding, callback) {
        if (signal?.aborted) {
          callback(
            signal.reason instanceof Error
              ? signal.reason
              : new Error("Batch export was aborted"),
          );
          return;
        }
        rowCount++;
        if (rowCount % 5000 === 0) {
          logger.info(
            `[BATCH EXPORT] Batch export ${batchExportId}: processed ${rowCount} rows`,
          );
        }
        callback(null, chunk);
      },
    });
    const fileStream = pipeline(
      dbReadStream,
      loggingTransform,
      streamTransformations[jobDetails.format as BatchExportFileFormat](),
      (error) => {
        if (error) {
          logger.error(
            "[BATCH EXPORT] Getting data from DB and transform failed: ",
            error,
          );
        } else {
          logger.info(
            `[BATCH EXPORT] Batch export ${batchExportId}: completed processing ${rowCount} total rows`,
          );
        }
      },
    );
    const fileExtension =
      exportOptions[jobDetails.format as BatchExportFileFormat].extension;
    const fileName = `${env.LANGFUSE_S3_BATCH_EXPORT_PREFIX}${Date.now()}-lf-${parsedQuery.data.tableName}-export-${projectId}.${fileExtension}`;
    await storageService.uploadFileBuffered({
      fileName,
      fileType:
        exportOptions[jobDetails.format as BatchExportFileFormat].fileType,
      data: fileStream,
      partSizeBytes: env.BATCH_EXPORT_S3_PART_SIZE_MIB * 1024 * 1024,
    });
    if (signal?.aborted) {
      throw signal.reason instanceof Error
        ? signal.reason
        : new Error("Batch export was aborted");
    }
    const signedUrl = await storageService.getSignedUrl(
      fileName,
      expiresInSeconds,
    );
    logger.info(`[BATCH EXPORT] Batch export file ${fileName} uploaded`);
    return {
      signedUrl,
      expiresAt: new Date(Date.now() + expiresInSeconds * 1000),
    };
  };

  let uploaded: { readonly signedUrl: string; readonly expiresAt: Date };

  if (managedDoris) {
    const admissionContext = getWorkerAnalyticsAdmissionContext();
    if (!admissionContext) {
      throw new Error("Doris batch export worker runtime is not admitted");
    }
    if (!source.scanIdentities) {
      throw new Error("Doris batch export identity source is unavailable");
    }
    const leaseOwner = `batch-export:${process.pid}:${batchExportId}`;
    const manifestClaim = await claimBatchExportManifest({
      client: prisma,
      admissionContext,
      projectId,
      batchExportId,
      leaseOwner,
      leaseMs: BATCH_EXPORT_MANIFEST_LEASE_MS,
    });
    if (!("sealed" in manifestClaim)) {
      try {
        const objectKey = batchExportManifestAttemptObjectKey({
          prefix: env.LANGFUSE_S3_BATCH_EXPORT_PREFIX,
          batchExportId,
          generation: manifestClaim.generation,
          claimId: manifestClaim.claimId,
        });
        const hardLimit = env.BATCH_EXPORT_ROW_LIMIT;
        const descriptor = await withBatchExportLeaseHeartbeat({
          intervalMs: BATCH_EXPORT_LEASE_HEARTBEAT_MS,
          renew: () =>
            renewBatchExportManifestLease({
              client: prisma,
              admissionContext,
              projectId,
              batchExportId,
              claimId: manifestClaim.claimId,
              generation: manifestClaim.generation,
              leaseOwner,
              leaseMs: BATCH_EXPORT_MANIFEST_LEASE_MS,
            }),
          run: (signal) =>
            writeBatchExportIdentityManifest({
              storage: storageService,
              objectKey,
              metadata: {
                batchExportId,
                projectId,
                tableName: parsedQuery.data.tableName,
                generation: manifestClaim.generation,
                claimId: manifestClaim.claimId,
                filterHash: manifestClaim.batchExport.manifestFilterHash,
              },
              identities: source.scanIdentities!(
                exportRequest,
                hardLimit,
                signal,
              ),
              maxRows: Math.min(parsedQuery.data.limit ?? hardLimit, hardLimit),
            }),
        });
        await sealBatchExportManifest({
          client: prisma,
          admissionContext,
          projectId,
          batchExportId,
          claimId: manifestClaim.claimId,
          generation: manifestClaim.generation,
          ...descriptor,
        });
      } catch (error) {
        await failBatchExportManifest({
          client: prisma,
          admissionContext,
          projectId,
          batchExportId,
          claimId: manifestClaim.claimId,
          generation: manifestClaim.generation,
          leaseOwner,
          failureCode: "BATCH_EXPORT_MANIFEST_ATTEMPT_FAILED",
          log:
            error instanceof Error && error.message
              ? error.message
              : "Batch export manifest preparation failed",
        });
        throw error;
      }
    }

    const executionClaim = await claimBatchExportExecution({
      client: prisma,
      admissionContext,
      projectId,
      batchExportId,
      leaseOwner,
      leaseMs: BATCH_EXPORT_EXECUTION_LEASE_MS,
      claimId: randomUUID(),
    });
    if ("completed" in executionClaim) return;
    const execution = executionClaim.batchExport;
    const renewExecutionLease = () =>
      renewBatchExportExecutionLease({
        client: prisma,
        admissionContext,
        projectId,
        batchExportId,
        claimId: executionClaim.claimId,
        generation: executionClaim.generation,
        leaseOwner,
        leaseMs: BATCH_EXPORT_EXECUTION_LEASE_MS,
      });
    try {
      uploaded = await withBatchExportLeaseHeartbeat({
        intervalMs: BATCH_EXPORT_LEASE_HEARTBEAT_MS,
        renew: renewExecutionLease,
        run: async (signal) => {
          const encodedManifest = await storageService.downloadStreamIfExists(
            execution.manifestObjectKey,
          );
          if (encodedManifest === null) {
            throw new Error("Sealed batch export manifest object is missing");
          }
          const identities = await openVerifiedBatchExportIdentityManifest({
            encodedBody: encodedManifest,
            descriptor: {
              objectKey: execution.manifestObjectKey,
              checksum: execution.manifestChecksum,
              rowCount: execution.manifestRowCount,
              byteCount: execution.manifestByteCount,
              formatVersion: execution.manifestFormatVersion as 1,
            },
            expected: {
              batchExportId,
              projectId,
              tableName: parsedQuery.data.tableName,
              generation: execution.manifestGeneration,
              claimId: execution.manifestClaimId,
              filterHash: execution.manifestFilterHash,
            },
            maxRows: env.BATCH_EXPORT_ROW_LIMIT,
          });
          const dbReadStream = await source.open(exportRequest, {
            identities,
            signal,
            revalidate: async () => {
              await renewExecutionLease();
            },
          });
          return uploadExport(dbReadStream, signal);
        },
      });
      await completeBatchExportExecution({
        client: prisma,
        admissionContext,
        projectId,
        batchExportId,
        claimId: executionClaim.claimId,
        generation: executionClaim.generation,
        url: uploaded.signedUrl,
        expiresAt: uploaded.expiresAt,
      });
    } catch (error) {
      await failBatchExportExecution({
        client: prisma,
        admissionContext,
        projectId,
        batchExportId,
        executionClaimId: executionClaim.claimId,
        executionGeneration: executionClaim.generation,
        failureCode: "BATCH_EXPORT_ATTEMPT_FAILED",
        log:
          error instanceof Error && error.message
            ? error.message
            : "Batch export execution failed",
      });
      throw error;
    }
  } else {
    uploaded = await uploadExport(await source.open(exportRequest));
    await prisma.batchExport.update({
      where: {
        id: batchExportId,
        projectId,
      },
      data: {
        status: BatchExportStatus.COMPLETED,
        url: uploaded.signedUrl,
        finishedAt: new Date(),
        expiresAt: uploaded.expiresAt,
      },
    });
  }

  // Send email to user
  const user = await prisma.user.findFirst({
    where: {
      id: jobDetails.userId,
    },
  });

  if (user?.email) {
    await sendBatchExportSuccessEmail({
      env,
      receiverEmail: user.email,
      downloadLink: uploaded.signedUrl,
      userName: user?.name || "",
      batchExportName: jobDetails.name,
    });

    logger.info(
      `[BATCH EXPORT] Batch export with id ${batchExportId} for project ${projectId} successful. Email sent to user ${user.id}`,
    );
  }
};
