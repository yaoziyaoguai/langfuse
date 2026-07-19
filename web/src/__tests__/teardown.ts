export default async function teardown() {
  const { redis, logger, DorisClientManager } =
    await import("@langfuse/shared/src/server");

  logger.debug(`Redis status ${redis?.status}`);
  if (redis && redis.status !== "end" && redis.status !== "close") {
    redis.disconnect();
  }

  await DorisClientManager.getInstance().closeAllConnections();

  logger.debug("Teardown complete");
}
