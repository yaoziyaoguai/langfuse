export const BATCH_EXPORT_MANIFEST_LEASE_MS = 10 * 60_000;
// 执行租约必须在 BullMQ 重试耗尽前失效；存活任务由 heartbeat 持续续租。
export const BATCH_EXPORT_EXECUTION_LEASE_MS = 2 * 60_000;
export const BATCH_EXPORT_LEASE_HEARTBEAT_MS = 30_000;
