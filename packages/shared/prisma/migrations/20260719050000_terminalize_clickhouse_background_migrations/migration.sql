-- ClickHouse remains a supported analytics backend. Keep this migration as a
-- history-compatible no-op: terminalizing its background migrations would
-- break both existing ClickHouse deployments and the default new deployment.
SELECT 1;
