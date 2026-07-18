-- U4 expand migration for the canonical event contract.
--
-- CanonicalAnalyticsEvent carries the OpenTelemetry status message separately
-- from its status level. Keep the migration retry-safe if a migrator loses its
-- connection after the ALTER commits but before the ledger row is recorded;
-- the migration runner reconciles ADD COLUMN statements against
-- information_schema before executing them.

ALTER TABLE events_current
ADD COLUMN status_message STRING NULL AFTER `level`;
