INSERT INTO "analytics_background_migration_retirement" (
  "id",
  "fence_name",
  "manager_instance_id",
  "generation",
  "status",
  "minimum_manager_build_id",
  "manager_build_id",
  "manager_heartbeat_at",
  "created_at",
  "updated_at"
) VALUES (
  'analytics-clickhouse-background-migrations-fence',
  'clickhouse-background-migrations',
  '__fence__',
  1,
  'OBSERVING',
  'operator-must-set',
  'control',
  TIMESTAMP '1970-01-01 00:00:00',
  CURRENT_TIMESTAMP,
  CURRENT_TIMESTAMP
) ON CONFLICT ("fence_name", "manager_instance_id") DO NOTHING;
