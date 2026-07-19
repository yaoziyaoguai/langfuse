DO $$
DECLARE
  fence_status "AnalyticsBackgroundMigrationRetirementStatus";
  active_lock_count INTEGER;
BEGIN
  SELECT "status"
  INTO fence_status
  FROM "analytics_background_migration_retirement"
  WHERE "fence_name" = 'clickhouse-background-migrations'
    AND "manager_instance_id" = '__fence__'
  FOR UPDATE;

  IF fence_status IS NULL THEN
    RAISE EXCEPTION 'ClickHouse background migration retirement fence is missing';
  END IF;

  -- 全新数据库没有可迁移的项目数据，可以直接完成 drain；升级数据库必须先执行 Release A drain。
  IF fence_status = 'OBSERVING'
    AND NOT EXISTS (SELECT 1 FROM "projects" LIMIT 1)
  THEN
    UPDATE "analytics_background_migration_retirement"
    SET "status" = 'DRAINED',
        "drained_at" = CURRENT_TIMESTAMP,
        "updated_at" = CURRENT_TIMESTAMP
    WHERE "fence_name" = 'clickhouse-background-migrations'
      AND "manager_instance_id" = '__fence__';
    fence_status := 'DRAINED';
  END IF;

  IF fence_status = 'TERMINALIZED' THEN
    RETURN;
  END IF;

  IF fence_status <> 'DRAINED' THEN
    RAISE EXCEPTION
      'ClickHouse background migrations must be DRAINED before Release B (current status: %)',
      fence_status;
  END IF;

  SELECT COUNT(*)
  INTO active_lock_count
  FROM "background_migrations"
  WHERE "script" IN (
    'migrateTracesFromPostgresToClickhouse',
    'migrateObservationsFromPostgresToClickhouse',
    'migrateScoresFromPostgresToClickhouse',
    'migrateDatasetRunItemsFromPostgresToClickhouse',
    'migrateDatasetRunItemsFromPostgresToClickhouseRmt',
    'backfillEventsFullFromObservations',
    'backfillEventsFullFromDatasetRunItems',
    'createRootSpansFromTraces',
    'rewriteObservationsToPidTidSorting',
    'dropPidTidSortingTables',
    'migrateEventLogToBlobStorageRefTable'
  )
    AND "locked_at" > CURRENT_TIMESTAMP - INTERVAL '60 seconds';

  IF active_lock_count > 0 THEN
    RAISE EXCEPTION
      'Cannot terminalize ClickHouse background migrations: % active lock(s) remain',
      active_lock_count;
  END IF;

  UPDATE "background_migrations"
  SET "finished_at" = COALESCE("finished_at", CURRENT_TIMESTAMP),
      "locked_at" = NULL,
      "worker_id" = NULL,
      "state" = COALESCE("state", '{}'::jsonb) || jsonb_build_object(
        'retirementReason', 'CLICKHOUSE_RUNTIME_REMOVED',
        'retiredAt', CURRENT_TIMESTAMP
      )
  WHERE "script" IN (
    'migrateTracesFromPostgresToClickhouse',
    'migrateObservationsFromPostgresToClickhouse',
    'migrateScoresFromPostgresToClickhouse',
    'migrateDatasetRunItemsFromPostgresToClickhouse',
    'migrateDatasetRunItemsFromPostgresToClickhouseRmt',
    'backfillEventsFullFromObservations',
    'backfillEventsFullFromDatasetRunItems',
    'createRootSpansFromTraces',
    'rewriteObservationsToPidTidSorting',
    'dropPidTidSortingTables',
    'migrateEventLogToBlobStorageRefTable'
  );

  UPDATE "analytics_background_migration_retirement"
  SET "status" = 'TERMINALIZED',
      "terminalized_at" = CURRENT_TIMESTAMP,
      "updated_at" = CURRENT_TIMESTAMP
  WHERE "fence_name" = 'clickhouse-background-migrations'
    AND "manager_instance_id" = '__fence__';
END $$;
