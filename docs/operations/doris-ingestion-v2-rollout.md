# Doris ingestion V2 rolling rollout

The V2 ingestion delivery protocol uses `analytics_ingestion_outbox_v2` and
`analytics-ingestion-v2-queue`. The legacy table and queue remain isolated so
adjacent versions never deserialize each other's delivery payloads.

## Rollout

1. Apply the expand migrations before starting new application instances.
2. Roll web and worker instances with
   `LANGFUSE_ANALYTICS_INGESTION_LEGACY_HANDOFF_ENABLED=false`. During overlap,
   legacy workers drain the legacy queue and V2 workers drain the V2 queue.
3. Confirm that no legacy web or worker instance remains. Do not enable handoff
   while a legacy consumer can still claim an outbox row.
4. Record the remaining nonterminal legacy backlog:

   ```sql
   SELECT count(*)
   FROM analytics_ingestion_outbox o
   JOIN analytics_ingestion_operations op ON op.id = o.operation_id
   WHERE op.terminal_at IS NULL;
   ```

5. Set `LANGFUSE_ANALYTICS_INGESTION_LEGACY_HANDOFF_ENABLED=true` on the worker.
   The runner locks each operation, creates an immediately eligible V2 row,
   and removes its legacy row in one Postgres transaction. While this flag is
   enabled, a Postgres advisory lock serializes the short indexed batch claim;
   leased rows are then migrated in independent transactions across replicas,
   and an expired lease is reclaimable after a crash. The
   `langfuse.analytics.ingestion.legacy_handoff` counter records migrated rows
   without adding a count query to the worker loop.
6. Wait for the V2 queue backlog to converge, then re-run the indexed SQL check
   from step 4. The SQL result and V2 queue backlog must both be zero before
   declaring cutover.
7. Return the handoff flag to `false`. Retain the legacy table and Redis queue
   through the seven-day recoverability horizon; their later removal is a
   separate contract migration.

If a legacy producer or consumer is discovered after step 5, stop the cutover,
disable handoff, and reconcile both outbox tables before resuming. Do not clear
the legacy Redis queue as a substitute for the Postgres backlog check.
