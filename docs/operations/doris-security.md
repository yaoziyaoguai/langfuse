# Doris Runtime Security and Credential Boundary

This document defines the R1A runtime boundary introduced in U2. ClickHouse is
the default, while `LANGFUSE_ANALYTICS_BACKEND=doris` activates the Doris
adapter for the whole deployment. The local `1 FE + 1 BE` profile is for
development only and does not prove production availability, capacity, RPO, or
RTO.

## Workload identities

| Workload     | Secret location        | Minimum access                                              | Runtime lifetime |
| ------------ | ---------------------- | ----------------------------------------------------------- | ---------------- |
| Web query    | web only               | SELECT on R1A tables and schema ledger                      | pooled runtime   |
| Worker query | worker only            | SELECT on R1A tables and schema ledger                      | pooled runtime   |
| Worker load  | worker only            | Stream Load/INSERT on application-written R1A tables        | runtime          |
| Migrator     | one-shot job only      | CREATE/ALTER/DROP needed by checked migrations              | job only         |
| Backup       | backup job only        | snapshot/backup to the frozen repository                    | job only         |
| Restore      | restore drill/job only | restore into an isolated target, then promoted by procedure | job only         |
| Monitor      | monitoring plane only  | read health, workload, compaction, and capacity metadata    | runtime          |

Web and worker query identities must differ. The worker load identity must
differ from both query identities. Migrator, backup, and restore credentials
must never be present in either runtime image. The application rejects root,
empty passwords, cleartext production endpoints, query/load identity reuse,
and unpinned Stream Load FE/BE addresses.

## Network and TLS boundary

- Query uses the MySQL protocol over verified TLS to the configured FE DNS
  hostname. `mysql2` verifies the certificate against that hostname; production
  query URLs using an IP literal or a different TLS server name are rejected.
- Stream Load starts at one configured HTTPS FE origin. Before credentials or
  body are sent, every resolved FE address must be in
  `DORIS_STREAM_LOAD_FE_IP_ALLOWLIST`.
- Only one body-preserving `307` is followed. The redirect origin must exactly
  match `DORIS_STREAM_LOAD_BE_ALLOWLIST`, every resolved address must match
  `DORIS_STREAM_LOAD_BE_IP_ALLOWLIST`, and TLS cannot be downgraded. Userinfo in
  `Location` is discarded; the client supplies its own load credential.
- Query and load CA files are mounted read-only from the workload's secret
  injection mechanism. Private FE/BE service ports are not exposed publicly.
- Doris disks and the backup repository require infrastructure encryption at
  rest. Backup manifest signing keys and the external latest-generation anchor
  are outside the backup repository and outside Doris credentials.

## Configuration ownership

`web` receives only `DORIS_QUERY_*`. `worker` receives its own
`DORIS_QUERY_*` plus `DORIS_STREAM_LOAD_*`. The one-shot migrator receives only
`DORIS_MIGRATION_*`. Do not mount a shared dotenv file containing all three
credential sets into a container; `.env.prod.example` is a variable catalog,
not a secret-delivery design.

Readiness checks all ordered migration checksums, the Doris 4.0.7 version,
physical key/partition/index/sequence fingerprints, and non-expired Postgres
receipts that still require another canonicalizer/schema contract. Liveness
does not depend on Doris, so a storage outage does not create a restart loop.

## Overlap-and-revoke rotation

1. Create a new least-privilege identity or secret without changing the old
   identity.
2. Deploy only the owning workload with the replacement secret. Multiple query
   pools may overlap during this window.
3. Prove readiness and one scoped query or zero-filter Stream Load using the new
   identity. Never log the URL, Authorization header, SQL values, or payload.
4. Revoke the old identity/secret.
5. From an isolated verification job, prove the old credential can no longer
   connect and the new credential still passes readiness.
6. Close old pools and record the rotation evidence and timestamp.

Any failed revocation check, unallowlisted address, certificate verification
failure, filtered row, unknown load without label reconciliation, or schema
fingerprint mismatch blocks readiness and cutover.

## Production gate still owned by the operator

Before production traffic, freeze the actual FE/BE topology and addresses,
replication and bucket counts, resource budget, HA/non-HA risk acceptance,
capacity evidence, object-store publication semantics, backup repository,
manifest key manager, external anti-rollback anchor, RPO, and RTO. The committed
DDL currently records the local single-replica bucket layout and cannot be
called production-ready until those inputs are supplied and the physical
design is amended by a forward-only migration and re-tested.
