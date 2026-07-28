# 仅使用 Doris 部署 Langfuse

本文给出一条从空环境部署本仓库 Langfuse 的完整路径。部署使用外部 Doris
集群，不启动 ClickHouse，也不配置任何 `CLICKHOUSE_*` 变量。

配套文件：

- [`docker-compose.doris.yml`](../../docker-compose.doris.yml)：构建本仓库
  Web/Worker，并启动单机 Postgres、Redis 和 MinIO；
- [`doris.env.example`](../../doris.env.example)：只包含这条部署路径需要的变量；
- [`doris-managed-cluster-onboarding.md`](./doris-managed-cluster-onboarding.md)：
  公司托管集群的账号申请、DBA 代跑 migration 和交付清单；
- [`doris-security.md`](./doris-security.md)：生产账号、TLS 和网络边界；
- [`analytics-backend-capabilities.md`](./analytics-backend-capabilities.md)：
  可选能力和激活状态。

## 适用范围

这条路径假设：

1. PostgreSQL 和 Doris 都是全新的 Langfuse 数据库；
2. Doris 由其他团队或现有平台维护，你能够获得连接地址和所需账号；
3. 当前先部署一个 Web 和一个 Worker；
4. 使用 Docker Compose 构建并运行本仓库代码；
5. Doris 是唯一的 analytics backend。

如果 PostgreSQL 中已经有 Langfuse 数据，不要设置 fresh initialization。应先按
[`analytics-backend-selection.md`](./analytics-backend-selection.md) 的
adoption 流程核验已有控制状态。

## 如果 Doris 是公司托管集群

你不需要拥有 Doris 管理员账号，也不需要自己维护 FE/BE。平台团队可以创建
database 和四个 workload identity，并通过 secret manager 注入凭据；如果 DDL
账号不能交给应用团队，也可以由平台团队使用相同 release image 代跑 one-shot
migrator。

但只有一个现成用户名和密码通常不够。按以下状态判断：

| 当前条件                                      | 结论                                                                             |
| --------------------------------------------- | -------------------------------------------------------------------------------- |
| 已有四个身份、TLS/FE/BE 拓扑和 migration 路径 | 可以继续本文部署                                                                 |
| 只有一个账号，但能申请身份或请 DBA 代跑       | 先按[公司托管 Doris 接入指南](./doris-managed-cluster-onboarding.md)完成平台交付 |
| 只有一个不可拆分账号，且无 DBA/平台协助       | 只能做有限连接评估，生产接入被阻塞                                               |

不要把同一个账号填入所有 `DORIS_*_USER`。成熟集群只改变 Doris 基础设施的
责任方，不会取消 Langfuse 的最小权限、TLS/网络、schema migration 和物理 DDL
评审要求。

## 先读生产限制

`docker-compose.doris.yml` 是可复现的单机部署模板，不是高可用生产架构：

- 内置 Postgres、Redis 和 MinIO 都是单实例；
- 内置 MinIO 为简化单机验收复用了 root credential，生产必须改为独立、
  bucket-scoped service identity；
- Web 和 MinIO API 的示例端口会绑定宿主机，生产必须由防火墙、TLS reverse
  proxy 或 ingress 限制；
- Doris migration 当前为本地验证拓扑设置了 `replication_num=1`，bucket
  数量也是固定的 2、4 或 8；
- Doris readiness 当前只接受 Doris `4.0.7`；
- Compose 环境变量不是生产 secret manager；
- 备份、签名 checkpoint、外部 anti-rollback anchor、RPO 和 RTO 需要部署方提供。

可以用这份模板完成真实集群接入、功能验收和受控单机部署。正式承载生产流量前，
必须根据实际 FE/BE 拓扑新增 forward-only Doris migration，调整副本和 bucket
设计，并使用高可用 Postgres、Redis、对象存储以及平台 secret injection。

## 部署拓扑

```text
用户 / SDK
    |
    v
Langfuse Web ---- PostgreSQL
    |                 |
    |                 +-- deployment marker / runtime leases / durable work
    |
    +------------ Redis / BullMQ
    |                 |
    v                 v
对象存储 <------ Langfuse Worker
                       |
                       +-- MySQL/TLS query ------> Doris FE
                       |
                       +-- HTTPS Stream Load ----> Doris FE --307--> Doris BE
```

Web 只有 Doris `SELECT_PRIV`。Worker 使用独立的 query 身份，并额外持有
table-scoped Stream Load 身份。Migrator 是独立的一次性任务，账号不会注入 Web
或 Worker。

## 一、准备条件

部署机需要：

- Docker Engine；
- Docker Compose v2，命令为 `docker compose`；
- `openssl`；
- 能访问 Doris FE MySQL/TLS 端口；
- Worker 所在网络能访问 Doris FE Stream Load 和所有可能重定向到的 BE；
- 一个能创建 database、用户和 grant 的 Doris 管理入口，或者 DBA 配合。

如果机器提供的是独立 `docker-compose` binary，而不是 Docker CLI plugin，把本文
所有 `docker compose` 原样替换为 `docker-compose`。可以用下面两个命令判断：

```bash
docker compose version
docker-compose --version
```

先确认当前代码分支：

```bash
git branch --show-current
git status --short
```

构建必须基于包含 Doris 实现的当前分支，不能直接使用上游官方
`langfuse/langfuse:3` 镜像代替本仓库构建结果。

## 二、核验 Doris 集群

### 1. 版本

使用管理员或临时核验账号连接 FE：

```sql
SELECT @@version_comment;
```

返回值必须包含 `doris-4.0.7`。其他版本会被 readiness 作为
`SCHEMA_MISMATCH` 拒绝。

### 2. 地址和 TLS

向 Doris 管理方确认并记录：

| 项目              | 示例                                       | 用途                                 |
| ----------------- | ------------------------------------------ | ------------------------------------ |
| FE MySQL DNS/端口 | `doris-fe.internal.example:9030`           | Web、Worker 和 migrator query        |
| FE HTTPS origin   | `https://doris-fe.internal.example:8030`   | Stream Load 初始请求                 |
| BE HTTPS origins  | `https://doris-be-1.internal.example:8040` | FE 的 `307` 重定向目标               |
| FE IP allowlist   | `192.0.2.10`                               | DNS 解析结果固定                     |
| BE IP allowlist   | `192.0.2.11,192.0.2.12`                    | 所有允许的重定向地址                 |
| CA 文件           | PEM                                        | Query、migration 和 Stream Load 验证 |

Query URL 必须使用证书 SAN 中的 DNS 主机名，不能使用 IP literal，也不能把
用户名或密码嵌入 URL。生产 Stream Load 必须是 HTTPS。

### 3. 数据库和账号

生产边界需要四个不同的非 root 用户：

| 身份                    | 最小权限                                                     |
| ----------------------- | ------------------------------------------------------------ |
| `langfuse_web_query`    | `langfuse.*` 上的 `SELECT_PRIV`                              |
| `langfuse_worker_query` | `langfuse.*` 上的 `SELECT_PRIV`                              |
| `langfuse_worker_load`  | application-written tables 上的 `LOAD_PRIV`                  |
| `langfuse_migrator`     | `SELECT_PRIV, LOAD_PRIV, CREATE_PRIV, ALTER_PRIV, DROP_PRIV` |

由 DBA 创建 database 和用户。下列 SQL 中的密码只是占位符，不要把真实密码放进
Git、工单正文或 shell history：

```sql
CREATE DATABASE IF NOT EXISTS `langfuse`;

CREATE USER IF NOT EXISTS 'langfuse_web_query'@'%'
  IDENTIFIED BY '<web-query-password>';
CREATE USER IF NOT EXISTS 'langfuse_worker_query'@'%'
  IDENTIFIED BY '<worker-query-password>';
CREATE USER IF NOT EXISTS 'langfuse_worker_load'@'%'
  IDENTIFIED BY '<worker-load-password>';
CREATE USER IF NOT EXISTS 'langfuse_migrator'@'%'
  IDENTIFIED BY '<migrator-password>';

GRANT SELECT_PRIV, LOAD_PRIV, CREATE_PRIV, ALTER_PRIV, DROP_PRIV
  ON `langfuse`.* TO 'langfuse_migrator'@'%';
```

Query 和 table-scoped load grant 在 migration 后执行，因为 load 表此时才存在。

## 三、准备环境文件和 secrets

复制模板：

```bash
cp doris.env.example .env.doris
mkdir -p runtime-secrets
mkdir -p runtime-data/doris-parquet-scratch
openssl rand -hex 32 > runtime-secrets/analytics-workload-epoch
chmod 600 runtime-secrets/analytics-workload-epoch
```

`.env.doris`、`runtime-secrets/` 和 `runtime-data/` 都被 `.gitignore` 排除。Linux
宿主机上，Worker 以 UID/GID `1001:1001` 运行；启用 analytics integrations 前，
需要让这个身份拥有 scratch 目录：

```bash
sudo chown 1001:1001 runtime-data/doris-parquet-scratch
sudo chmod 700 runtime-data/doris-parquet-scratch
```

生产环境应把 `LANGFUSE_DORIS_PARQUET_SCRATCH_HOST_PATH` 指向有容量限制的加密临时
文件系统，而不是仓库目录。

把受信 CA 放入 `runtime-secrets/`。同一个 CA 可以复制成三个文件，也可以在
`.env.doris` 中让三个 host path 指向同一文件：

```text
runtime-secrets/doris-query-ca.pem
runtime-secrets/doris-migration-ca.pem
runtime-secrets/doris-stream-load-ca.pem
```

编辑 `.env.doris`，至少填写：

- `DATABASE_URL`、`POSTGRES_PASSWORD`；
- `NEXTAUTH_SECRET`、`SALT`、`ENCRYPTION_KEY`；
- `REDIS_AUTH`、`MINIO_ROOT_PASSWORD`；
- Web query、Worker query、Worker load 和 migrator 的四组 Doris 凭据；
- Query、FE Stream Load、BE redirect 和 IP allowlist；
- 三个 CA host path。

生成应用 secrets 时可以分别运行：

```bash
openssl rand -base64 32
openssl rand -base64 32
openssl rand -hex 32
```

第三条的 64 位 hex 输出用于 `ENCRYPTION_KEY`。不要把命令输出提交到仓库。

`DATABASE_URL` 中如果密码包含 `@:/%#?` 等字符，必须进行 URL percent-encoding。
Compose 内部 Postgres 的典型地址是：

```text
postgresql://postgres:<encoded-password>@postgres:5432/postgres
```

只有在确认这是全新的 PostgreSQL 和 Doris database 后，第一次启动前把它改成：

```text
LANGFUSE_ANALYTICS_ALLOW_FRESH_INITIALIZATION=true
```

这个开关只能用于能够证明 PostgreSQL 和 Doris 都属于本次新部署的场景。

## 四、解析配置并构建镜像

先让 Compose 完成变量插值和 YAML 校验：

```bash
docker compose \
  --env-file .env.doris \
  -f docker-compose.doris.yml \
  config --quiet
```

然后构建当前分支的 Web 和 Worker：

```bash
docker compose \
  --env-file .env.doris \
  -f docker-compose.doris.yml \
  build langfuse-web langfuse-worker
```

模板没有 ClickHouse service，也没有任何 `CLICKHOUSE_*` 环境变量。

## 五、启动基础依赖

```bash
docker compose \
  --env-file .env.doris \
  -f docker-compose.doris.yml \
  up -d postgres redis minio
```

确认三项都是 healthy：

```bash
docker compose \
  --env-file .env.doris \
  -f docker-compose.doris.yml \
  ps
```

如果生产环境已经提供高可用 Postgres、Redis 或对象存储，应从 orchestrator
manifest 删除对应单机 service，并把 `.env.doris` 指向外部服务。

## 六、执行 Doris migration

先于 Web/Worker 执行一次性 migrator：

```bash
docker compose \
  --env-file .env.doris \
  -f docker-compose.doris.yml \
  --profile migrate \
  run --rm langfuse-doris-migrator
```

成功输出类似：

```text
Doris migrations: applied 5 (...), skipped 0 already applied
```

再次执行必须幂等，输出应显示所有 migration 已 skipped。已记录 migration 的
checksum 发生变化时，migrator 会拒绝继续，而不是覆盖历史。

### 迁移后授予运行时权限

```sql
GRANT SELECT_PRIV ON `langfuse`.*
  TO 'langfuse_web_query'@'%';
GRANT SELECT_PRIV ON `langfuse`.*
  TO 'langfuse_worker_query'@'%';

GRANT LOAD_PRIV ON `langfuse`.`events_current`
  TO 'langfuse_worker_load'@'%';
GRANT LOAD_PRIV ON `langfuse`.`scores_current`
  TO 'langfuse_worker_load'@'%';
GRANT LOAD_PRIV ON `langfuse`.`blob_storage_file_log`
  TO 'langfuse_worker_load'@'%';
GRANT LOAD_PRIV ON `langfuse`.`trace_tombstones`
  TO 'langfuse_worker_load'@'%';
GRANT LOAD_PRIV ON `langfuse`.`project_tombstones`
  TO 'langfuse_worker_load'@'%';
GRANT LOAD_PRIV ON `langfuse`.`dataset_run_items_current`
  TO 'langfuse_worker_load'@'%';
GRANT LOAD_PRIV ON `langfuse`.`dataset_tombstones`
  TO 'langfuse_worker_load'@'%';
GRANT LOAD_PRIV ON `langfuse`.`dataset_run_tombstones`
  TO 'langfuse_worker_load'@'%';
```

不要给 load 用户 database-wide `LOAD_PRIV`，也不要给 Web/Worker query 用户
DDL 权限。

## 七、首次启动

### 1. 只启动 Web

Web entrypoint 会执行 PostgreSQL migration。首次启动时，它还会使用 workload
epoch 创建 Doris generation 1 deployment marker：

```bash
docker compose \
  --env-file .env.doris \
  -f docker-compose.doris.yml \
  up -d langfuse-web
```

等待 readiness：

```bash
curl --fail --silent --show-error \
  http://127.0.0.1:3000/api/public/ready
```

### 2. 关闭 fresh initialization

Web ready 后，立即把 `.env.doris` 改成：

```text
LANGFUSE_ANALYTICS_ALLOW_FRESH_INITIALIZATION=false
```

重新创建 Web，确保后续启动不能重新初始化 marker：

```bash
docker compose \
  --env-file .env.doris \
  -f docker-compose.doris.yml \
  up -d --force-recreate langfuse-web
```

再次确认 Web ready。

### 3. 启动 Worker

```bash
docker compose \
  --env-file .env.doris \
  -f docker-compose.doris.yml \
  up -d langfuse-worker
```

Worker 只会在 Web ready 后启动：

```bash
curl --fail --silent --show-error \
  http://127.0.0.1:3030/api/health
```

查看最终状态和有限日志：

```bash
docker compose \
  --env-file .env.doris \
  -f docker-compose.doris.yml \
  ps

docker compose \
  --env-file .env.doris \
  -f docker-compose.doris.yml \
  logs --tail=100 langfuse-web langfuse-worker
```

不要把完整环境或 Docker inspect 输出复制到工单，它们可能包含密码。

## 八、基础功能验收

### 1. 检查运行时租约

Compose 默认使用两个稳定 instance ID：

```text
web-compose-1
worker-compose-1
```

在 PostgreSQL 中核验：

```sql
SELECT component, instance_id, backend, deployment_generation, state,
       lease_expires_at
  FROM analytics_runtime_leases
 WHERE superseded_at IS NULL
 ORDER BY component, instance_id;
```

Web 和 Worker 都应为 `DORIS`、同一个 deployment generation、`ACTIVE`，且
lease 尚未过期。

### 2. 写入和读取一条 trace

在 UI 创建 organization、project 和 API keys，然后只在当前 shell 中设置：

```bash
export LANGFUSE_PUBLIC_KEY='<project-public-key>'
export LANGFUSE_SECRET_KEY='<project-secret-key>'
export LANGFUSE_SMOKE_TRACE_ID="doris-smoke-$(date +%s)"
```

写入：

```bash
curl --fail --silent --show-error \
  --user "$LANGFUSE_PUBLIC_KEY:$LANGFUSE_SECRET_KEY" \
  --header 'Content-Type: application/json' \
  --data "{
    \"batch\": [{
      \"id\": \"event-$LANGFUSE_SMOKE_TRACE_ID\",
      \"timestamp\": \"$(date -u +%Y-%m-%dT%H:%M:%S.000Z)\",
      \"type\": \"trace-create\",
      \"body\": {
        \"id\": \"$LANGFUSE_SMOKE_TRACE_ID\",
        \"name\": \"doris-deployment-smoke\"
      }
    }]
  }" \
  http://127.0.0.1:3000/api/public/ingestion
```

异步 Worker 完成后读取：

```bash
curl --fail --silent --show-error \
  --user "$LANGFUSE_PUBLIC_KEY:$LANGFUSE_SECRET_KEY" \
  "http://127.0.0.1:3000/api/public/traces/$LANGFUSE_SMOKE_TRACE_ID"
```

还应在 UI 验证 trace list、trace detail、observation/trace metrics、score
analytics 和删除路径。一次成功的 API 返回不能替代这些路径。

## 九、激活完整 Doris 能力

核心 ingestion、读取、query engine、monitors 和 custom dashboards 随基础运行时
启用。以下六项 durable capability 默认 `DISABLED`，必须逐项 DARK、核验 fleet
census，再激活：

1. `coreBatchExports`
2. `datasetRunIngestion`
3. `experiments`
4. `datasetRunExports`
5. `evaluations`
6. `analyticsIntegrations`

其中 `experiments` 依赖 `datasetRunIngestion`；`datasetRunExports` 依赖
`coreBatchExports`。`evaluations` 和 `analyticsIntegrations` 没有上述依赖，但仍需
自己的 bootstrap 和 census。

准备 inventory 文件：

```json
[{ "instanceId": "web-compose-1" }, { "instanceId": "worker-compose-1" }]
```

复制到 Worker：

```bash
docker compose \
  --env-file .env.doris \
  -f docker-compose.doris.yml \
  cp analytics-inventory.json \
  langfuse-worker:/tmp/analytics-inventory.json
```

Compose 镜像使用编译后的 operator scripts。命令入口如下：

| 能力                     | 容器内命令                                                      |
| ------------------------ | --------------------------------------------------------------- |
| Core exports             | `node worker/dist/scripts/core-batch-exports-capability.js`     |
| Evaluations              | `node worker/dist/scripts/evaluations-capability.js`            |
| Experiments/dataset runs | `node worker/dist/scripts/u6-capabilities.js`                   |
| Integrations             | `node worker/dist/scripts/analytics-integrations-capability.js` |

例如检查 core export 状态：

```bash
docker compose \
  --env-file .env.doris \
  -f docker-compose.doris.yml \
  exec langfuse-worker \
  node worker/dist/scripts/core-batch-exports-capability.js status
```

从 `status` 输出读取当前 activation generation，再依次运行
`begin-dark` 和 `activate`。不要猜 generation：

```bash
docker compose \
  --env-file .env.doris \
  -f docker-compose.doris.yml \
  exec langfuse-worker \
  node worker/dist/scripts/core-batch-exports-capability.js \
  begin-dark \
  --expected-generation <current-activation-generation>

docker compose \
  --env-file .env.doris \
  -f docker-compose.doris.yml \
  exec langfuse-worker \
  node worker/dist/scripts/core-batch-exports-capability.js \
  activate \
  --expected-deployment-generation <deployment-generation> \
  --expected-activation-generation <dark-generation> \
  --runtime-inventory-file /tmp/analytics-inventory.json
```

其余能力的准确 DARK、bootstrap、activate、drain 和 disable 参数见：

- [Core exports capability](./analytics-backend-capabilities.md)；
- [Doris evaluations](./doris-evaluations.md)；
- [Doris experiments and dataset runs](./doris-experiments.md)；
- [Doris analytics integrations](./doris-analytics-integrations.md)。

激活 integrations 前必须确认 outbound allowlist、对象存储语义和
`LANGFUSE_DORIS_PARQUET_SCRATCH_ROOT` 的容量与加密。Compose named volume
不自动证明磁盘加密。

## 十、日常操作

### 查看状态

```bash
docker compose \
  --env-file .env.doris \
  -f docker-compose.doris.yml \
  ps

docker compose \
  --env-file .env.doris \
  -f docker-compose.doris.yml \
  logs --tail=100 langfuse-web langfuse-worker
```

### 重启应用，不重启基础设施

```bash
docker compose \
  --env-file .env.doris \
  -f docker-compose.doris.yml \
  restart langfuse-web langfuse-worker
```

### 升级

1. 备份 PostgreSQL、Doris 和对象存储；
2. 检查新版本的 PostgreSQL/Doris migrations；
3. 用新 tag 构建 Web/Worker；
4. 先运行 one-shot Doris migrator；
5. 按 Web、Worker 顺序滚动；
6. 等待所有 runtime lease 变为新 build 且 readiness 通过；
7. 逐项验收 active capability。

Doris migration 是 forward-only。不要通过删除
`_langfuse_schema_migrations` 记录、修改 checksum 或手工回退 DDL 来回滚。应用
版本回滚必须仍在 schema/canonicalizer compatibility window 内。

### Retention

持有 Enterprise License 并启用 `data-retention` entitlement 时，项目设置页中的
retention days 会直接驱动 Doris per-project retention。至少一个 Worker 必须保留：

```text
QUEUE_CONSUMER_DATA_RETENTION_QUEUE_IS_ENABLED=true
```

项目 cutoff 会先持久化到 PostgreSQL，再通过有界的 Doris Stream Load DELETE
清理 event、score 和 file-reference projection。旧 load 会在写入前重新检查
barrier；已完成 cutoff 不会因延长或关闭配置而后退。已经开始的 run 会在配置关闭后
继续收敛，这是防止部分删除和旧数据 replay 的安全要求。Media、Doris projection
和 raw-object 清理使用同一个 UTC cutoff；仍被当前 file-reference 引用的 raw object
不会提前删除。上线前至少验证一个项目中“旧数据删除、新数据保留、另一项目不受影响、
关闭后旧数据不复活”四个场景。

`LANGFUSE_DORIS_GLOBAL_RETENTION_DAYS` 默认留空。只有在 checkpoint、备份、恢复
和 anti-resurrection barrier 都验证后才能设置。首次启用前必须备份，因为已完成
cutoff 的推进不可逆。

## 十一、故障定位

| 现象                                     | 优先检查                                                                    |
| ---------------------------------------- | --------------------------------------------------------------------------- |
| Web readiness 为 `ANALYTICS_UNAVAILABLE` | FE DNS、9030 网络、TLS CA、query 用户和密码                                 |
| Web readiness 为 `SCHEMA_MISMATCH`       | Doris 是否为 4.0.7、migration ledger checksum、物理 DDL fingerprint         |
| 启动报告 adoption required               | fresh flag、workload epoch 文件、Postgres deployment marker                 |
| Worker Stream Load 失败                  | FE/BE HTTPS origins、307、FE/BE IP allowlist、load table grants             |
| Web ready、Worker 不 ready               | Worker query/load 身份、Redis、对象存储、重复 instance ID                   |
| 功能返回 unsupported/501                 | 对应 durable capability 是否仍为 `DISABLED`、`DARK` 或 `DRAINING`           |
| activation 报 inventory mismatch         | inventory 是否包含所有且仅包含未过期的 Web/Worker instance ID               |
| migration drift                          | migration 文件被修改或数据库 ledger 不属于当前构建；停止部署并比对 checksum |

readiness 会 fail closed。不要通过跳过 migration、放宽到 root、关闭 TLS、清空
Redis 队列或删除控制表来绕过错误。

## 十二、生产上线签字项

只有以下项目都有可审计证据时，才能把此部署称为 production-ready：

- Doris 4.0.7 FE/BE 拓扑、DNS、TLS 和 IP allowlist 已冻结；
- 根据实际节点数完成 replication/bucket 的 forward-only migration；
- 四类 Doris 身份通过允许和拒绝权限测试；
- PostgreSQL、Redis、对象存储和 Doris 均有 HA/容量结论；
- PostgreSQL、Doris、对象存储完成一致 checkpoint、备份和恢复演练；
- RPO、RTO、监控、告警和 on-call 归属明确；
- Web/Worker readiness、真实 ingestion、query、delete 和所有 active capability
  验收通过；
- retention 和 control-state cleaner 默认关闭，或已有单独审批与恢复证据。

部署的实现原理、ClickHouse 兼容性影响和完整测试证据保存在
[`doris-clickhouse-dual-backend-implementation.md`](./doris-clickhouse-dual-backend-implementation.md)。
