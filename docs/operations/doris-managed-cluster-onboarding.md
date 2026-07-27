# 接入公司托管的 Doris 集群

本文说明如何把本仓库的 Langfuse 接入由公司平台团队维护的成熟 Doris
集群。你不需要自己部署或管理 Doris，但平台团队只提供一个用户名和密码时，
还不能直接完成生产接入。

本文重点解决三件事：

1. 判断现有账号能支持到哪一步；
2. 向 Doris 平台团队一次性申请完整的接入条件；
3. 在不把管理员凭据交给 Langfuse 的前提下完成 schema migration 和运行时验收。

完整部署步骤见
[`doris-only-deployment.md`](./doris-only-deployment.md)，安全边界见
[`doris-security.md`](./doris-security.md)。

## 先看结论

“成熟集群”表示 Doris 的可用性、备份、容量和节点运维已有责任方，不表示
Langfuse 的数据库、表、账号和网络访问已经准备好。

如果你现在真的只有一个用户名和密码，先按该账号的实际权限判断：

| 现有账号能力                                   | 能完成什么                                  | 还缺什么                                                         |
| ---------------------------------------------- | ------------------------------------------- | ---------------------------------------------------------------- |
| 只有 `SELECT_PRIV`                             | 连接和版本核验；schema 已存在时可做只读检查 | 不能建表、迁移或 Stream Load                                     |
| `SELECT_PRIV` + `LOAD_PRIV`                    | 可做部分读写探测                            | 不能完成 DDL migration；也不能作为生产环境共用的 query/load 身份 |
| 有 database DDL 权限                           | 可作为短期 migrator 候选                    | 不能长期注入 Web/Worker；仍需拆分三个运行时身份                  |
| 一个不可变的共享账号，平台团队也不提供其他协助 | 只能做有限的接入评估                        | **生产部署被阻塞**                                               |

生产接入需要四个不同的非 root 用户：

| 身份         | 用途                         | 最小权限                                                                         |
| ------------ | ---------------------------- | -------------------------------------------------------------------------------- |
| Web query    | Web 查询 Doris               | `<database>.*` 上的 `SELECT_PRIV`                                                |
| Worker query | Worker 查询 Doris            | `<database>.*` 上的 `SELECT_PRIV`                                                |
| Worker load  | Worker 通过 Stream Load 写入 | 指定应用表上的 table-scoped `LOAD_PRIV`                                          |
| Migrator     | 一次性创建和升级 schema      | `<database>.*` 上的 `SELECT_PRIV, LOAD_PRIV, CREATE_PRIV, ALTER_PRIV, DROP_PRIV` |

平台团队可以把这些凭据通过 secret manager 注入部署环境，不必把密码直接交给
Langfuse 维护人员。关键是四个 workload identity（工作负载身份）及其权限边界必须
存在，不能把一个高权限账号复制给所有容器。

## 支持状态判断

按下面三种状态判断是否可以继续：

### 可以直接进入接入流程

同时满足：

- Doris 版本为 `4.0.7`；
- 平台团队提供 FE MySQL/TLS、FE HTTPS Stream Load 和所有可能的 BE redirect
  地址；
- 平台团队提供 CA、DNS/IP allowlist 和网络放通；
- 可以创建四个独立身份，或者平台团队能够代为创建并注入；
- 平台团队允许 Langfuse one-shot migrator 运行，或者愿意用相同 release image
  代跑；
- 平台团队会评审 Langfuse 表的副本、bucket、容量和资源隔离设计。

### 需要平台团队配合后才能继续

你只有一个普通账号，但能够提交数据库申请、权限申请和变更工单。这是公司托管
集群最常见、也是支持的接入方式。直接使用本文的工单模板即可。

### 当前被阻塞

出现任意一项就不要启动生产 Web/Worker：

- 只能使用一个不可拆分的共享身份；
- 无法确认账号有哪些 grant；
- 无法获得受信 CA，或生产连接只能使用明文；
- 只有 FE 地址，也没有 Stream Load 的 client-visible redirect/gateway contract；
- Doris 不是 `4.0.7`；
- 不允许应用 migrator，也没有 DBA 代跑 migration；
- 不允许创建 Langfuse 所需的表、索引、分区或 migration ledger；
- 不接受在生产前处理当前 DDL 的 `replication_num=1` 和固定 bucket 设计。

这些是当前实现的显式安全和 schema contract，不是通过关闭 readiness 就可以绕过的
建议项。

## 双方责任边界

```text
Langfuse 部署方
  ├─ 固定代码版本和镜像 digest
  ├─ 提供 migration runner、校验和与所需 grant
  ├─ 配置 Web/Worker 和其他依赖
  └─ 执行 readiness、真实写入和功能验收

Doris 平台 / DBA
  ├─ 提供版本、FE/BE 拓扑、TLS CA 和网络条件
  ├─ 创建 database、用户、grant 和 secret
  ├─ 允许或代跑 one-shot migration
  ├─ 评审副本、bucket、资源组、配额和容量
  └─ 负责集群 HA、备份、监控、RPO 和 RTO
```

成熟集群减少的是 Doris 基础设施运维，不会消除应用 schema migration。Langfuse
仍然需要创建自己的表和 `_langfuse_schema_migrations` 账本。

## 一、向平台团队收集接入信息

不要只申请“一个 Doris 账号”。至少收集以下信息：

| 项目                 | 平台团队需要提供的结果                                        |
| -------------------- | ------------------------------------------------------------- |
| 环境                 | 开发、测试或生产，以及集群标识                                |
| 版本                 | `SELECT @@version_comment` 的脱敏结果，必须包含 `doris-4.0.7` |
| Database             | 为 Langfuse 分配的 database 名称                              |
| Query endpoint       | FE MySQL TLS DNS 和端口                                       |
| Stream Load endpoint | FE HTTPS origin                                               |
| Redirect topology    | 所有可能返回给客户端的 BE/gateway HTTPS origins               |
| IP allowlist         | FE DNS 和每个 client-visible redirect DNS 的允许解析 IP       |
| TLS                  | Query、migration、Stream Load 使用的 CA chain                 |
| Network              | Web、Worker、migrator 到上述端点的路由和防火墙放通            |
| Accounts             | 四个身份的用户名、权限和 secret reference                     |
| Migration            | 应用执行或 DBA 代跑的变更窗口                                 |
| Physical design      | 副本、bucket、存储、资源组和配额的评审结果                    |
| Operations           | 备份、监控、容量告警、RPO、RTO 和联系人                       |

用户名和 secret reference 可以进入工单；真实密码、Authorization header、完整连接
URL 和证书私钥不能进入工单、聊天、Git 或验收日志。

## 二、核验现有账号

使用公司批准的 MySQL-compatible client 连接 FE。让 `-p` 触发交互式密码输入，
不要把密码放在命令行：

```bash
mysql \
  --ssl-mode=VERIFY_IDENTITY \
  --ssl-ca=/approved/path/doris-ca.pem \
  -h doris-fe.internal.example \
  -P 9030 \
  -u existing_langfuse_user \
  -p
```

连接后执行：

```sql
SELECT @@version_comment;
SHOW GRANTS;
```

记录版本和 grant 的脱敏结果，不记录账号密码。若公司客户端或权限策略不允许
`SHOW GRANTS`，请 DBA 书面确认该账号的 object scope 和 privilege 列表。

现有账号不应直接填入所有 `DORIS_*_USER`。即使密码正确，生产配置仍要求 query
和 Stream Load 使用不同身份；least-privilege grant 合同还要求 Web query、Worker
query、Worker load 和 migrator 四个用户名互不相同。

## 三、创建 database 和四个身份

由 DBA 创建 database、身份和初始 migrator grant。示例名称可以按公司规范修改，
但必须是四个不同的非 root 身份：

```sql
CREATE DATABASE IF NOT EXISTS `langfuse`;

CREATE USER IF NOT EXISTS 'langfuse_web_query'@'%'
  IDENTIFIED BY '<managed-by-secret-platform>';
CREATE USER IF NOT EXISTS 'langfuse_worker_query'@'%'
  IDENTIFIED BY '<managed-by-secret-platform>';
CREATE USER IF NOT EXISTS 'langfuse_worker_load'@'%'
  IDENTIFIED BY '<managed-by-secret-platform>';
CREATE USER IF NOT EXISTS 'langfuse_migrator'@'%'
  IDENTIFIED BY '<temporary-migration-secret>';

GRANT SELECT_PRIV, LOAD_PRIV, CREATE_PRIV, ALTER_PRIV, DROP_PRIV
  ON `langfuse`.* TO 'langfuse_migrator'@'%';
```

此时先不要给 Worker load 用户 database-wide `LOAD_PRIV`。它只应在 migration
创建表后获得八张应用写入表的 table-scoped grant。

如果公司要求 database 由平台自动创建，可以跳过 `CREATE DATABASE`，但最终
database 名称必须与 Query URL、Stream Load 配置和 migration URL 中的名称一致。

## 四、选择 migration 执行方式

### 方式 A：平台提供临时 migrator secret

这是推荐方式：

1. 平台团队创建短期 `langfuse_migrator` 身份；
2. 通过 secret manager 把凭据和 CA 注入一次性 job；
3. Langfuse 部署方运行与待发布代码同版本的 migrator；
4. 第二次运行确认所有 migration 都被 skipped；
5. 保存脱敏的 migration 名称和 checksum 证据；
6. 平台团队撤销或轮换 migrator secret。

使用本仓库 Compose 时：

```bash
docker compose \
  --env-file .env.doris \
  -f docker-compose.doris.yml \
  --profile migrate \
  run --rm langfuse-doris-migrator
```

首次成功输出包含 applied 数量；再次执行必须显示已应用 migration 被 skipped。

### 方式 B：Doris 平台团队代跑

如果应用团队不能获得 DDL 凭据，由平台团队在批准的网络环境内运行**相同代码版本
和镜像 digest**：

```text
image: <approved-langfuse-web-image>@sha256:<approved-digest>
entrypoint: dumb-init --
command: node packages/shared/dist/doris/scripts/migrate.js
```

一次性 job 只注入：

- `DORIS_MIGRATION_URL`；
- `DORIS_MIGRATION_USER`；
- `DORIS_MIGRATION_PASSWORD`；
- `DORIS_MIGRATION_TLS_ENABLED=true`；
- `DORIS_MIGRATION_TLS_CA_PATH`；
- 可选的 connect/query timeout。

不要让 DBA 手工复制若干 SQL 后跳过 runner。Runner 会按顺序维护
`_langfuse_schema_migrations`，校验已应用文件的 checksum，并在历史被修改时拒绝
继续。平台团队可以先评审 migration SQL，但正式执行仍应使用固定 release 的
runner。

无论采用哪种方式，migrator 凭据都不能出现在长期运行的 Web 或 Worker 环境中。

## 五、迁移后授予运行时权限

表创建完成后，由 DBA 应用运行时 grant：

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

不要给：

- Web/Worker query 用户任何 `LOAD_PRIV` 或 DDL 权限；
- Worker load 用户 database-wide `LOAD_PRIV`、`SELECT_PRIV` 或 DDL 权限；
- Web/Worker 容器 migrator 凭据；
- 任意运行时容器 root 用户凭据。

## 六、把平台信息映射到部署配置

复制 [`doris.env.example`](../../doris.env.example) 后按下表映射：

| 平台交付物                     | Langfuse 配置                                            |
| ------------------------------ | -------------------------------------------------------- |
| FE MySQL TLS endpoint/database | `DORIS_QUERY_URL`, `DORIS_MIGRATION_URL`                 |
| Web query secret               | `DORIS_WEB_QUERY_USER`, `DORIS_WEB_QUERY_PASSWORD`       |
| Worker query secret            | `DORIS_WORKER_QUERY_USER`, `DORIS_WORKER_QUERY_PASSWORD` |
| Migrator secret                | `DORIS_MIGRATION_USER`, `DORIS_MIGRATION_PASSWORD`       |
| FE HTTPS origin                | `DORIS_STREAM_LOAD_FE_URL`                               |
| Worker load secret             | `DORIS_STREAM_LOAD_USER`, `DORIS_STREAM_LOAD_PASSWORD`   |
| FE resolved IPs                | `DORIS_STREAM_LOAD_FE_IP_ALLOWLIST`                      |
| BE HTTPS origins               | `DORIS_STREAM_LOAD_BE_ALLOWLIST`                         |
| BE resolved IPs                | `DORIS_STREAM_LOAD_BE_IP_ALLOWLIST`                      |
| CA files                       | 三个 `*_TLS_CA_HOST_PATH`                                |

生产 Query URL 必须使用证书 SAN 中的 DNS hostname，不能使用 IP literal，也不能
嵌入用户名或密码。Stream Load 从 FE 开始，FE 返回 `307` 后 Worker 会校验 BE
origin 和解析 IP；因此只提供一个 FE 地址不够。

如果平台提供 L7 HTTPS gateway，并保证在代理内部处理 FE 到 BE 的跳转、不会把
物理 BE origin 返回给客户端，不需要申请每台 BE 的地址。但必须让平台书面确认
client-visible endpoint contract，并把客户端唯一允许看到的 gateway origin/IP
配置进 redirect allowlist。真实 Stream Load 验收必须证明响应不会跳到未批准的
origin。当前生产配置即使实际不发生 `307`，也要求 redirect origin/IP allowlist
非空。

Compose 的 `.env.doris` 适合本地受控验收。正式部署应让 orchestrator 从公司的
secret manager 分别注入每个 workload 所需的 secret，不要给所有容器挂载一个
包含四套凭据的共享 dotenv 文件。

## 七、完成接入验收

至少保留以下脱敏证据：

1. `SELECT @@version_comment` 包含 `doris-4.0.7`；
2. migrator 首次执行的 applied 结果和第二次执行的 skipped 结果；
3. `_langfuse_schema_migrations` 中 migration name/checksum 与 release 一致；
4. `SHOW CREATE TABLE` 满足 readiness 所需的 key、partition、index 和 sequence
   fingerprint；
5. 四个身份的 allow/deny 权限探测符合最小权限边界；
6. Web 和 Worker readiness 都通过；
7. 一条真实 trace 能通过 Stream Load 写入并从 UI/API 读回；
8. 日志和证据中没有密码、Authorization header、带凭据 URL 或业务 payload。

具体启动和 trace smoke 步骤见
[`doris-only-deployment.md`](./doris-only-deployment.md)。

另外，当前 migration DDL 使用 `replication_num=1` 和固定的 2/4/8 buckets。
公司集群即使已经高可用，这些**表级物理属性**也不会自动变成生产设计。平台团队
必须结合 BE 数量、数据规模、资源组和 SLA 评审，并通过 forward-only migration
调整后再签字承载生产流量。

## 可直接复制的内部申请模板

```text
标题：Langfuse 接入公司 Doris 集群申请

业务系统：Langfuse
环境：<dev/test/prod>
业务负责人：<owner>
部署负责人：<owner>
目标上线时间：<date>
目标 database：<database>
固定代码版本 / image digest：<git-sha-or-image-digest>

请平台团队提供或确认：

1. Doris 版本
   - SELECT @@version_comment 的脱敏结果
   - 当前实现要求 Doris 4.0.7

2. 网络和 TLS
   - FE MySQL TLS DNS/port
   - FE HTTPS Stream Load origin
   - 所有 client-visible BE/gateway redirect HTTPS origins
   - FE 和 client-visible redirect DNS 的允许解析 IP
   - 如果平台 gateway 内部处理 redirect，请确认不会暴露物理 BE origin
   - 受信 CA chain 的 secret/file reference
   - Web、Worker、migrator 所在网络的防火墙放通

3. Database 和四个独立身份
   - Web query：database-wide SELECT_PRIV
   - Worker query：database-wide SELECT_PRIV
   - Worker load：以下八张应用表的 table-scoped LOAD_PRIV
     events_current, scores_current, blob_storage_file_log,
     trace_tombstones, project_tombstones, dataset_run_items_current,
     dataset_tombstones, dataset_run_tombstones
   - Migrator：database-wide SELECT_PRIV, LOAD_PRIV, CREATE_PRIV,
     ALTER_PRIV, DROP_PRIV；仅在一次性 migration job 使用
   - 请返回用户名和 secret reference，不要在工单中填写密码

4. Migration
   - 方案 A：向一次性 job 注入临时 migrator secret
   - 方案 B：平台团队使用指定 image digest 和 compiled migration runner 代跑
   - 需要提供首次 applied、再次 skipped、ledger name/checksum 的脱敏证据

5. 生产物理设计
   - 评审 replication、bucket、分区、索引、资源组和容量
   - 当前基线 replication_num=1，必须在生产前给出调整或风险处置

6. 运维责任
   - 备份/恢复、HA、容量告警、RPO、RTO
   - 值班联系人和变更窗口

验收要求：
- Web/Worker readiness 通过
- 四个身份的 allow/deny 权限探测通过
- 真实 Stream Load 写入和 trace 读回通过
- 验收材料不包含任何密码、Authorization header 或业务 payload
```

## 常见问题

### 平台只愿意给一个普通账号，可以先部署吗？

可以先做版本、网络和权限评估；不能把这个账号同时配置给 Web query、Worker
query 和 Worker load 后称为生产完成。若平台能继续通过工单创建身份并代跑
migration，接入仍然可行。

### 平台不允许把 DDL 账号交给应用团队怎么办？

不需要交。让平台团队运行固定 image digest 中的 compiled migration runner，
或者把短期 secret 只注入受控的一次性 job。Web/Worker 永远不需要 DDL 凭据。

### 平台已经有备份和高可用，还需要做什么？

仍需确认 Langfuse database 是否进入备份范围、恢复是否经过演练，以及 Langfuse
表的副本、bucket、资源组和容量是否符合实际 SLA。集群高可用不等于新建表已经
采用正确的物理设计。

### 账号和密码之外为什么还要 BE 地址？

Doris Stream Load 的初始请求发给 FE，FE 会用 `307` 把带 body 的请求重定向到
BE。Worker 在发送凭据和数据前会同时校验 FE/BE origin、解析 IP 和 TLS；不知道
所有合法 BE 目标就无法安全跟随重定向。

## Related

- [`doris-only-deployment.md`](./doris-only-deployment.md)：从配置到首次启动的完整步骤
- [`doris-security.md`](./doris-security.md)：账号、TLS、网络和轮换边界
- [`analytics-backend-selection.md`](./analytics-backend-selection.md)：backend
  marker、adoption 和 cutover 规则
