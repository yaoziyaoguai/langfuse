---
title: Langfuse Community Doris Analytics Storage Refactor - PRD
type: feat
date: 2026-07-17
canonical_plan: docs/plans/2026-07-17-001-feat-langfuse-doris-storage-plan.md
upstream_baseline: langfuse@3.218.0
upstream_commit: 85d233edc65ed65d2f0949ec86766aeac3deb719
delivery_boundary: R1A-core / R1B-adoption-gated
---

# Langfuse Community Doris Analytics Storage Refactor - PRD

## 1. 改造目标

将 Langfuse Community Edition 改造成供内部团队自托管使用的稳定分支：保留 Postgres、Redis/Valkey 和 S3-compatible object storage 的现有职责，仅用 Apache Doris 替换 ClickHouse 分析数据面，并优先保留团队实际需要的 tracing、debugging、evaluation、prompt、dataset 和基础 analytics 能力。

本项目不是建设通用可插拔数据库平台，也不是重新实现 Langfuse Cloud/Enterprise。R1A 先交付可长期维护、运行时完全不依赖 ClickHouse、行为可验收的内部核心版本；R1B 只在内部有明确 owner 和使用信号后启用 evaluator/experiment 与 global retention，不阻塞 R1A 上线。

## 2. 背景与问题

当前 Langfuse 将控制面数据放在 Postgres，将队列和缓存放在 Redis/Valkey，将 raw ingestion、media 与导出文件放在对象存储，将高容量 trace、observation、score 和 dashboard 查询放在 ClickHouse。

ClickHouse 并不是一个可以通过替换连接字符串移除的依赖。现有实现把 `FINAL`、`ReplacingMergeTree`、`Map`/`Array`、`argMaxIf`、`ARRAY JOIN`、`quantile`、`WITH FILL`、全文索引和原生流式导出等语义分散在 ingestion、repository、filter/search、metrics、dashboard、deletion 和 export 路径中。

用户约束已经明确：

- 只服务内部团队，不建设面向大量外部租户的平台。
- 不购买商业版，只使用并修改 MIT 许可范围内的 Community core。
- 不希望部署或维护 ClickHouse，分析存储统一使用 Doris。
- 希望保留 Langfuse 其他有价值的能力，但允许按核心价值分阶段完成。
- `Understand Anything` / “Ask Anything” 是本机代码理解插件，与 Langfuse 产品、Doris 数据或本项目授权无关。

## 3. 产品原则

1. **核心工作流优先。** 先完成 ingest → observe → debug → score 的内部闭环；evaluator/experiment 只在有实际采用证据后进入 R1B。
2. **行为兼容高于物理兼容。** 保留 UI、Public API、MCP 和领域对象语义，不复制 ClickHouse 的物理表和 SQL 方言。
3. **一个 canonical tracing model。** fresh deployment 只持久化 v4 canonical events；legacy API 未来也只能转换为 canonical events，不能恢复 v3 `traces`/`observations` 双模型。
4. **无静默失败。** Doris 不可用、写入未提交、查询失败或功能未支持时必须显式失败，不能返回空列表、零指标或成功响应掩盖问题。
5. **最小长期分叉。** 数据库差异收敛在语义化 persistence/query boundary 内，避免 UI、route 和 worker 直接拼 Doris SQL。
6. **许可证边界不可穿透。** 不修改、不复制、不依赖 `ee/`、`web/src/ee/`、`worker/src/ee/` 中的商业实现来完成本项目。
7. **核心上线与采用扩展分离。** R1A 只承担可观测、调试、基础 score、删除和恢复；R1B 不进入 R1A 的 cutover dependency chain。
8. **兼容承诺有基线。** 本 PRD 的行为兼容冻结在 `langfuse@3.218.0` / commit `85d233edc65ed65d2f0949ec86766aeac3deb719`；以后合并上游必须经过内部需求评审并更新 compatibility corpus，不承诺永久自动跟随。

## 4. 用户与主要任务

- **平台运维人员：** 部署和升级内部 Langfuse，监控 Doris、队列、对象存储和写入积压，执行备份恢复与数据重放。
- **AI 应用工程师：** 通过 Langfuse SDK 或 OTLP 上报 trace，查询 observation，筛选异常，分析 token、cost 和 latency。
- **评估与标注人员：** 查看 trace，创建或检查 score，运行 evaluator，比较 dataset experiment 结果，添加 comment/annotation。
- **项目管理员与成员：** 管理 project、API key、prompt、dataset、model connection 和项目内访问；权限沿用 Community 能力。
- **API/MCP 自动化客户端：** 在 API key 对应的 project scope 内读取 observation、metric、score 和 experiment 结果，得到与 UI 一致的对象和错误语义。

## 5. 能力取舍矩阵

| 能力 | R1A 核心上线 | R1B 有采用信号后 | 后续 R2 | 明确不做 |
|---|---|---|---|---|
| Basic org/project/member/auth/API key | 保留并做回归 | - | - | Enterprise fine-grained RBAC、SCIM、SSO |
| OTLP tracing ingestion | JSON、protobuf、gzip；v4 canonical events | - | 扩展兼容矩阵 | - |
| 现代 Langfuse SDK | 支持冻结基线的 v4 ingestion contract | - | 新版本按需求评审 | 永久自动跟随上游 |
| Legacy trace/observation write APIs | 明确 unsupported，不静默降级 | - | `LegacyEventCanonicalizer` 写 canonical events | 恢复 v3 storage |
| Trace/observation/session/user | list、detail、stable pagination、filters、search | - | 低频 legacy read parity | - |
| Token/cost/latency/model usage | 精确 totals、时间序列、p50–p99 | - | 高级 attribution | Cloud billing/metering |
| Scores/feedback/evals | numeric/boolean/categorical score 与基础 score 视图 | LLM/code evaluator 自动执行闭环 | 高级 score analytics | Enterprise evaluator features |
| Prompt management/model connections/playground | Postgres 路径保持可用并回归 | - | - | Protected prompt labels |
| Datasets/basic experiments | dataset/item/run definition 保持可用 | run/result/eval/score 最小执行闭环 | 大规模 experiment 优化 | Enterprise-only automation |
| Annotation/comments | 保留现有 Community 行为并回归 | - | 高级队列优化 | Enterprise workflows |
| Home dashboard | 现有 home presets：traces、model costs、scores、traffic、usage、users、latency | - | 自定义 dashboard/widget builder | - |
| Public API/MCP | observation、score、metric 的查询/分页/projection/error/isolation parity | experiment/evaluator surface | 新工具和长任务增强 | Agent-only shadow data |
| Manual trace/project deletion | materialized 可见性、幂等、进度和 anti-resurrection | - | strict raw-object erasure | 假装 raw S3 已立即删除 |
| Retention | raw/canonical prefix 最长 7 天 lifecycle | 可选 deployment-wide global retention | 需求出现后另做 retention classes | 复制 Enterprise per-project retention |
| Batch export | - | - | JSON/CSV/JSONL；Parquet direct-to-S3 | ClickHouse native export compatibility |
| Product monitors | - | - | Community monitor parity | Cloud alerts |
| Operational monitoring | Doris/queue/load/query/backup health | global-retention health | 持续完善 | - |
| Historical ClickHouse migration | - | - | 另立项目且需真实数据需求 | 本 PRD 内 backfill/dual-run |
| Ask/Understand Anything plugin | - | - | - | 全部排除 |

R1B 的每个能力必须记录一个内部 owner、当前使用信号和上线验收人；没有证据时保持显式 unavailable。R1A 的完成与上线不等待 R1B。

## 6. R1A/R1B 产品需求

### 6.1 数据与授权边界

- R1. Postgres 继续承载 users、organizations、projects、memberships、prompts、datasets、model configuration、API keys、jobs、稀疏 UI state，以及不含用户 payload 的 ingestion/load/entity-head/tombstone 与 R1B retention durability control metadata；Redis/Valkey 继续承载 BullMQ/cache/rate limit；对象存储继续承载 raw/canonical ingestion 和 media；Doris 承载 analytics telemetry 及其查询投影。
- R2. 所有 analytics read/write/delete repository 必须显式接收可信 `projectId`；该值只能来自 session、API key 或 MCP `ServerContext`，不能由 MCP model 参数覆盖。
- R3. Doris 层不实现新的权限模型。UI、Public API 和 MCP 必须继承 Community 的 project membership/API-key 授权，并对跨 project ID 返回 scoped NotFound/Forbidden，不泄露对象是否存在。
- R4. R1 代码变更不得修改或复制 Enterprise-licensed 目录。Commercial feature gate 不得被绕过。

### 6.2 Canonical ingestion 与持久性

- R5. OTLP JSON/protobuf/gzip 和冻结基线的 v4 SDK trace 进入同一 canonical event path；R1A 不创建或写入 v3 `traces`、`observations`、`observations_batch_staging`，不注册 v3→v4 propagation/backfill runtime。
- R6. Ingestion 具有可观察状态：`accepted → queued → persisted → visible`，以及 `retrying`、`partial_failed`、`quarantined`、`unrecoverable`、`cancelled_by_deletion` 与 mixed terminal `completed_with_cancellations`。每个 accepted request 返回不可猜测且 project-scoped 的 `operationId`：v4 API 在兼容响应体中返回，OTLP 使用 `x-langfuse-ingestion-operation-id` 响应头。`GET /api/public/ingestion-operations/{operationId}` 返回 operation/child 状态、可见 entity link、safe cancellation reason、expiry 与可行动的 retry/support 提示，但不返回 payload、SQL 或拓扑；terminal status 至少保留 30 天。HTTP accepted 只表示 raw payload 与持久化 ingestion receipt 已可靠落盘；BullMQ enqueue 失败由 ingestion-specific outbox/reconciler 补偿，不表示 Doris 已可见。
- R7. Accepted receipt 的 manifest 初始为 `pending`。每个 source operation 必须在任何 Stream Load 前原子发布 canonicalizer/schema version、source checksum、canonical candidate manifest，以及 entity-head CAS 后的完整 child disposition/load manifest（`load_required`/`noop`/`quarantined`/`cancelled_by_deletion` 与稳定 batch identity）。`visible` 仅表示全部 required non-noop child 已成功 load 且没有 cancellation；没有 visible child 且其余工作均因 deletion barrier 取消时为 `cancelled_by_deletion`，visible 与 deletion-cancelled child 混合时为 `completed_with_cancellations`。Cancellation 不能覆盖已 visible 或独立 failed/quarantined child；load outcome unknown 必须先 reconcile 再分类。BullMQ job 只有在所有必需 child `VISIBLE` 且 filtered rows 为 0，或每个 nonvisible child 都有 durable terminal disposition 后才能离开重试队列；进程内 buffer 接收成功不等于 job 成功。
- R8. Delivery 为 at-least-once；重复 batch、response loss、worker crash 和 S3 replay 必须通过稳定 operation/batch identity、Doris Stream Load label、Postgres receipt/load ledger、Unique Key Merge-on-Write 与 sequence 得到同一最终状态。所有 ordering token 与 system timestamp 都必须来自 raw payload 或持久化 receipt，禁止在 replay 时重新读取处理时钟。
- R9. 对有资格成为 current state 的同一 `(project, entity type, entity id, logical version)`，系统必须按 R1 source-version contract 以原子 compare-and-set 认领唯一 canonical payload hash 与 immutable partition identity；并发不同 payload 的失败方进入 quarantine，不能使用“先读再写”或到达顺序决定赢家。已被更高 logical version 淘汰的旧版本可以幂等忽略。
- R10. canonicalizer 保留现有 prompt/model/cost enrichment、masking、rate limiting 和 failure tracking；其版本写入 receipt/manifest。第一次被 crash-safe protocol 成功发布的 enrichment 定义历史：在 entity-head CAS 前，完整 canonical child payload、hash 和 resolved prompt/model-price identifiers 写入与 raw 同生命周期的 S3 canonical-ingestion artifact，并由 Postgres 原子发布 pointer/candidate manifest；retry/recovery 重放该 artifact，不能按当前可变 Postgres 定义重新 enrichment。R1A 支持的领域副作用沿用其 queue/hook，并以稳定 operation/entity identity 幂等触发；R1B evaluator/experiment producer、scheduler、queue instance/registration、consumer 与 ClickHouse-backed service 在 R1A 由中央 capability gate 禁用/移除，ingestion 不得产生此类 job。副作用不绑定 Doris transport callback、不因 child-load retry 重复执行，也不借本项目引入通用 side-effect framework。

R1 source-version contract 固定如下；实现不得使用 replay 时的 `Date.now()`、worker 到达顺序或 Doris load time 决定 current winner：

| Entity | Logical version / sequence | 冲突与时间规则 |
|---|---|---|
| 冻结基线 v4 observation mutation | 必填 top-level ingestion-envelope `timestamp` | body `startTime`/`endTime` 可缺失；缺失 canonical `start_time` 时确定性使用同一个 envelope timestamp，绝不能使用 receipt/processing time，`endTime` 保持 nullable。多次 update 即使共享 `endTime` 也按 envelope timestamp 排序。同 token/hash 为 no-op、同 token/不同 hash quarantine、更晚 token 胜出；首次 canonical start 固定 UTC `partition_date`，跨日修改 quarantine |
| OTLP span snapshot | 合法 source `end_time`；仅对 protocol-valid incomplete snapshot 回退合法 source `start_time` | Adapter 按冻结 OTLP contract 验证；缺失该 contract 必需 source time 时 durable validation/quarantine，不能用 receipt/processing time。其余同 token/hash 为 no-op、同 token/不同 hash quarantine、更晚 token 胜出；首次 source start 固定 UTC `partition_date` |
| score | raw `updated_at`；契约没有该字段时使用 raw score `timestamp` | token 缺失为 validation failure；同 token 不同 hash quarantine；score name 不是 identity；`score_date` 固定为首次合法 raw score `timestamp` 的 UTC date，跨日修改 quarantine |
| dataset run item projection | Postgres control row 的 `updated_at`/declared version，在 receipt 创建时捕获 | replay 使用已捕获 token，不重新读取当前 Postgres row；immutable run date 来自首次捕获的 control-row `created_at` |
| blob/file reference | 继承 parent entity 的 logical version/partition，并带 stable file ID | parent 冲突或删除时同一 fence 生效 |
| trace/project tombstone | Postgres 生成的单调 deletion generation | tombstone sequence 高于所有 ordinary version，且不可由 replay 回退 |

没有 raw 业务时间的 system `created_at/updated_at` 使用 receipt 中一次持久化的 `accepted_at`；相同 operation 的重放必须复用它。

Timestamp token 先规范化为 UTC Unix epoch nanoseconds 的 checked signed `BIGINT`/decimal string；等价 RFC3339/protobuf 表达必须得到同一 token，TypeScript 禁止经 `number` 丢失精度。普通 sequence 必须小于 `INT64_MAX`，行级 terminal delete 使用保留的 `INT64_MAX`；Postgres deletion generation 另字段保存，不能与业务时间混算。

Entity identity 也必须固定：event/span 使用 collision-free、length-prefixed 的 `(trace_id, span_id)` composite（不能只用 OTLP `span_id` 或可歧义字符串拼接），score 使用 immutable `score_id`，R1B run item 使用 immutable run-item ID，file reference 使用 `(parent entity key, stable file ID)`。`analytics_entity_heads` 与 Doris physical key 必须从同一 typed identity encoder 产生并以跨 project/trace collision corpus 验证。

`canonical_payload_hash` 固定为 SHA-256(domain-separated、length-prefixed tuple：`canonicalizer/schema version`、typed identity、normalized version token、deterministic canonical child JSON)。JSON object keys 排序，timestamps/decimals/BigInt 使用规范字符串，child rows 按 typed identity 排序；arrays 与 Unicode code points 保持业务原值，不做会改变内容的重排/normalization。Raw byte checksum 与 canonical hash 是两个不同字段；resolved prompt/model-price enrichment 必须包含在 canonical hash 中。

Canonical publication protocol 固定为：Postgres CAS 先写 `canonicalization_fence`、预声明的 fence-specific object key 与 `manifest=pending`；worker enrichment 后以 conditional PUT 写不可变 artifact；随后 Postgres 在 fence 仍有效时用一次 transaction 发布 object pointer、artifact checksum 与 candidate manifest。Crash/lease takeover 必须先 HEAD/GET 并验证预声明 key：存在则发布/复用，只有确认不存在才允许新 fence 重新 enrichment。Stale fence 的 object 只能成为 7 天 lifecycle 管理的 orphan，不能被发布。Entity-head CAS 完成后，在任何 Stream Load 前再用 transaction 冻结每个 candidate 的 disposition 与完整 load-batch manifest。U1 必须冻结实际 production object-store provider，并以真实集成测试证明 conditional-create collision 与 read-after-write `HEAD`/`GET` 可见性/完整性；若不满足，必须先修改协议，不能只凭 S3-compatible API 名称假设一致性。

### 6.3 观测与查询

- R11. R1A 支持 trace、observation、session 和 user 的 list/detail；排序必须有确定性 tie-breaker，cursor pagination 跨页不得重复或遗漏。
- R12. Trace 只要包含至少一个未删除 event 就存在。优先使用真实 root span；缺失 root 时按 `start_time`、`span_id` 确定性选择 fallback，并将其视为 incomplete，而不是写入 synthetic root 或隐藏整条 trace。
- R13. Session/user 列表从 canonical events 聚合；新的 Postgres `TraceControlState` 只保存 bookmark/public 等稀疏控制状态，不复用 legacy trace table，也不成为 telemetry source of truth。Ingestion 仅可按稳定 operation id 执行 initialize-if-absent，不能更新已存在值；UI/API mutation 递增 durable revision 并始终优先，replay 不得覆盖。Central public-trace authorization middleware 与所有读 service 使用同一状态和 revision 规则；telemetry environment 与 UI/control state 保持不同语义。
- R14. 复用冻结基线的 `FilterState`、search grammar、Public API filter schema 和 MCP input schema。必须覆盖 metadata missing/empty、array any/none/all、score negative filters、null/empty、numeric/date comparisons、Unicode/raw escaped JSON 和 content substring 等高风险语义。R1A full-content search 必须提供显式 `[from,to)`，单次最大 30 天；point detail 不受此限制。UI 在 dispatch 前阻止缺少/超过范围的请求，保留 query、聚焦现有 date-range control 并解释 30 天限制；REST/MCP 返回相同的 structured `InvalidTimeRange`（含 accepted range 与 `maxDays=30`）。
- R15. 所有时间范围使用 UTC 和 `[from, to)`；每次 telemetry scan 都必须带可信 `project_id` 与日期/分区条件。ID-only detail 必须先由 `analytics_entity_heads` 定位分区，或使用 U1 唯一冻结且证明有界的策略；默认排序之外必须追加 entity ID tie-breaker。
- R16. Doris timeout/unavailable/overload 映射为领域错误：Public API 返回明确 5xx/503，MCP 返回 sanitized `InternalError`；不得泄露 SQL、host、table、credential 或内部 retry detail。

### 6.4 成本、指标与 dashboard

- R17. Token totals 使用整数，cost totals 使用固定精度 decimal；模型价格仍从 Postgres model definitions/enrichment 计算后写入 Doris。count、token、cost 必须精确一致，不允许近似误差。
- R18. Observation latency 为 `end_time - start_time`；TTFT/streaming latency 为 `completion_start_time - start_time`。缺少边界时间时返回 null，不以 0 代替 unknown。
- R19. Trace token/cost totals 只汇总当前、未删除、可计费 observation；真实 root span 仅按其自身 observation 计费，root fallback 不生成额外计费行。
- R20. Percentile、histogram 和 distinct 等允许 Doris 近似算法，但 canonical corpus 上相对误差不得超过 1%，返回 shape、bucket boundary 和 null handling 必须稳定。
- R21. R1A home dashboard 覆盖现有 home presets：traces、model costs、scores、traces/observations over time、model usage、user consumption、scores over time、trace/generation/observation latency、model latency 和 score analytics。Custom widget builder 不属于 R1A。

### 6.5 Scores、evals、datasets 与 experiments

- R22. `scores_current` 独立持久化 numeric、boolean、categorical score，使用 immutable score ID 和 version sequence；score 可先于目标 event 到达，随后正确关联，不因暂时 orphan 被丢弃。
- R23. **[R1B]** 最小 experiment 闭环为 dataset/item → create run → ingest trace → associate run item → execute eval → write idempotent score → view result。短暂不可见的 trace 使用有界 retry，部分失败状态对用户可见且可重试。启用前必须有 named owner、当前使用信号和验收人；同一个中央 capability gate 同时覆盖 navigation/direct URL/REST/MCP/tRPC/server action，以及 evaluator/experiment 的 producer、scheduler、queue registration 和 consumer，U9 只有在 Doris 实现与 backlog policy 同时通过后才能原子启用。
- R24. Prompt management、model connections、playground、dataset definitions、comments 和 annotations 的 Postgres 路径不迁移，但必须在 Doris trace 数据上完成回归。
- R25. UI、Public API 与 MCP 对相同 observation、score、dataset run item 和 metric 使用同一 service/repository 语义层，返回相同 ID、projection、pagination 和 totals；本迁移不改变 MCP tool name、input schema、annotations 或 project context。尚未启用的 R1B/R2 surface 使用固定 channel matrix：正常 UI navigation 隐藏，direct URL 显示带 capability code 与恢复说明的 unavailable page；已有 REST route 保持注册并返回 structured `UnsupportedFeature`（完全不支持时 HTTP 501，mixed batch 逐 child 报错）；已有 MCP tool 保持 name/schema/annotations 注册并返回 sanitized `UnsupportedFeature`；tRPC/server action 在 mutation/queue enqueue 前返回同一 capability error，相关 scheduler/queue/consumer 不注册。任何 channel 都不能返回空成功、404 dead link、半可用结果或后台 orphan job。

### 6.6 删除、retention 与 raw 数据

- R26. Trace delete 先写 Postgres authoritative tombstone，并在 Doris 写入所有查询都必须应用的 trace-level visibility barrier，再异步删除 event、trace-bound scores、Doris dataset-run analytics link、相关 trace control state 和可安全识别的 media/blob references。Postgres dataset/run definition 与审计控制记录保留，并显示 result trace unavailable/deleted。Project delete 先写独立于 Project row、长期保留的 Postgres deletion generation 与 Doris `project_tombstones` barrier，等待 barrier 前 operation 终态并最终 sweep 后，才移除整个 project 范围；唯一例外是 organization-scoped、无 payload 的 30 天 project-deletion status projection，以及永久保留的 generation/barrier evidence。所有 query、entity-head claim、batch seal、load、DLQ/replay 都检查 project generation；重复请求和部分失败可重入。
- R27. Trace/project tombstone generation/fence 在 entity-head claim、batch seal 与 Stream Load commit 前被重新验证；Doris trace/project barrier 与行级终止 sequence 共同防止已通过早期检查的 in-flight write、延迟 job、旧 version 或 client retry 复活数据。删除只有在 barrier 已 `VISIBLE`、barrier 前 operation 已终结且各 surface 已不可见后才能报告完成。
- R28. 删除请求幂等返回不可猜测的 `deletionOperationId`。Trace deletion operation 保持 project-scoped；project deletion operation 在 tombstone 写入时固定为 `(organization_id, deleted_project_id, generation)` scope，并独立于之后删除的 Project row、membership 与 API key。Project 删除后，其无 payload safe status 至少保留 30 天，只允许当前 organization 中仍满足 Community project-delete 授权的已登录用户通过 organization-scoped UI/tRPC 查询；已撤销 project API key、其他 organization 与猜测 ID 统一失败且不泄露存在性，UI 提供返回 organization project list/operations 的退出路径。只有 Postgres tombstone 与 Doris visibility barrier 都可靠落盘后才返回 `scheduled`，从该时刻起各 surface 必须 logical invisible；barrier 暂不可用时状态为 `retrying`、phase=`visibility_barrier`、`logicallyInvisible=false`，不能声称已隐藏。后续统一状态为 `scheduled`、`retrying`、`needs_attention`、`completed`；completed 表示 Doris/可识别 media 已清理，不表示多 trace raw object 已逐 trace 擦除。确认文案必须同时说明 logical invisibility、materialized/media completion 和最长 7 天 raw/canonical lifecycle；R1B experiment result 对已删 trace 显示 unavailable 并提供可退出路径。
- R29. Production 必须对专用 raw-ingestion 与 canonical-ingestion prefix 配置相同的最长 7 天 object lifecycle。Quarantine 只保存 safe metadata/hash 与这些 pointer，不复制出更长寿命的 shadow payload；每个 operation 持久化 expiry，未在 expiry 前解决时明确进入 terminal unrecoverable/data-loss，schema contract gate 只等待尚未过期的 recoverable set。若业务未来要求单 trace 立即物理删除 payload，必须先实现 file↔trace manifest/refcount 或按 trace 拆分对象，再提升删除承诺。
- R30. **[R1B]** Global Doris retention 默认关闭且不阻塞 R1A cutover；只有在 named owner、使用信号与恢复验收人存在后才实施和启用。每次 run 使用不可变 generation/cutoff，并持久化不可回退的 `purged-before` watermark 及跨 event/score/run-link/blob/head 的清理进度。关闭或延长 retention 不能让 replay 恢复已 purge 数据；不提供 per-project retention UI 或语义。

### 6.7 运维、升级与恢复

- R31. Schema migration 采用版本化、forward-only、可重复执行的 Doris migrations。Readiness 在 schema/version 不兼容时失败，web/worker 不接收会写入未知 schema 的流量。
- R32. Rolling deploy 期间相邻 app versions 的 queue payload、canonicalizer 和 Doris schema 必须兼容；破坏性 schema change 拆成 expand → migrate → contract。Contract gate 不仅等待旧 app 退出，还必须覆盖 BullMQ、DLQ、quarantine 与最长 7 天 raw/canonical replay horizon 中最老的、尚未过期的可恢复 canonicalizer/schema version。ClickHouse-only background migration 使用两版本退役：Release A 仍携带脚本，但为 manager 增加 durable retirement fence、build heartbeat 与 chunk-boundary cooperative abort/drain；只有所有 pre-A worker 已消失、已加载内存的目标 migration 已 drain/abort 后，fence 才能进入 retired。Release B 的 forward migration 对 active-lock TTL 内的已知 row 必须失败，不能仅清 DB lock；确认 fence/heartbeat 后才 terminalize stale row、移除脚本与 ClickHouse 并完成 Doris cutover。数据库 `finished_at`/lock 变化本身不能停止旧进程内的 `run()`。
- R33. 本项目不支持回滚到 stock Langfuse/ClickHouse。Rollback 仅指回滚到上一个 Doris-compatible app/schema 版本；生产接流前必须完成 Doris PoC 与恢复演练。
- R34. Recovery 组合 Doris backup/restore、Postgres backup、S3 canonical replay 和 load ledger/DLQ，但必须使用共同 checkpoint manifest。U8-owned checkpoint coordinator 获取单一 lease/fence 后记录 checkpoint generation 与 operation/load-ledger high-watermark；高水位之后的新 ingestion 仍可 accepted 并持久化排队，但 load、deletion barrier/cleanup 与可选 purge 等 Doris mutation 不得越过 fence。Coordinator 等待高水位内所有已 dispatch mutation `VISIBLE` 或 durable terminal，记录 Postgres exported-snapshot/backup identity 与 WAL LSN、trace/project generation、可选 purge watermark 和 Doris schema/version，再在 fence 内取得 Doris backup/snapshot ID；只有两端 artifact digest 均可验证后才原子 seal manifest，超时或任一失败只产生 aborted checkpoint 并释放 fence。Signed envelope 必须包含 `keyId`、creation time、单调 generation、predecessor-manifest hash、所有 artifact digest 与 deletion/purge high-watermark；旧 verification key 保留完整 backup horizon，最新 accepted generation/hash 锚定在 backup repository 之外的 append-only authority。默认 restore 在任何 unknown/expired key、chain/digest/authentication failure、unsealed checkpoint 或低于 external latest anchor 的旧但有效签名上，都必须在 mutation 前拒绝。该 high-watermark 是恢复契约，不宣称跨数据库 ACID snapshot：restore 先恢复 control/tombstone/purge gate 和 Doris snapshot，再将 Postgres 高水位后 ledger 视为待 reconcile/replay，不能因 `success` 字段直接信任。不存在可验证共同 checkpoint 或 replay window 已失效时必须报告 RPO breach，不得宣称恢复成功。
- R35. Operational health 覆盖 FE/BE availability、replica/disk/compaction、Stream Load visible latency/filtered rows/retry、queue backlog age、DLQ/quarantine age/expiry、Postgres receipt/ledger/control-row growth 与 cleanup watermark、query latency/error、connection pools、backup/restore。Production Doris 仅部署在 private network：web 只到 FE MySQL query endpoint，worker 分别持有 query 与 Stream Load 身份并只到 FE query/HTTP 与 allowlisted BE redirect endpoint，migrator/backup/restore 为无常驻 one-shot workload，monitor 只读 metrics endpoint；cluster-internal 端口在 U1 对选定 topology 冻结并默认拒绝其他 ingress。Local compose 仅绑定 `127.0.0.1`/private bridge。Query/load/migration/backup/restore credentials 分离，通过 runtime secret injection 提供；privileged credentials 不进入 web/worker image，并用双凭据滚动轮换后撤销旧身份。Product monitors 延期不影响这些告警。

## 7. 关键用户流程

### F1. 上报并查看 trace

1. 应用通过 v4 SDK 或 OTLP 发送 spans。
2. API 完成 auth、masking、rate limit，将 raw batch 与 durable ingestion receipt 落盘，ingestion outbox 发布 BullMQ，返回 accepted 与 `operationId`。
3. Worker 先按 receipt 预留 canonicalization fence/object key，发布或 reconcile immutable canonical artifact/candidate manifest，再按 source-version contract 原子认领 entity head、冻结 disposition/load manifest，完成全部 required Stream Load child，并在终态后 ack。
4. 调用方按 project-scoped status endpoint 观察 queued/persisted/visible 或 child-level failure；visible entity 提供可访问 link。
5. 用户在 trace list/detail 看到 trace、observation、cost、token 和 latency；UI、REST、MCP 结果一致。

### F2. 搜索和调试

1. 用户选择 project/time range，组合 metadata、tag、name、model、score、user/session 和 content search；全文搜索没有合法的最长 30 天范围时，UI 保留输入并引导修正，REST/MCP 返回同一 validation error。
2. Query service 强制 project/date scope，编译 Doris query。
3. 稳定分页返回 compact row；detail 按 ID 读取完整 I/O。
4. Doris 失败时返回显式 unavailable，不返回空成功。

### F3. 评估与实验（R1B）

1. 用户创建 dataset run，应用上报关联 trace。
2. Evaluator 等待目标可见并执行，幂等写 score。
3. 用户在 run/result、trace detail、score view 和 home dashboard 查看一致结果。
4. 单 item 失败可见且可重试，不把整个 run 伪装为成功。

### F4. 删除与重放

1. 管理员确认 logical invisibility、materialized/media cleanup 和 raw/canonical lifecycle 的差异后请求删除 trace；系统先持久化 tombstone，并幂等返回 `deletionOperationId`。
2. Doris barrier 可见后状态才进入 `scheduled` 且各 surface 不可见；barrier 失败时保持 `retrying`/`logicallyInvisible=false`。随后 Worker 分步删除各数据面并记录 `scheduled`/`retrying`/`needs_attention`/`completed`，失败可重试。
3. 受影响的 nonterminal ingestion child 在 reconcile unknown load 后进入 `cancelled_by_deletion`，但不能覆盖已 visible 或独立失败的 child；source operation 因 child 组合进入 `cancelled_by_deletion`、`completed_with_cancellations` 或原有 failure 终态。后续延迟 replay 与新 client retry 命中 generation fence/tombstone，被拒绝或保持 delete state。
4. Raw/canonical ingestion object 到 object lifecycle 时过期；在此之前不宣称单 trace payload bytes 已物理擦除。若删除的是 project，project API key 随即失效；管理员从 organization-scoped operations 页面查询保留 30 天的 safe status，不再依赖已删除 Project row。

### F5. 故障与恢复

1. Doris 暂时不可用时，可靠接收的 S3/queue 数据进入 retry/backpressure，不 ack 丢弃。
2. 查询入口显式 unavailable，运维看到 backlog/load/query 告警。
3. 正常运行时 checkpoint coordinator 短暂冻结高水位后的 Doris mutation dispatch；新 ingestion 继续 durable accepted/queued。高水位内 mutation 全部可见/终态后，coordinator 在 fence 内记录 Postgres snapshot/WAL 与 Doris snapshot，并在两端 artifact 验证后 seal/sign manifest。
4. Doris 恢复后 drain queue；灾备时只选择通过外部 key 认证且 artifact digest 全部有效的 sealed common-checkpoint manifest。
5. 先恢复 trace/project tombstone 与已启用的 purge gate，再将高水位后的 ledger 视为 unknown 并 reconcile，随后重放 S3 canonical artifact；raw 仅用于首次 canonicalization 尚未成功的 operation。不存在共同恢复点时明确报告 RPO breach。

### F6. 生产者就绪与核心上线

1. U1 冻结 producer census 的权威来源与观察窗口：至少包含内部 service/config ownership source，以及按 credential fingerprint/principal + endpoint/protocol 聚合的 authenticated ingestion traffic；默认窗口 30 天，若正常 producer 周期更长则取该周期加 grace。
2. 上线清单必须与 census 双向 reconcile，枚举每个 producer、owner、当前 SDK/endpoint、目标 v4 SDK/OTLP protocol 和迁移日期；未知 active principal、遗漏项或无法解释的 delta 直接失败，明确排除项记录 owner、reason 与 expiry。
3. 每个 R1A 必需 producer 在候选 Doris-only build 上完成一次真实 project-scoped end-to-end：accepted operation 最终 visible，trace 可查询。
4. 任一必需 producer 仍依赖 legacy write 或没有 owner/证据时，阻断 R1A 流量切换；fresh database 或“清单里没有它”都不能替代 producer readiness。以后上游版本或新 producer 通过显式需求评审和 corpus 更新进入。

## 8. R1A/R1B 验收示例

- AE1. 给定同一 OTLP batch 被提交三次且第一次响应丢失，当 worker 重试时，Doris 中每个 entity 只有一个正确 current state，全部 required child loads 已 `VISIBLE` 或 durable terminal failure 后 source operation 才结束。
- AE2. 给定旧 source-version token 在新 token 后到达，当 compaction 前后查询时，返回值一致且旧 token 不能覆盖新值。
- AE3. 给定一个 trace 没有 explicit root event，但存在两个 spans，当 UI/API/MCP 查询时，trace 可见并使用确定性 fallback，跨页不重复。
- AE4. 给定中文、韩文、阿拉伯文、emoji、raw JSON 与 `\\uXXXX` content，当 search/filter 时，结果符合冻结 corpus，metadata expansion 不被 preview truncation 误伤。
- AE5. 给定 Doris timeout，当查询相同 trace 时，REST 返回明确 5xx/503、MCP 返回 sanitized internal error，不返回空数组或 NotFound。
- AE6. 给定 Project A API key 和 Project B entity ID，当 UI/API/MCP 查询时，不返回 B 的行、count、timing-sensitive existence detail 或 SQL 信息。
- AE7. 给定 score 先于 observation 到达，当 observation 后续可见时，score 能正确关联；重复 score delivery 不产生重复 current score。R1B evaluator 启用后复用同一幂等保证。
- AE8. 给定 trace delete 与一个已通过早期 tombstone check、尚未 Stream Load 的新 span 并发，当删除状态完成后，Doris barrier 使 UI/API/MCP 始终不可见，in-flight event 不能复活，raw/canonical object 状态符合 7 天 lifecycle 承诺。
- AE9. 给定 U1 冻结的 production topology 发生声明故障（HA 为 FE failover/BE loss；非 HA 为服务中断后 restart/restore），系统无 silent drop，durable backlog 在声明 RTO 内清空且不虚构未提供的 failover。
- AE10. 给定 checkpoint fence 期间仍有新 ingestion accepted，且 Postgres 与 Doris snapshot 时间存在偏斜，当按已认证 sealed checkpoint 的 high-watermark 恢复并执行 reconciliation/canonical replay 时，高水位内 mutation 与 manifest 一致，高水位后 ledger 不因历史 `success` 被盲信而是补齐，已删除或 R1B 已 purge trace 被 tombstone/watermark 阻止重新出现；checkpoint 超时/半成品、unknown/rotated-away key、broken predecessor chain 或低于 external latest anchor 的旧有效签名都在 mutation 前明确失败。
- AE11. 给定同一时间范围和 filters，当 home dashboard、Public metrics API 与 MCP metrics 查询时，精确 count/token/cost 相同，percentile/histogram 在声明容差内。
- AE12. 给定 R1A fresh deployment，当扫描 runtime dependencies、compose 和 storage env 时，没有 ClickHouse analytics client/service/config；`AUTH_CLICKHOUSE_CLOUD_*` 作为第三方身份 provider 名称不被误删。
- AE13. 给定两个 worker 并发处理同 entity/source-version token、不同 canonical payload，当同步通过预处理屏障时，原子 entity-head CAS 只认领一个 hash，另一个 operation durable quarantine；compaction/restart 后赢家不变。
- AE14. 给定同一 raw object 跨 UTC 午夜、跨 app version 和 restore 后 replay，同一 entity 的 immutable partition date、physical key 与 current winner 完全一致。冻结 v4 create/update body 可同时缺少 `startTime/endTime`，且多次 update 可共享 `endTime`：mandatory envelope timestamp 仍确定性补 canonical start 并排序 mutation；OTLP 缺失其协议必需 source time 或跨日 partition mutation 时 quarantine，不能用 receipt/processing time 补值或写出第二个 current row。
- AE15. 给定 project delete 与一个已接受、尚未 load 的 operation 并发，当删除完成后，manifest-pending、post-CAS、sealed 与 load-unknown child 经 reconcile 后分别落到 visible、既有 failure 或 `cancelled_by_deletion` 终态；长期保留的 project barrier 阻止 operation、DLQ 与 replay 重新写入任何 project row，最终 scoped sweep 除 organization-scoped 30 天 safe status 与永久 generation/barrier evidence 外为空。
- AE16. 给定 worker 在 canonical artifact PUT 前后或 pointer/manifest transaction 前后崩溃，且 model price/prompt 随后修改，当 retry/restore 时，系统先 reconcile 预声明 object key；已成功发布的 artifact/hash/cost/prompt enrichment 不变，不存在已发布 artifact 时才允许新 fence enrichment；artifact 已过期则明确报告 RPO breach。
- AE17. 给定 accepted OTLP request，当调用方使用响应头中的 `operationId` 查询时，只能在同 project 看到 operation/child 状态、expiry 和 visible links；跨 project、猜测 ID 或未授权访问不泄露存在性或 payload。
- AE18. 给定全文 query 没有日期或超过 30 天，当从 UI/REST/MCP 发起时，UI 不 dispatch 且保留 query/聚焦日期控件，REST/MCP 都返回含 `maxDays=30` 的 `InvalidTimeRange`。
- AE19. 给定 trace/project deletion 的 barrier 或后续 cleanup 部分失败，当用户刷新或跨 UI/API 查询状态时，barrier pending 明确显示 retrying/`logicallyInvisible=false`；scheduled 后 logical data 始终不可见，后续状态一致，completed 文案不声称 raw bytes 已立即擦除。Project row/API key 被移除后，只有当前 organization 内仍具删除授权的 session 能从 organization-scoped operations 页面读取同一 safe status；撤销的 key、跨 organization 与猜测 ID 不泄露 existence。
- AE20. 给定某个 active legacy producer 被漏写进人工清单，但仍出现在 service/config census 或 declared-window authenticated traffic 中，当执行 cutover gate 时，R1A 被阻断；只有 inventory/census 无 unexplained delta 且每项都有 owner/date 和 Doris-only E2E evidence 才能接流。
- AE21. 给定旧 worker 已把 ClickHouse-only migration 加载进内存，当 Release A retirement fence 启用时，pre-A heartbeat 会阻止推进，A worker 在 chunk boundary drain/abort；Release B 遇 active-TTL lock 明确失败，只在 fence 成功与 lock 过期后 terminalize row，manager 不会 require 已删除 script。
- AE22. 给定被篡改的 backup artifact/manifest、signing-key rotation 后仍在 retention 内的备份，或旧但有效签名的 manifest rollback，当 restore 开始时，digest/keyId/key-retention/chain/external-anchor 校验分别得到预期结果，并在不满足契约时于任何数据 mutation 前停止。

## 9. 非功能目标与 PoC 基线

以下是实现和验收的初始 planning baseline，不是对未知生产流量的承诺。进入生产前应替换为内部真实 30 天样本并保留同一测试方法。

| 维度 | 初始基线 |
|---|---|
| Retained corpus | 30 天 10M current events；这是查询/存储基线，不代表 100 events/s 全天持续 30 天。包含真实 project skew、trace/span cardinality、宽 metadata、长 I/O 和 scores，并记录压缩字节数；R1B experiment links 只作为未来 contract seed，不计入 R1A production DDL/PASS gate |
| Ingestion capacity | 最低能力测试为 100 events/s 持续 60 分钟、500 events/s 持续 10 分钟；实施前另填团队真实 steady-state rate 与 duty cycle。若真实流量是 100 events/s 全天持续，retained corpus 必须相应提升到约 259M/30 天后重新定案 |
| 可见性 | accepted 后 `visible` p95 ≤ 5s，p99 ≤ 10s；故障恢复同时承接新流量并以至少正常写入速率 4× drain backlog，具体完成时间按故障积压量计算 |
| 查询 | detail p95 ≤ 1s，list/filter p95 ≤ 2s，home dashboard bundle p95 ≤ 5s；固定并发/query mix 下分别记录 cold/warm cache、1/7/30 天与 page 1/10/100；同硬件相对 ClickHouse reference regression ≤ 20% |
| 正确性 | count/token/cost/row identity 精确；声明近似指标误差 ≤ 1%；filtered load rows 恒为 0 |
| 稳定性 | response loss、worker crash 与 U1 冻结 topology 的声明故障下零 silent drop、零重复语义；只有 HA target 承诺 BE loss/FE failover |
| 隔离 | 任一 UI/API/MCP query 都不跨 project；export/backfill 不在 R1A，但 load test 不得拖垮 interactive query |
| 恢复 | 仅从 sealed、key-valid、hash-chain-valid 且不低于 external latest anchor 的 checkpoint restore；空集群恢复后核心 table count/hash 与 query corpus 一致，RPO 不超过实际 backup/raw retention window |

## 10. Doris 产品约束

- 截至 2026-07-17，官方将 `4.0.7` 标为 Stable、`4.1.3` 标为 Latest。R1A 只以 `4.0.7` 的精确 patch image 做 PoC/生产基线，并在实施开始时重新核对其官方支持状态；4.1.x canary/soak 属于 cutover 后的 upgrade workflow，不扩大 R1A PoC。
- R1 使用存算一体 cluster。Local development 使用最小 FE+BE。内部运维必须在 R0 benchmark 开始前，从“明确接受 RPO/RTO 的非 HA topology”或“参考 3 FE Followers、至少 3 BE、3 replicas”中冻结一个 production target、资源预算和 RPO/RTO；R0–R1A 只对该 target 出具生产结论。
- SQL/query 走 Doris MySQL protocol；ingestion 走 HTTP Stream Load。R1 不依赖 Arrow Flight SQL、Group Commit、2PC、CCR 或 4.1-only schema feature。
- `events_current` 是唯一 application-written event fact table；Doris `trace_tombstones` / `project_tombstones` 是 lifecycle barriers，不是第二份 event storage。R0 必须在进入 production DDL 前冻结 key order、partition/bucket、search index、byte-based load limits，以及是否需要 Doris-managed trace summary 和 dashboard rollup；应用不得对 full/core 两张事实表分别写、改、删。
- Event partition date 来自 canonical `start_time` 的 UTC date，并作为 entity identity invariant 固化在 Postgres entity head；冻结 v4 body 缺失 start 时，该 canonical start 来自 raw envelope timestamp，OTLP 则必须来自其 source-time contract，二者都不是 receipt/processing time。后续跨日修改进入 conflict/quarantine。Point lookup/delete 可使用 entity head locator。
- 热点 identity/filter/group/order 字段使用 typed columns；长尾 metadata/model/tool parameters 使用 VARIANT。不能把 ClickHouse `Map` 机械翻译成 Doris `MAP`。

## 11. 版本与交付阶段

### R0 — Go/No-Go PoC

- 冻结 canonical corpus 和 Top 30–50 storage-sensitive queries。
- 冻结 retained rows/bytes、真实 write duty cycle、payload 分布、project skew、query mix/concurrency 与 cold/warm 方法；比较候选 key/bucket/index/projection，产出唯一 production physical design。
- 冻结真实 object-store 的 conditional-create/HEAD/GET 一致性结论、producer census 来源/窗口，以及 backup repository、manifest key lifecycle 与 repository 外 append-only latest-checkpoint anchor。
- 验证 Unique Key/sequence/delete barrier、Stream Load response-loss、Unicode search、percentile、VARIANT、writer memory/backpressure、failure recovery、engine backup/restore 和目标负载。
- 任一 correctness/durability gate 无法通过则停止产品改造，不进入全面迁移。

### R1A — Internal Core Cutover

- 完成 modern SDK/OTLP、ingestion status、trace/observation/session/user、filters/search、home dashboard、基础 scores、核心 Public API/MCP、manual trace/project deletion、backup/recovery 和 operational health。
- runtime、dependencies、compose、env、queues 和 query paths 不含 ClickHouse analytics dependency。
- producer readiness 清单与 service/config、authenticated-traffic census 双向一致；R1B 能力在 UI/API/MCP/tRPC 与 producer/scheduler/queue/consumer 上一致 unavailable，R1A 不创建 evaluator/experiment job。

### R1B — Adoption-Gated Extensions

- 只有在能力记录 named owner、当前使用信号和验收人后，实施 evaluator/basic experiment 执行闭环与可选 global retention。
- R1B 使用同一 canonical/Doris boundary，不恢复 ClickHouse、v3 storage 或 Enterprise code，也不改变 R1A 的上线完成定义。

### R2 — Demand-Driven Parity

- 按真实需求分别立项 legacy API adapter、custom dashboards、product monitors、batch export、strict raw-object deletion 和更高级 analytics。
- 每项仍写 canonical events/Doris，不重新引入 v3 storage 或通用 multi-database framework。

## 12. 工作量级估算

这是一项 repository-wide storage rewrite，不是 driver replacement。用于排期的粗略量级：

- R0 PoC：2–4 engineer-weeks。
- R1A core cutover：PoC 通过后约 8–12 engineer-weeks。
- R1B adoption-gated extensions：被真实需求触发后约 4–8 engineer-weeks。
- R2 parity：按所选能力额外 8–12+ engineer-weeks。

估算假设 1–2 名熟悉 TypeScript、Langfuse 和 Doris 的工程师，且不存在历史 ClickHouse 数据迁移。真实数据规模、Doris 运维能力或 legacy SDK 数量会显著改变工期。

## 13. 风险与明确接受的限制

- R1A 不兼容 legacy trace/observation write endpoint；调用方必须升级 SDK/OTLP，接口返回可行动的 unsupported error。
- R1A 不提供单 trace raw OTLP 立即物理删除；raw/canonical ingestion prefix 最长保留 7 天。
- R1A 不提供 evaluator/experiment execution、global/per-project retention、自定义 dashboard、product monitors 或 batch export；前两项只有满足 R1B adoption gate 后才启用。
- fresh deployment 一旦接收 Doris 数据，不能回滚为 stock ClickHouse build；恢复依赖 Doris-compatible app、backup 和 S3 replay。
- 上游 Langfuse 会继续改变 v4 schema/query semantics；本 PRD 只承诺冻结基线。内部 fork 合并任何上游变更前必须完成 demand review、更新 corpus，再改 adapter。

## 14. 外部依据

- [Apache Doris downloads and release status](https://doris.apache.org/download/)
- [Apache Doris versioning policy](https://doris.apache.org/docs/4.x/features-architecture/versioning/)
- [Stream Load](https://doris.apache.org/docs/4.x/key-features/stream-load/)
- [Transactions and label lifecycle](https://doris.apache.org/docs/4.x/data-operate/transaction/)
- [Unique Key model](https://doris.apache.org/docs/4.x/table-design/data-model/unique/)
- [Concurrent update sequence](https://doris.apache.org/docs/4.x/data-operate/update/unique-update-concurrent-control/)
- [Partitioning and bucketing](https://doris.apache.org/docs/4.x/table-design/data-partitioning/basic-concepts/)
- [VARIANT data type](https://doris.apache.org/docs/4.x/sql-manual/basic-element/sql-data-types/semi-structured/VARIANT/)
- [Inverted index](https://doris.apache.org/docs/4.x/key-features/inverted-index/)
- [Monitoring metrics](https://doris.apache.org/docs/4.x/admin-manual/maint-monitor/metrics/)
- [Backup and restore](https://doris.apache.org/docs/4.x/admin-manual/data-admin/backup-restore/overview/)
