# 自托管 Community Extensions

本分支基于较早的 Doris 社区源码 `ff78576fa`，上游版本为 Langfuse `3.218.0`。
九项扩展通过独立模块接入现有 MIT 控制面和存储契约。代码只提取必要实现和接入点，
提交历史从个人社区基线延续。根 `LICENSE`、`ee/LICENSE` 和第三方 notices 保留。

## 启用与配置

在 Web 和 Worker 使用同一设置，修改后重启两个服务：

```dotenv
LANGFUSE_COMMUNITY_EXTENSIONS_ENABLED=true
```

默认值为 `false`。关闭时保留上游 entitlement 和 license 行为；开启时使用下表中的
独立实现。该开关不生成 Enterprise key、不修改签名校验，也不把部署标记为
`self-hosted:enterprise`。现有 EE 源码仍受其原许可约束。

| 能力 | 使用方式与行为 |
| --- | --- |
| Audit Logs | 组织和项目设置中的审计页面，支持分页与变更详情。查询校验资源权限，敏感快照字段脱敏，API 写入与审计共用事务。 |
| Project RBAC | 项目成员中指定角色；显式项目角色覆盖组织继承角色，`NONE` 拒绝项目访问。更新校验项目属于当前组织。 |
| Data Retention | 项目设置中指定至少 3 天的保留时间。Worker 根据所选后端删除过期数据；Doris 使用固定 cutoff、项目作用域、claim/fence 和可恢复水位。 |
| Ingestion Masking | 配置下面的 HTTP callback，在 canonicalization 前脱敏，处理后的持久化 payload 用于后续重放。 |
| Protected Prompt Labels | 项目设置中保护标签，写入仍校验用户资源权限。保留上游 `latest` 标签规则。 |
| Organization Creators | `LANGFUSE_ALLOWED_ORGANIZATION_CREATORS` 为逗号分隔的邮箱列表；空配置保留所有用户可创建组织的行为。 |
| UI Customization | `LANGFUSE_UI_*` 配置 logo、文档/支持/反馈链接、API host、默认模型与产品导航。导航隐藏只控制显示，API 继续执行各自权限校验。 |
| Admin API | 现有 `/api/admin/*` 与组织 key 管理的公开 API；校验管理凭据、API key scope、组织/项目边界，保护最后一个组织 owner，并记录审计。 |
| SCIM Users | 现有 `/api/public/scim/*` 的 Users 子集及 schema/config 端点；支持受限 filter、分页、单操作 PATCH 和 activation/deprovisioning。拒绝非法 payload 与最后一个 owner 的移除或降级。 |

脱敏示例（地址由部署者提供）：

```dotenv
LANGFUSE_COMMUNITY_MASKING_CALLBACK_URL=https://masking.example.org/process
LANGFUSE_COMMUNITY_MASKING_CALLBACK_TIMEOUT_MS=500
LANGFUSE_COMMUNITY_MASKING_CALLBACK_FAIL_CLOSED=true
LANGFUSE_COMMUNITY_MASKING_MAX_RETRIES=1
LANGFUSE_COMMUNITY_MASKING_PROPAGATED_HEADERS=x-mask-tenant
```

callback 限制 URL、DNS 解析和连接目标，拒绝 redirect，限制响应体和超时时间，
仅传递明确列出的 headers。连接内网服务时按实际目标配置
`LANGFUSE_COMMUNITY_MASKING_WHITELISTED_HOST`、`_IPS` 或 `_IP_SEGMENTS`；
这些配置仍经过目标验证。示例环境文件不包含真实服务地址或凭据。

导航示例：

```dotenv
LANGFUSE_UI_HIDDEN_PRODUCT_MODULES=playground
```

可用模块为 `dashboards,tracing,evaluation,prompt-management,playground,datasets`。
设置 `LANGFUSE_UI_VISIBLE_PRODUCT_MODULES` 时以该列表为准。

## 代码入口与边界

- shared capability contract：`packages/shared/src/features/community-extensions/`。
- shared 服务与脱敏：`packages/shared/src/server/community-extensions/`。
- 后端无关脱敏 adapter：`@langfuse/shared/src/server/ingestion-masking`，生产导出指向 `dist`。
- Web access、hooks 与审计 UI：`web/src/features/community-extensions/`。
- Admin handlers：`web/src/features/admin-api/`；UI 配置：`web/src/features/ui-customization/`。
- Worker retention：`worker/src/features/community-extensions/data-retention/`。

独立扩展目录不得直接 import EE 源码或写入 license 伪造标记。
`pnpm run community:check` 在 lint、typecheck 和 build:check 前验证静态边界，
`pnpm run community:check:test` 验证检查器的拒绝行为。公共接入层保留上游官方路径，
如开关关闭时的脱敏 adapter，以及仅 Cloud 部署加载的 SFDC 集成；这些不是独立扩展
提供的功能，原许可继续适用。静态检查只证明所检查的源码边界。

Doris 项目 retention 复用 MIT global retention 状态机和 materialized deletion writer，
以 `project:<projectId>` 隔离 durable state。设置关闭后仍完成已发布的 active run；
删除确认可见后才清理控制面 heads。一个 ingestion operation 的对象仍被其他 heads
共享时保留它们；清理对象失败时保留 references 供重试。历史 EE retention helpers
未复制到本扩展。

## 验证与运行范围

针对性测试覆盖默认关闭行为、资源权限、最后一个 owner、审计脱敏、callback
限制和重放、retention 恢复及共享对象保护。跨包改动运行 lint、typecheck 和
shared/web/worker 测试；SCIM 定义使用 Fern 检查并重新生成 SDK。

本轮实际命令、结果和环境限制以
[当前进度记录](2026-07-21-001-feat-doris-community-parity-loop-progress.md#community-extensions-2026-10-05)
为准。历史 Doris 测试记录保留为历史证据，不能代替本轮真实后端验收或生产部署验证。
