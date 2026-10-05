# 自托管社区能力提取方案

本方案以已发布的 `codex/personal-public` 为基线，准备把自托管部署需要的部分
商业版类似能力作为独立的 Community 扩展维护。本提交只包含方案，没有增加或
启用这些能力。

## 提取边界

已有独立实现只有在发布者确认有权公开后才能复用。目录名称、个人作者邮箱和
没有 EE import 都不能单独证明代码来源。曾在受限 EE 目录下创建、再移到普通
feature 目录的文件，需要单独核对许可和来源；改名、移动文件或改写措辞不能代替
独立实现。

公开分支只接收经过逐文件核对的实现与必要接入点，不合并公司分支的整段历史，
也不携带公司部署、CI、地址、账号或本地配置。原有许可和第三方 notices 保留。

## 能力与建议顺序

| 顺序 | 能力 | 独立实现与提取范围 | 关键验证 |
| --- | --- | --- | --- |
| 1 | Project RBAC | 复用 MIT 核心的组织/项目 membership 模型，接入邀请、创建、更新和 signup；显式项目角色覆盖继承角色 | 无 capability 时保持旧行为；`NONE` 拒绝项目访问；跨组织操作失败 |
| 1 | Protected Prompt Labels | 在 MIT prompt 路由和设置页增加独立 capability 接入，保留现有资源授权 | `latest` 不能被保护；无写权限用户不能改标签或受保护 prompt |
| 1 | Organization Creator Allowlist | 独立的邮箱解析和组织创建策略，空名单保持旧行为 | 大小写、无邮箱、非法配置和直接 API 调用 |
| 2 | UI Customization | 独立提供 logo、链接、默认模型配置和导航可见性；接入布局与设置路由 | 默认布局回归；隐藏导航不改变 API 授权；配置 URL 处理 |
| 2 | Audit Logs | 复用 MIT PostgreSQL 审计表与 writer，独立实现页面和查询入口 | 组织/项目隔离、分页、敏感字段脱敏、同事务审计 |
| 3 | Ingestion Masking | 独立的受限 HTTP callback adapter，在 canonicalization 前处理 payload | URL/DNS/redirect 限制、响应大小、timeout/retry、失败策略、重放一致性 |
| 3 | Data Retention | 独立 scheduler/processor，分别调用 MIT ClickHouse 与 Doris 删除契约；有 EE 来源的历史 helper 不直接搬入 | 固定 cutoff、项目隔离、重试/崩溃恢复、水位与备份恢复语义 |
| 4 | Admin API | 独立的组织、项目、API key 和 membership handlers，使用现有公开契约 | API key scope、Cloud 禁用、最后一个 owner 保护、审计与异步删除 |
| 4 | SCIM Users | 基于公开 SCIM 2.0 规范独立实现 Users 子集与 schema/config endpoints | 认证组织隔离、filter/PATCH、activation/deprovisioning、最后一个 owner 保护 |

RBAC、protected labels 和组织创建策略优先，因为主要复用 MIT 控制面，不需要新增
analytics 删除或外部回调链路。Admin API/SCIM 放在其依赖的授权与审计能力之后。

## 接入设计

- 普通 feature 根下提供 `community-extensions` capability contract 与服务；服务
  只注册已经完成实现和验证的能力。Web、Worker 和 shared 保持现有依赖方向。
- 使用显式、默认关闭的 `LANGFUSE_COMMUNITY_EXTENSIONS_ENABLED` 配置与按能力
  注册表；配置开启不能让尚未实现的服务返回成功。
- Web session、前端 hooks 与后端 guard 使用同一 capability 注册结果。每个入口
  同时校验部署能力和用户/组织/项目权限；UI 可见性不作为授权边界。
- 已有官方 license、entitlement 与 plan 机制保留。独立能力进入自己的实现，不
  伪造 key、修改签名校验或把部署标记为 `self-hosted:enterprise`。
- 独立实现不 import `ee/`、`web/src/ee/`、`worker/src/ee/` 或官方 entitlement
  adapter；最终 Web/Worker 产物也要检查实际模块图，不能只检查源文件目录名。
- ClickHouse 和 Doris 分别验证；Doris 不通过回退到 ClickHouse 掩盖缺少实现。

## 每个实现增量的发布条件

1. 记录复用代码的公开授权与来源；受限来源未解决时，按公开契约独立实现或排除
   该能力。
2. 在现有 feature suite 验证真实行为与上述负例。跨包改动运行仓库要求的 lint、
   typecheck 和相关 shared/web/worker 检查；公共 API 变动同步 Fern。
3. 对相关 UI 使用公开 seed 场景做浏览器验收；有数据模型需求时扩展 seed 场景，
   不用临时数据库插入代替。
4. 检查模块依赖、代码来源、敏感配置和实际产物；静态相似度检查不作为独立作者
   身份的证明。
5. 验证完成后，只把该增量提交推到个人 GitHub 新分支，并核对远端提交。

实现范围、来源确认和本轮执行状态沿用
[当前记录](2026-07-21-001-feat-doris-community-parity-loop-progress.md#personal-public-candidate)。
