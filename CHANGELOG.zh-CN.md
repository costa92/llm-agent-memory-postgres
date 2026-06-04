# Changelog

`github.com/costa92/llm-agent-memory-postgres` 的所有重要变更都将记录在本文件中。

<!-- Keep a Changelog format: https://keepachangelog.com/en/1.1.0/ -->
<!-- Semver: https://semver.org/ -->

## [Unreleased]

### Added

- M8a-prep 中继加固 + schema 迁移框架 v2：
  - **schema 迁移框架 v2。** `migrationGroup` 类型带有每组的
    `Transactional bool` 控制，基于分组的运行器会原子地记录每个
    version，并提供 `AcceptableSkewVersions = 5` 的余量，使得一次
    滚动部署——较新的 pod 连到了较旧 pod 的数据库——不再强制要求
    严格相等的 version 检查（`ErrSchemaVersionAhead`
    只在超出该余量时触发）。
  - **schema 分组 v2。** 三个新的可空 outbox 列（`claimed_by`、
    `claimed_at`、`lease_expires_at`）+ 部分索引
    `<outbox>_lease_idx (status, lease_expires_at) WHERE status='processing'`。
    全部采用 `ADD COLUMN IF NOT EXISTS` 以保证部分失败的幂等性。
  - **中继重写。** `ClaimBatch` 以 `FOR UPDATE SKIP LOCKED` 认领待处理或
    租约过期的行，并递增 `attempt_count`。`Ack` 携带一个所有权谓词
    （`claimed_by = $workerID AND lease_expires_at >
    NOW()`），使得租约被抢走的 worker 无法变更该行。
    零影响行数返回新的 `ErrLeaseLost` 哨兵值。`RunOnce`
    通过 `errors.Join` 收集 ack 错误，而非提前退出，因此
    单个热点行不会阻塞批次中其余行的进展。
    `Release(ctx)` 将仅自己拥有的租约翻回 `pending` 以实现优雅关闭
    （不会递减 `attempt_count`——该认领仍计入重试预算）。
  - **RunStats.LeaseLost** 字段向调用方暴露每个 tick 的租约丢失计数；
    指标可据此接线，对卡住的发布告警。
  - **Worker 身份。** `NewRandomWorkerID()` 返回
    `<hostname>-<32-hex-char>`（128 位 `crypto/rand`）；当
    `os.Hostname()` 失败时以字面量 `unknown` 替代。worker
    在每次进程启动时都会重新生成它，使得崩溃 pod 的租约可以
    按租约时间而非按身份被重新认领。
  - **写侧 event-type 白名单。** `validateEventType` + 新的
    `ErrInvalidEventType` 哨兵值会在 `AppendEvent`、
    `EnqueueOutbox` 和 `mutateRecord` 处拒绝拼写错误。为 M8a 在
    白名单中新增两个 event type：`memory_promoted`、`memory_dedupe_collapsed`。
  - **`outboxStatusFailed`** 常量；`attempt_count` 达到
    `MaxAttempts` 的行会永久转入此状态，直到运维人员对其执行
    `RequeueFailed`。
  - **运维 API。** `Store.ListFailed(ctx, limit)` 返回失败行的
    最新优先窗口（id、aggregate_id、event_type、
    attempt_count、last_error、created_at）。`Store.RequeueFailed(ctx,
    outboxID)` 将一个 `failed` 行翻回 `pending` 并把
    `attempt_count` 重置为 0；对非 `failed` 行为空操作（RowsAffected=0）。
  - **`LeaseAwarePublisher`** 测试桩，带有可注入的 `PublishHook` 用于
    延迟/错误场景（尤其是「发布超过租约 TTL」这一情况）。
- 为 M7 校验遥测 + 决策链路工作，向既有 schema 迁移序列追加了
  `memory_decision_trace` 表与三个配套索引（tenant+time、
  request、stage+reason）。`reason` 列在 v1.x 中为自由格式，
  并将在 v2（M8）中冻结为枚举。

### Changed

- `HeadSchemaVersion` 提升至 **2**；`SchemaVersion` 保留为
  别名，以向后兼容 M5-M7 的调用方。
- Postgres 11+ 现在是最低支持版本（M8a-prep 依赖
  可空的 `ADD COLUMN` 只改元数据这一特性）。

## [0.1.1] - 2026-06-02

### Fixed

- **`ResolveDedupe` 首写者竞态（M8 C1）。** 首写者分支先执行
  `SELECT ... FOR UPDATE` 再执行裸 `INSERT`，但当去重行尚不存在时
  `FOR UPDATE` 不会锁住任何东西。两个使用相同
  `dedupe_key` 的并发去重者（例如合并工作进程与会话关闭收割器）
  都看到了空索引并都执行了插入；其中一个触发了
  `(tenant_id, dedupe_key)` 唯一约束，暴露出一个原始的 `23505` 错误，
  而两个调用方都未将其当作陈旧处理（导致操作失败）。改为
  一个原子的 `INSERT ... ON CONFLICT DO NOTHING RETURNING`：返回行表示
  本候选者胜出；被抑制的插入表示已存在一个在先/并发的胜出者，
  因此失败者路径照旧运行。实现了
  M8 伞形仓库 §4.3 已经规定的失败者消解。无 schema 或 API 变更。

## [0.1.0] - 2026-05-26

### Added

- 从 SDK 模块拆分出的初始 Postgres 持久存储后端。
- `postgres.Store`，包含：
  - schema 迁移
  - 幂等的 `WriteRecord`
  - OCC 变更路径：`PatchRecord`、`DeleteRecord`、`PinRecord`、`DisableRecord`
  - 租户绑定的 `GetRecord`
- 带有可插拔发布者接口的轮询发件箱（outbox）中继
- `cmd/memory-migrate`

### Dependencies

- `github.com/costa92/llm-agent-memory` 提供由 SDK 拥有的持久抽象
- `github.com/jackc/pgx/v5` 提供 Postgres 连接

### Notes

- 在线 Postgres 测试由 `LLM_AGENT_MEMORY_PG_URL` 环境变量门控。
- 网关 HTTP 与服务组合有意不作为本模块的一部分。
