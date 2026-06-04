[English](./README.md) | [简体中文](./README.zh-CN.md)

# llm-agent-memory-postgres

`github.com/costa92/llm-agent-memory` 的 Postgres 持久存储后端。

## 范围（Scope）

本模块拥有由 SDK 拥有的可插拔记忆数据抽象的第一个具体持久后端。

拥有：

- Postgres schema 与 schema 迁移运行器
- 持久记录持久化
- OCC 变更路径
- 幂等性持久化
- 轮询事务性发件箱（outbox）中继
- `cmd/memory-migrate`

不拥有：

- SDK 核心记忆抽象
- HTTP 网关逻辑
- 认证 / 请求绑定

## 包布局（Package Layout）

- `postgres/`
  - `Store` 构造器与 schema 迁移运行器
  - 记录写入 / 变更 / 读取路径
  - 幂等性重放与冲突处理
  - 轮询发件箱（outbox）中继
- `cmd/memory-migrate/`
  - 轻量 schema 迁移命令

## 与 SDK 的关系（SDK Relationship）

本模块依赖 `github.com/costa92/llm-agent-memory/memory`，并直接实现由 SDK
拥有的存储层抽象，而非重新定义自己的持久记忆契约。

这里使用的持久模型本身就是由 SDK 拥有的后端中立类型，而非本地兼容性别名。

## 当前能力（Current Capabilities）

今天已实现：

- `(*Store).Migrate(ctx)`
- `(*Store).WriteRecord(ctx, in)`
- `(*Store).PatchRecord(ctx, in)`
- `(*Store).DeleteRecord(ctx, in)`
- `(*Store).PinRecord(ctx, in)`
- `(*Store).DisableRecord(ctx, in)`
- `(*Store).GetRecord(ctx, tenantID, memoryID)`
- `NewRelay(...).RunOnce(ctx)`

## 最简用法（Minimal Usage）

```go
pool, err := pgxpool.New(ctx, os.Getenv("LLM_AGENT_MEMORY_PG_URL"))
if err != nil {
	panic(err)
}

store, err := postgres.New(pool, postgres.Config{})
if err != nil {
	panic(err)
}

if err := store.Migrate(ctx); err != nil {
	panic(err)
}
```

schema 迁移命令：

```bash
LLM_AGENT_MEMORY_PG_URL=postgres://... GOWORK=off go run ./cmd/memory-migrate
```

## 测试（Testing）

默认测试不需要一个在线的 Postgres 实例：

```bash
GOWORK=off go test ./... -count=1
```

在线 Postgres 测试由环境变量门控：

```bash
LLM_AGENT_MEMORY_PG_URL=postgres://... GOWORK=off go test ./postgres -count=1
```

当 `LLM_AGENT_MEMORY_PG_URL` 未设置时，在线集成测试会按设计跳过。

## 推迟（Deferred）

仍推迟到后续 gateway/worker 里程碑：

- HTTP API 面
- 从请求上下文进行认证与租户绑定
- 真实的 MQ 后端
- 向量索引集成
- 缓存失效工作进程
- 决策链路持久化与校验指标
