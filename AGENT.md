
---

# AGENT.md: OlricStack Distributed Storage Platform

## 1. 愿景与目标
构建一套云原生、高性能、带 MySQL 异步兜底的自动化分布式 KV 存储平台。
*   **架构范式**："Stack-based Operator"（一个 CRD 对象 = 一套完整的自治集群）。
*   **核心特性**:
    *   **透明回源**：缓存 Miss 经 sidecar 直读 MySQL。
    *   **异步落盘**：脏写经 shm oplog ring 投递到 sidecar，sidecar 合并后异步 upsert MySQL。
    *   **高可用控制**：通过独立 Watchdog 组件管理集群拓扑，解耦 K8s API 压力。

## 2. 核心架构模型 (The "Stack" Concept)
每一个 `OlricStack` 实例是一个完全隔离的自治单元，包含：
*   **控制面 (Control Plane)**：专属的 **Watchdog Pod**，负责监听 CRD 状态，管理集群拓扑并推送给业务节点。
*   **数据面 (Data Plane)**：一组 **Olric Deployment Pods**，每个 Pod 内含 `olric-node` + `olric-sidecar` 两个容器，共享一个 `emptyDir{ medium: Memory }` 卷用于 oplog ring 与 unix socket。**没有 PVC**：缓存即易失，durability 由 sidecar→MySQL 的异步路径保证。
*   **隔离策略**：通过标签 `olric.io/stack-id: <metadata.name>` 实现所有资源的绑定与隔离。

## 3. 容器内进程边界
*   **olric-node**：Olric data plane + 用户 RESP 入口。每次写：stamp fence (G,E) → fragment lock → in-memory mutation → stamp owner_seq → `ring.Append`（成功即为 ack）→ Notify sidecar。Miss 路径走 unix-socket 调用 sidecar `LoadFromMySQL`。
*   **olric-sidecar**：消费 ring → 合批 → MySQL upsert（带 fence-aware OnConflict）。同时承载 5 条 unix-socket 控制 RPC：`Notify`、`LoadFromMySQL`、`DrainPartition`、`PurgeBelowGeneration`、`Shutdown`。
*   **节点身份**：`writer_id` 是每次 Pod 启动新生成的 UUID，因此 `MemoryFenceSequencer` 重启后从 `S=1` 开始仍然安全——`(generation, epoch, owner_seq, writer_id)` 始终唯一。

## 4. 技术栈
*   **语言**: Go 1.26+
*   **分布式 KV**: `github.com/olric-data/olric`（底层路由与分片，patched fork）
*   **Operator**: `controller-runtime`
*   **MySQL**: `gorm.io/gorm`（仅在 sidecar 进程使用）
*   **IPC**: 自实现 SPSC mmap ring（`internal/ring`）+ unix-socket gRPC（JSON codec）

## 5. 开发核心职责分工

### A. Operator（调谐与生命周期）
*   监控 `OlricStack` CRD 的增删改。
*   启动并维护该 Stack 专属 Watchdog 的 Deployment/Service。
*   注入 `STACK_ID` 与 `STACK_NAMESPACE` 到 Watchdog Pod。
*   不直接创建 Olric Deployment——那是 Watchdog 的职责。

### B. Watchdog（集群大脑）
*   读取自身 `STACK_ID` 对应的 `OlricStack`，调谐本组 Olric **Deployment** 与 RESP Service。
*   状态维护：以 Olric 节点心跳为主事实源；K8s Pod 列表作为辅助观察。
*   服务发现：暴露 gRPC 接口供 Olric Pod 订阅心跳与拓扑。
*   Bookworm：周期性推送完整拓扑快照携带当前 `epoch` 用于反熵。

### C. Olric Node（数据面）
*   `DurableHook`：BeforeX 只 stamp fence；AfterX(success) 才 stamp owner_seq + `ring.Append` + Notify。AfterX(error) 不发布。
*   `LoadOnMiss`：unix-socket 调用 sidecar，sidecar 先看 pending、再回 MySQL。
*   `DrainForHandoff`：unix-socket 调用 sidecar `DrainPartition`，sidecar 同步把该 partition 的 pending 落入 MySQL。
*   拓扑：主动建立到 Watchdog 的 gRPC 订阅流，发送心跳并接收 `TopologyEnvelope`。

### D. Olric Sidecar（持久化）
*   单 consumer 循环：`ring.Pop` → 合并到 in-memory pending → `gorm CreateInBatches` upsert。
*   Fence comparator：strict lex `(generation, epoch, owner_seq, writer_id)`，旧 owner 永远输给更高 fence。
*   Pending map 同时是 read-your-write 的保证：在 ring 已 Append 但 MySQL 还没 upsert 的窗口里，`LoadFromMySQL` 会优先返回 pending。

## 6. 核心协议定义 (gRPC)

拓扑控制（Watchdog ↔ Node，TCP gRPC）：
```proto
service TopologyControl {
  rpc GetTopology(TopologyQuery) returns (TopologyEnvelope);
  rpc Watch(stream Heartbeat) returns (stream TopologyEnvelope);
}
```

Sidecar 控制（Node ↔ Sidecar，unix-socket gRPC，JSON codec）：
```
service OplogControl {
  rpc Notify(NotifyRequest) returns (Empty);
  rpc LoadFromMySQL(LoadRequest) returns (LoadResponse);
  rpc DrainPartition(DrainPartitionRequest) returns (Empty);
  rpc PurgeBelowGeneration(PurgeRequest) returns (PurgeResponse);
  rpc Shutdown(Empty) returns (Empty);
}
```

数据面 IPC（Node → Sidecar，shm 单向）：JSON-encoded `internal/oplog.Entry`，承载 op/dmap/key/hkey/encoded entry/ttl/ts/g/e/s/writer_id/updated_at。

## 7. 架构指导原则
*   **最终一致性优先**：集群扩容时的视图延迟由 Olric 的 Gossip 协议和 Watchdog 的巡检机制兜底。
*   **Ack 边界即 ring**：客户端"写成功"= `ring.Append` 返回成功。Pod 重调度丢失 ring 中未消费数据是被显式接受的折衷。
*   **零耦合**：Olric Pod 不应该感知 Kubernetes 的存在，所有外部信息均由 Watchdog 经 gRPC 下发；durability 路径只经 sidecar，不经 K8s API。
*   **没有 PVC**：缓存与 ring 都不跨 Pod 生命期。durability 唯一的家是 MySQL。

详见 [Sidecar Oplog Cutover](docs/sidecar-oplog-cutover.md)。

---
