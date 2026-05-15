
---

# AGENT.md: OlricStack Distributed Storage Platform

## 1. 愿景与目标
构建一套云原生、高性能、带 MySQL 持久化兜底的自动化分布式 KV 存储平台。
*   **架构范式**: “Stack-based Operator” (一个 CRD 对象 = 一套完整的自治集群)。
*   **核心特性**:
    *   **透明回源**: 缓存 Miss 自动触发 MySQL `Load`。
    *   **异步落盘**: 脏写先进入本地持久 WAL，再通过合并与异步队列 (`Write-Behind`) 批量落入 MySQL。
    *   **高可用控制**: 通过独立 Watchdog 组件管理集群拓扑，解耦 K8s API 压力。

## 2. 核心架构模型 (The "Stack" Concept)
每一个 `OlricStack` 实例是一个完全隔离的自治单元，包含：
*   **控制面 (Control Plane)**: 专属的 **Watchdog Pod**，负责监听 CRD 状态，管理集群拓扑并推送给业务节点。
*   **数据面 (Data Plane)**: 一组 **Olric StatefulSet Pods**，运行嵌入式 `olric` 引擎，处理读写流量与 MySQL 刷盘。
*   **隔离策略**: 通过标签 `olric.io/stack-id: <metadata.name>` 实现所有资源的绑定与隔离。

## 3. 技术栈建议
*   **语言**: Go 1.22+
*   **分布式 KV**: `github.com/olric-data/olric` (底层路由与分片)
*   **Operator**: `controller-runtime` (K8s 调谐逻辑)
*   **MySQL**: `GORM` (高性能异步落盘)
*   **RPC**: `gRPC` (Watchdog -> Olric 的拓扑指令分发)

## 4. 开发核心职责分工

### A. Operator (调谐与生命周期)
*   **Reconciler 职责**:
    *   监控 `OlricStack` CRD 的增删改。
    *   启动并维护该 Stack 专属 Watchdog 的 Deployment/Service。
    *   注入环境变量 `STACK_ID` 与 `STACK_NAMESPACE` 到 Watchdog Pod。
*   **多集群隔离**: 确保 Watchdog 与 Olric 的 Label Selector 完全绑定在该实例的 ID 上。

### B. Watchdog (集群大脑)
*   **本组控制器**: 读取自身 `STACK_ID` 对应的 `OlricStack`，根据 Spec 调谐本组 Olric StatefulSet 与 headless Service。
*   **状态维护**: 以 Olric 节点心跳与上报信息为主事实源；K8s Pod 列表与状态作为辅助观察，只用于补充和排除终态 Pod，不单独创建拓扑成员。
*   **服务发现**: 暴露 gRPC 接口供 Olric Pod 注册及获取拓扑。
*   **鲁棒性**: Olric 节点主动订阅 Watchdog 并周期心跳；Watchdog 仅在已有订阅流上推送拓扑，避免反向连接节点。
*   **Bookworm 校验**: Watchdog 周期性推送完整拓扑快照，即使成员未变化也携带当前 `epoch`，用于反熵同步和版本确认。

### C. Olric Node (高性能载体)
*   **持久化层 (`CacheStore` 接口)**:
    *   `Load`: 同步回源查询 MySQL。
    *   `Store`: ack 前先写入本地持久 WAL，再通知后台 flusher。
*   **异步落盘 (Write-Coalescing)**: 
    *   由 WAL flusher 定时合并数据，通过带版本裁决的 upsert 写入 MySQL。
*   **拓扑更新**: 主动建立到 Watchdog 的 gRPC 订阅流，发送心跳并接收拓扑更新；收到新 IP 列表后执行 `olric.Memberlist().Join(nodes)`。

## 5. 核心协议定义 (gRPC)
```proto
service TopologyControl {
  rpc GetTopology(TopologyQuery) returns (TopologyEnvelope);
  rpc Watch(stream Heartbeat) returns (stream TopologyEnvelope);
}

message Heartbeat {
  string stack_id = 1;
  string node_id = 2;
  string pod_name = 3;
  string pod_ip = 4;
  int64 incarnation = 5;
  int64 observed_epoch = 6;
  NodeState state = 7;
  string protocol_version = 8;
}

message TopologyEnvelope {
  string stack_id = 1;
  repeated Member members = 2;
  int64 epoch = 3;
  TopologyReason reason = 4;
  int64 valid_until_unix_ms = 5;
  string watchdog_id = 6;
  int64 watchdog_generation = 7;
  WatchdogRole watchdog_role = 8;
}
```

## 6. 开发路径与优先级
1.  **Phase 1: 核心存储引擎 (The Engine)**
    *   实现 `olric.CacheStore` 接口，打通内存与 MySQL 的双向链路。
    *   验证 WAL flusher 批量合并写入 MySQL 的稳定性。
2.  **Phase 2: 动态集群发现 (The Join)**
    *   实现 Olric 节点到 Watchdog 的订阅流与心跳保活，验证动态 `Join` 对集群稳定性的影响。
3.  **Phase 3: 云原生编排 (The Operator)**
    *   编写 CRD 与 Controller，实现 `OlricStack` 的自动创建与扩缩容。
    *   引入 Watchdog 实现集群成员的动态实时感知与同步。

## 7. 架构指导原则
*   **最终一致性优先**: 集群扩容时的视图延迟由 Olric 的 Gossip 协议和 Watchdog 的巡检机制兜底。
*   **压力熔断**: 当 WAL dirty 集合超过阈值，直接触发 `503`，保护 MySQL 不被击穿。
*   **零耦合**: Olric Pod 不应该感知 Kubernetes 的存在，所有外部信息均由 Watchdog 经 gRPC 下发。

---
