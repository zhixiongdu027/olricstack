# OlricStack

OlricStack is a stack-based distributed KV platform prototype. Each stack is intended to own an isolated data plane, a watchdog control plane, and a MySQL-backed write-behind persistence path.

## Current Components

- `cmd/olric-node`: data-plane bootstrap with a MySQL-backed cache store and a Watchdog topology subscription.
- `cmd/watchdog`: per-stack controller and gRPC topology service; it reconciles its own Olric StatefulSet/headless Service and tracks membership by heartbeat subscriptions.
- `cmd/operator`: bootstrap Operator that reconciles one `OlricStack` into an isolated Watchdog Deployment/Service.
- `internal/store`: synchronous MySQL `Load` plus asynchronous coalesced `Store` flushes using `INSERT ... ON DUPLICATE KEY UPDATE`.
- `internal/topology`: in-memory topology state with per-stack isolation and monotonic epochs.
- `internal/operator`: bootstrap reconciliation logic for Watchdog lifecycle.
- `internal/watchdog`: per-stack Olric reconciliation logic and heartbeat-first cluster state owned by Watchdog.
- `internal/workloads`: shared Kubernetes workload builders for Watchdog and Olric resources.

## Environment

`olric-node`:

- `MYSQL_DSN`: required MySQL DSN.
- `STACK_ID`: stack identity injected by the future Operator.
- `WATCHDOG_SVC_NAME`: Watchdog gRPC address, for example `olric-a-watchdog:8081`.
- `POD_NAME`: current pod name.
- `POD_IP`: current pod IP.
- `DIRTY_QUEUE_SIZE`: optional queue capacity, default `1024`.
- `FLUSH_INTERVAL`: optional Go duration, default `1s`.
- `FLUSH_BATCH_SIZE`: optional batch size, default `256`.
- `HEARTBEAT_INTERVAL`: optional Watchdog heartbeat interval, default `10s`.
- `WATCHDOG_RECONNECT_INTERVAL`: optional retry interval after subscription disconnect, default `3s`.

`watchdog`:

- `WATCHDOG_ADDR`: listen address, default `:8081`.
- `STACK_ID`: current stack name; when set, Watchdog enables the per-stack Olric controller.
- `STACK_NAMESPACE`: current stack namespace, default falls back to `POD_NAMESPACE` then `default`.
- `SUSPECT_AFTER`: missing-heartbeat window before a member becomes suspect, default `20s`.
- `EXPIRE_AFTER`: missing-heartbeat window before a member is removed, default `30s`.
- `REAP_INTERVAL`: interval for pruning expired nodes, default `15s`.
- `BOOKWORM_INTERVAL`: interval for pushing full versioned topology snapshots even when membership has not changed, default `10s`.
- `TOPOLOGY_LEASE_TTL`: validity window sent to nodes in each topology envelope, default `30s`.
- `WATCHDOG_ID`: identity of this Watchdog process, default `HOSTNAME`.
- `WATCHDOG_ROLE`: static role, `primary` or `standby`; default `primary`.
- `WATCHDOG_GENERATION`: monotonic generation for this Watchdog process, default process start timestamp.
- `WATCHDOG_LEASE_DURATION`: Kubernetes Lease duration for primary election, default `15s`.
- `WATCHDOG_LEASE_RENEW_INTERVAL`: primary Lease renewal interval, default `5s`.
- `WATCHDOG_LEASE_ACQUIRE_INTERVAL`: standby acquisition retry interval, default `5s`.
- `WATCHDOG_RECONCILE_INTERVAL`: interval for reconciling this stack's Olric StatefulSet/headless Service, default `10s`.

## Topology Model

Olric nodes do not expose a topology control port. Each node dials Watchdog and opens `Watch`, then keeps that stream alive with periodic `Heartbeat` messages. Watchdog pushes `TopologyEnvelope` updates only on the existing node-owned stream.

This keeps the control plane decoupled from node network reachability and gives Watchdog a direct liveness signal. If heartbeats stop for the configured TTL, Watchdog prunes the node and advances the topology epoch.

The Watchdog also runs a bookworm loop: it periodically pushes the complete structured member topology with the current epoch even when membership has not changed. Nodes can use the epoch and `valid_until` to distinguish unchanged anti-entropy refreshes from real topology transitions and stale leases.

Watchdog is designed for primary/standby operation. Only the primary accepts node membership and pushes topology. Standby instances expose their role and generation but do not own topology. When `STACK_ID` is set, Watchdog starts as standby and must win the per-stack Kubernetes Lease before becoming primary; `WATCHDOG_ROLE` is only a local fallback when election is not enabled. The bootstrap Operator creates two Watchdog replicas by default so they can compete for the Lease.

## Watchdog State Model

Watchdog keeps one state machine per stack. Node heartbeats are the primary source of membership: a node is not added to topology until it reports through the subscription stream. Kubernetes Pod observations are secondary: they never create membership by themselves, but they can enrich node state and exclude a heartbeating node if Kubernetes reports the pod as terminal (`Failed` or `Succeeded`).

The same state machine feeds topology pushes and is updated by the Watchdog reconciler after it lists this stack's Pods. This links reconciliation, node health, and topology without making Kubernetes Pod state the authority for cluster membership.

See [Watchdog State And Protocol Review](docs/watchdog-state-protocol-review.md) for production hardening concerns, extreme scenario walkthroughs, and protocol v2 recommendations.

The primary Watchdog persists the latest topology epoch into a stack-local ConfigMap and reloads it after failover. This prevents topology epoch rollback across primary changes.

## Persistence Caveat

The current MySQL write-behind path retries failed flushes in memory, but it is not yet durable across node process death. Production durability requires a local WAL or synchronous durable-write mode before acknowledging writes.

## Kubernetes

The bootstrap Operator creates only the Watchdog Deployment/Service for each `OlricStack`. Each Watchdog then reads its own `OlricStack` and reconciles that stack's Olric StatefulSet and headless Service. This keeps high-frequency Olric control local to the stack and reduces pressure on the global Operator.

Install the CRD and RBAC:

```bash
kubectl apply -f config/crd/olric.io_olricstacks.yaml
kubectl apply -f config/rbac/operator.yaml
```

Create a DSN secret and a sample stack:

```bash
kubectl create secret generic demo-mysql --from-literal=dsn='user:pass@tcp(mysql:3306)/olric?parseTime=true'
kubectl apply -f config/samples/olricstack.yaml
```

## Development

```bash
go test ./...
go build ./cmd/olric-node ./cmd/watchdog ./cmd/operator
```

Run the local gRPC e2e topology test:

```bash
make e2e
```

Run the full local verification suite:

```bash
make verify
```

Generate topology protobuf bindings after changing `api/topology/v1/topology.proto`:

```bash
PATH="$(go env GOPATH)/bin:$PATH" protoc \
  --go_out=. --go_opt=paths=source_relative \
  --go-grpc_out=. --go-grpc_opt=paths=source_relative \
  api/topology/v1/topology.proto
```
