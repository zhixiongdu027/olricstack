# OlricStack

OlricStack is a stack-based distributed KV platform prototype. Each stack is intended to own an isolated data plane, a watchdog control plane, and a MySQL-backed write-behind persistence path.

## Current Components

- `cmd/olric-node`: real `github.com/olric-data/olric` data-plane process with Watchdog topology subscription, local WAL/MySQL bootstrap, and an in-process durable string KV service boundary. The native Olric RESP/TCP port remains a cache/development endpoint until the durable proxy API is bound.
- `cmd/watchdog`: per-stack controller and gRPC topology service; it reconciles its own Olric StatefulSet/headless Service and tracks membership by heartbeat subscriptions.
- `cmd/operator`: bootstrap Operator that reconciles one `OlricStack` into an isolated Watchdog Deployment/Service.
- `internal/stringkv`: front-door string KV business layer. It gates client operations on the topology lease, writes WAL before applying Olric mutations, records tombstones/TTL metadata, and performs MySQL read-through refill on Olric misses.
- `internal/store`: synchronous MySQL `Load` plus WAL-backed asynchronous coalesced `Store` flushes using version-fenced upserts.
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
- `FLUSH_BACKOFF`: optional retry backoff after a failed MySQL flush, default `1s`.
- `FLUSH_BATCH_SIZE`: optional batch size, default `256`.
- `WAL_PATH`: local durable dirty-write log path, default `/var/lib/olricstack/cache.wal` in `olric-node`.
- `RESP_BIND_ADDR`: user-facing OlricStack RESP bind address, default `0.0.0.0`.
- `RESP_BIND_PORT`: user-facing OlricStack RESP bind port, default `3321`.
- `RESP_DEFAULT_DMAP`: hidden internal DMap used by the user RESP API, default `__default__`.
- `RESP_COMMAND_TIMEOUT`: per-command user RESP timeout, default `5s`.
- `HEARTBEAT_INTERVAL`: optional Watchdog heartbeat interval, default `10s`.
- `WATCHDOG_RECONNECT_INTERVAL`: optional retry interval after subscription disconnect, default `3s`.
- `OLRIC_BIND_ADDR`: internal Olric RESP/TCP bind address, default `0.0.0.0`.
- `OLRIC_BIND_PORT`: internal Olric RESP/TCP bind port, default `3320`.
- `OLRIC_MEMBERLIST_BIND_ADDR`: memberlist bind address, default `OLRIC_BIND_ADDR`.
- `OLRIC_MEMBERLIST_BIND_PORT`: memberlist bind port, default `3322`.
- `OLRIC_ADVERTISE_ADDR`: memberlist advertise address, default `POD_IP`.
- `OLRIC_ADVERTISE_PORT`: memberlist advertise port, default `OLRIC_MEMBERLIST_BIND_PORT`.
- `OLRIC_PEERS`: optional comma-separated initial memberlist peers in `host:port` form.
- `OLRIC_REPLICA_COUNT`: optional Olric replica count, default `1`.
- `OLRIC_READ_QUORUM`: optional Olric read quorum, default `1`.
- `OLRIC_WRITE_QUORUM`: optional Olric write quorum, default `1`.
- `OLRIC_MEMBER_COUNT_QUORUM`: optional Olric member-count quorum, default `1`.

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
- `WATCHDOG_GENERATION`: in Kubernetes leader-election mode this is allocated from stack-local persistent state; standalone mode falls back to process start timestamp.
- `WATCHDOG_LEASE_DURATION`: Kubernetes Lease duration for primary election, default `15s`.
- `WATCHDOG_LEASE_RENEW_DEADLINE`: official leader-election renew deadline, default `10s`.
- `WATCHDOG_LEASE_RETRY_PERIOD`: official leader-election retry period, default `2s`.
- `WATCHDOG_RECONCILE_INTERVAL`: interval for reconciling this stack's Olric StatefulSet/headless Service, default `10s`.

## Topology Model

Olric nodes do not expose a topology control port. Each node dials Watchdog and opens `Watch`, then keeps that stream alive with periodic `Heartbeat` messages. Watchdog pushes `TopologyEnvelope` updates only on the existing node-owned stream.

The durable data-plane business API is the OlricStack string KV proxy layer running in the same process as Olric. The user-facing RESP API listens on port `3321` and supports `GET`, `SET`, `DEL`, `EXPIRE`, `PING`, and `HELLO 3`. These commands are routed through `stringkv.Service` into a hidden default DMap, so clients do not use Olric native `DM.*` commands. The native Olric RESP/TCP server on port `3320` remains an internal node-to-node endpoint. Memberlist gossip uses port `3322`.

This keeps the control plane decoupled from node network reachability and gives Watchdog a direct liveness signal. If heartbeats stop for the configured TTL, Watchdog prunes the node and advances the topology epoch.

The Watchdog also runs a bookworm loop: it periodically pushes the complete structured member topology with the current epoch even when membership has not changed. Nodes can use the epoch and `valid_until` to distinguish unchanged anti-entropy refreshes from real topology transitions and stale leases.

Watchdog is designed for primary/standby operation using Kubernetes client-go leader election. When `STACK_ID` is set, a Watchdog process does not start topology gRPC, topology reaping/bookworm loops, or stack reconciliation until the official leader-election callback grants leadership. When leadership is lost, the Watchdog immediately reports not ready, stops the gRPC server to break existing node streams, and exits so Kubernetes restarts the Pod instead of keeping a demoted standby in place. The bootstrap Operator creates two Watchdog replicas by default so they can compete for the Lease.

The Watchdog Deployment exposes gRPC health as readiness and reports `SERVING` only while primary. Kubernetes Services therefore route node topology streams to the current primary instead of load-balancing equally across primary and standby replicas. The Deployment rolling update allows one unavailable replica because standby Pods are intentionally not ready.

## Watchdog State Model

Watchdog keeps one state machine per stack. Node heartbeats are the primary source of membership: a node is not added to topology until it reports through the subscription stream. Kubernetes Pod observations are secondary: they never create membership by themselves, but they can enrich node state and exclude a heartbeating node if Kubernetes reports the pod as terminal (`Failed` or `Succeeded`).

The same state machine feeds topology pushes and is updated by the Watchdog reconciler after it lists this stack's Pods. This links reconciliation, node health, and topology without making Kubernetes Pod state the authority for cluster membership.

See [Watchdog State And Protocol Review](docs/watchdog-state-protocol-review.md) for production hardening concerns, extreme scenario walkthroughs, and protocol v2 recommendations.

The primary Watchdog persists the latest topology epoch into a stack-local ConfigMap and reloads it after failover. This prevents topology epoch rollback across primary changes.

The same stack-local ConfigMap also allocates `watchdog_generation` before a primary starts serving. This keeps primary fencing monotonic across Pod restarts and avoids relying on local node clocks.

## Persistence Contract

The target durable product contract is a MySQL-backed string KV, not arbitrary native Olric DMap persistence. Olric remains responsible for routing, ownership, backup replication, read-repair, migration, and in-memory eviction. MySQL is the durable source of truth only for the string KV business API.

The design supports three working modes:

- memory only;
- evictable cache;
- durable read-through string KV, where Olric may evict entries internally but business `GET` can refill from MySQL on a current-primary miss.

MySQL read-through must not be wired into the generic Olric storage engine `Get` path. Generic storage reads are also used by internal replica lookup, previous-owner lookup, read-repair, migration, and maintenance paths. Durable MySQL refill belongs in the current primary owner's business `GET` miss path, with serving-lease checks and operation-origin metadata.

The preferred implementation keeps Olric as a black-box in-process engine behind the string KV proxy layer. Olric source changes are optional escape hatches for future owner-context hooks, not the primary persistence mechanism.

See [Durable String KV Design](docs/durable-string-kv-design.md) for the authoritative persistence architecture, proxy-layer persistence boundary, operation-origin matrix, WAL model, and failure tests.

Watchdog reconciles Pod observations by Pod name first and only falls back to Pod IP for nodes without a known Pod name. This avoids marking an old member terminal when Kubernetes rapidly reuses an IP for a different Pod. If a primary Watchdog loses its Kubernetes Lease, it sends a standby topology envelope to active streams, marks itself not ready, and terminates the process so nodes reconnect to the newly promoted primary.

## Kubernetes

The bootstrap Operator creates only the Watchdog Deployment/Service for each `OlricStack`. Each Watchdog then reads its own `OlricStack` and reconciles that stack's Olric StatefulSet and headless Service. This keeps high-frequency Olric control local to the stack and reduces pressure on the global Operator.

The Olric headless Service is the StatefulSet governing Service and exposes
only the user RESP port `3321`. Watchdog still distributes Pod IP topology for
Olric internal communication; clients should use the RESP API and must not call
Olric native `DM.*` commands directly.

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
