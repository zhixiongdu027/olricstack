# OlricStack

OlricStack is a stack-based distributed KV platform prototype. Each stack
owns an isolated data plane, a watchdog control plane, and an
asynchronous MySQL-backed dirty-data path.

## Current Components

- `cmd/olric-node`: real `github.com/olric-data/olric` data-plane process
  with Watchdog topology subscription and the user-facing RESP entrypoint.
  Durable mutations are published to an shm oplog ring; the sidecar drains
  the ring into MySQL asynchronously. There is no node-local WAL.
- `cmd/olric-sidecar`: per-Pod sidecar process. Consumes the shm oplog
  ring, upserts records into MySQL, and serves a unix-socket gRPC control
  surface (`Notify`, `LoadFromMySQL`, `DrainPartition`,
  `PurgeBelowGeneration`, `Shutdown`).
- `cmd/watchdog`: per-stack controller and gRPC topology service; it
  reconciles its own Olric Deployment/Service and tracks membership by
  heartbeat subscriptions.
- `cmd/operator`: bootstrap Operator that reconciles one `OlricStack` into
  an isolated Watchdog Deployment/Service.
- `internal/stringkv`: front-door string KV business layer. Gates client
  operations on the topology lease, stamps the fence triple on every
  mutation, publishes oplog entries to the shm ring, and forwards miss
  reads through the sidecar.
- `internal/ring`: SPSC mmap-backed byte ring used as the IPC between
  `olric-node` (producer) and `olric-sidecar` (consumer).
- `internal/oplog`: JSON envelope carried by ring payloads.
- `internal/sidecar`: control RPC surface + ring consumer + gorm-backed
  MySQL upsert backend.
- `internal/topology`: in-memory topology state with per-stack isolation
  and monotonic epochs.
- `internal/operator`: bootstrap reconciliation logic for Watchdog
  lifecycle.
- `internal/watchdog`: per-stack Olric reconciliation logic and
  heartbeat-first cluster state owned by Watchdog.
- `internal/workloads`: shared Kubernetes workload builders.
- `internal/store`: wire-format types (`EntryRef`, `EntryRecord`) shared
  by node, sidecar, and MySQL.

## Environment

`olric-node`:

- `STACK_ID`: stack identity injected by the Operator.
- `WATCHDOG_SVC_NAME`: Watchdog gRPC address, for example
  `olric-a-watchdog:8081`.
- `POD_NAME`, `POD_IP`: from the downward API.
- `RING_PATH`: shm oplog ring path shared with the sidecar, default
  `/var/lib/olricstack/shared/oplog.ring`.
- `CONTROL_SOCKET`: unix socket path for the sidecar control RPCs,
  default `/var/lib/olricstack/shared/oplog.sock`.
- `RING_APPEND_BUDGET`: maximum wall time `AfterX` will retry a full
  ring before surfacing an error, default `5s`. If the in-memory
  mutation has already succeeded and the durable publish still fails,
  `olric-node` revokes its serving lease and shuts down instead of
  continuing to serve potentially unlogged memory.
- `RESP_BIND_ADDR`, `RESP_BIND_PORT`: user-facing RESP listener,
  default `0.0.0.0:3321`.
- `RESP_DEFAULT_DMAP`: hidden internal DMap used by the user RESP API,
  default `__default__`.
- `RESP_COMMAND_TIMEOUT`: per-command user RESP timeout, default `5s`.
- `HEARTBEAT_INTERVAL`: Watchdog heartbeat interval, default `10s`.
- `WATCHDOG_RECONNECT_INTERVAL`: retry interval after subscription
  disconnect, default `3s`.
- `OLRIC_BIND_ADDR`, `OLRIC_BIND_PORT`: internal Olric RESP/TCP listener,
  default `0.0.0.0:3320`.
- `OLRIC_MEMBERLIST_BIND_ADDR`, `OLRIC_MEMBERLIST_BIND_PORT`: memberlist
  listener, default `0.0.0.0:3322`.
- `OLRIC_ADVERTISE_ADDR`, `OLRIC_ADVERTISE_PORT`: memberlist advertise
  endpoint, default derived from `POD_IP` and the memberlist port.
- `OLRIC_PEERS`: optional comma-separated initial memberlist peers.
- `OLRIC_REPLICA_COUNT`, `OLRIC_READ_QUORUM`, `OLRIC_WRITE_QUORUM`,
  `OLRIC_MEMBER_COUNT_QUORUM`: Olric quorum settings, default `1`.

`olric-sidecar`:

- `MYSQL_DSN`: required MySQL DSN.
- `RING_PATH`, `CONTROL_SOCKET`: must match the node's settings.
- `RING_CAPACITY_BYTES`: ring file size on first creation, default
  `16777216` (16 MiB).
- `CONSUMER_BATCH_SIZE`: maximum entries drained from ring per cycle,
  default `256`.
- `CONSUMER_IDLE_POLL`: backoff between empty drains, default `200ms`.
- `CONSUMER_FLUSH_TIMEOUT`: deadline for a single MySQL flush call,
  default `30s`.
- `FLUSH_BATCH_SIZE`: gorm `CreateInBatches` batch size, default `256`.
- `MAX_PENDING_RECORDS`: optional cap for sidecar pending records. When
  reached, the sidecar stops advancing the ring head so backpressure
  reaches `olric-node`.
- `MAX_PENDING_BYTES`: optional approximate cap for sidecar pending bytes.
  This is a soft threshold because the ring cannot reject an entry after
  `Pop`; once crossed, the sidecar stops draining more entries.

`watchdog`:

- `WATCHDOG_ADDR`: listen address, default `:8081`.
- `STACK_ID`, `STACK_NAMESPACE`: stack identity (namespace falls back to
  `POD_NAMESPACE` then `default`).
- `SUSPECT_AFTER`, `EXPIRE_AFTER`, `REAP_INTERVAL`: heartbeat timings,
  defaults `20s`/`30s`/`15s`.
- `BOOKWORM_INTERVAL`: full topology snapshot push interval, default
  `10s`.
- `TOPOLOGY_LEASE_TTL`: serving-lease window sent to nodes, default
  `30s`.
- `WATCHDOG_ID`: identity of this Watchdog process, default `HOSTNAME`.
- `WATCHDOG_GENERATION`: under Kubernetes leader-election this is
  allocated from stack-local persistent state; standalone mode falls back
  to process start timestamp.
- `WATCHDOG_LEASE_DURATION`, `WATCHDOG_LEASE_RENEW_DEADLINE`,
  `WATCHDOG_LEASE_RETRY_PERIOD`: leader-election timings, defaults
  `15s`/`10s`/`2s`.
- `WATCHDOG_RECONCILE_INTERVAL`: interval for reconciling this stack's
  Olric Deployment/Service, default `10s`.

## Topology Model

Olric nodes do not expose a topology control port. Each node dials
Watchdog and opens `Watch`, then keeps that stream alive with periodic
`Heartbeat` messages. Watchdog pushes `TopologyEnvelope` updates only on
the existing node-owned stream.

The durable data-plane business API is the OlricStack string KV proxy
layer running in the `olric-node` process. The user-facing RESP API
listens on port `3321` and supports `GET`, `SET`, `DEL`, `EXPIRE`,
`PING`, and `HELLO 3`. These commands are routed through
`stringkv.Service` into a hidden default DMap, so clients do not use
Olric native `DM.*` commands. The native Olric RESP/TCP server on port
`3320` remains an internal node-to-node endpoint. Memberlist gossip uses
port `3322`.

This keeps the control plane decoupled from node network reachability
and gives Watchdog a direct liveness signal. If heartbeats stop for the
configured TTL, Watchdog prunes the node and advances the topology
epoch.

The Watchdog also runs a bookworm loop: it periodically pushes the
complete structured member topology with the current epoch even when
membership has not changed.

Watchdog uses Kubernetes client-go leader election for primary/standby
operation. When `STACK_ID` is set, a Watchdog process does not start
topology gRPC or stack reconciliation until leader-election grants
leadership. The Operator creates two Watchdog replicas by default.

## Persistence Contract

Durable state lives in MySQL. The asynchronous path is:

```
node                          shm ring                    sidecar                 MySQL
─────                         ─────────                   ───────                 ─────
RESP SET → DurableHook
        → fence stamp (G, E)
        → fragment lock
        → in-memory mutation
        → fence stamp S
        → ring.Append      ──→
                                                          consumer.Pop
                                                          → batched UPSERT      ──→
node returns success ◀── ring.Append ok (this is the ack point)
```

Ack semantics: **`ring.Append` returning success is the durable
acknowledgement returned to the RESP client.** The sidecar drains into
MySQL on its own schedule. Pod reschedule loses any unconsumed ring
entries; this is accepted by design.

If a DMap mutation succeeds but the subsequent ring append fails, the
node treats that as a fatal data-plane fault: it revokes its local
serving lease and shuts down. This fail-stop rule prevents a node that
may contain unlogged memory from continuing to serve traffic. The
longer-term ring-as-WAL transaction design is tracked in
[Ring-as-WAL Execution Plan](docs/ring-as-wal-execution-plan.md).

Sidecar pending memory is bounded when `MAX_PENDING_RECORDS` or
`MAX_PENDING_BYTES` is configured. Under MySQL pressure, the sidecar
stops draining the ring instead of buffering indefinitely; the ring then
fills and backpressures new writes before they mutate DMap memory.

The fence comparator at MySQL upsert time is strict lex
`(generation, epoch, owner_seq, writer_id)`. `writer_id` is a fresh
per-Pod-instance UUID, so the in-memory `MemoryFenceSequencer` is safe
to restart `S` at `1` after every Pod boot — the tuple stays unique.

See [Sidecar Oplog Cutover](docs/sidecar-oplog-cutover.md) for the
authoritative description of this architecture.

## Kubernetes

The bootstrap Operator creates only the Watchdog Deployment/Service for
each `OlricStack`. Each Watchdog then reads its own `OlricStack` and
reconciles that stack's Olric Deployment (two containers: `olric-node`
and `olric-sidecar`) and its RESP Service. The Pod has one shared volume
backed by tmpfs (`emptyDir{ medium: Memory }`) for the oplog ring file
and the unix socket; there are no PVCs.

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
go build ./cmd/olric-node ./cmd/olric-sidecar ./cmd/watchdog ./cmd/operator
```

GitHub Actions CI is the authoritative full validation path for this
project. It runs unit tests, race tests, third-party fork tests, host
e2e, kind e2e, Docker validation, security checks, and nightly deep
suites. Local commands are useful for fast iteration, but CI is the
release gate.

Run the local gRPC e2e topology test:

```bash
make e2e
```

Run the full local verification suite:

```bash
make verify
```

Generate topology protobuf bindings after changing
`api/topology/v1/topology.proto`:

```bash
PATH="$(go env GOPATH)/bin:$PATH" protoc \
  --go_out=. --go_opt=paths=source_relative \
  --go-grpc_out=. --go-grpc_opt=paths=source_relative \
  api/topology/v1/topology.proto
```
