# Sidecar Oplog Cutover

This document replaces the old node-owned WAL/MySQL design.

## Boundary

- `olric-node` owns the Olric data plane and the user-facing RESP entrypoint.
- `olric-sidecar` consumes a shared-memory oplog ring and asynchronously
  upserts dirty entries into MySQL. It also serves a small control RPC
  surface over a unix socket.
- The two containers run in the same Pod and share a single `emptyDir`
  volume backed by tmpfs (`medium: Memory`). The shm ring file and the
  unix socket both live there.

## Channels

Only two channels exist between `olric-node` and `olric-sidecar`:

1. **shm ring** (`/var/lib/olricstack/shared/oplog.ring`)
   - Producer: `olric-node` (`internal/ring.Producer`).
   - Consumer: `olric-sidecar` (`internal/ring.Consumer`).
   - Single-producer, single-consumer. Append serializes one Olric
     mutation at a time per node, which is the only producer.
   - **Ack semantics**: `Append` returning success IS the durable
     acknowledgement. `olric-node` returns success to its client at that
     point. If the Pod is rescheduled, any unconsumed entries are lost.
     This is acceptable by design — durability is bounded by Pod
     lifetime.
2. **unix-socket gRPC** (`/var/lib/olricstack/shared/oplog.sock`)
   - `Notify(pending)` — wake the consumer loop on demand.
   - `LoadFromMySQL(ref)` — miss-path read; returns pending entry first,
     then falls through to MySQL.
   - `DrainPartition(dmap, partitionId, partitionCount)` — synchronous
     flush of a partition during fragment handoff.
   - `PurgeBelowGeneration(minGeneration)` — drop pending entries and
     terminal rows below the watchdog generation.
   - `Shutdown` — flush pending, then return.

## Wire format

Ring payloads are JSON-encoded `internal/oplog.Entry`:

```
{ v, op, dmap, key, hkey, entry, ttl, ts, g, e, s, w, u }
```

`(g, e, s, w)` is the fence triple plus per-pod-instance writer id used
by the upsert comparator (`G > E > S > W` strict lex). `u` is the wall
time the producer stamped, used only for the MySQL `updated_at` column.

## Identity & sequencing

- `writer_id` is a per-pod-instance UUID — stable for the lifetime of the
  Pod, fresh after every restart. This is what makes the in-memory
  `MemoryFenceSequencer` safe even though `S` resets to 1 after restart:
  the tuple `(G, E, writer_id, S)` is unique.
- `OwnerSequence` is no longer persisted. There is no bbolt file, no
  fsync per write.

## Workload

- `Deployment`, not `StatefulSet`. There is no PVC and no per-pod stable
  hostname requirement — `writer_id` carries identity.
- Two containers per Pod:
  - `olric-node`
  - `olric-sidecar`
- One Volume: `olric-shared`, `emptyDir{ medium: Memory }`, mounted at
  `/var/lib/olricstack/shared` in both containers.

## Recovery

There is nothing on disk to recover. After Pod reschedule:

- shm ring is fresh.
- `MemoryFenceSequencer` starts at S=1 under the writer's new
  per-instance id.
- Stale rows in MySQL stamped by an older `(generation, epoch, writer)`
  cannot win the upsert comparator against the new write.

## Tests

- `internal/ring`: round-trip, FIFO order, wrap-around, full back-pressure.
- `internal/oplog`: encode/decode + version & op validation.
- `internal/sidecar`: consumer drain, pending priority on read, partition
  drain, purge.
- `internal/stringkv`: hook stamps fence in BeforeX, publishes to ring in
  AfterX(success), no-ops on AfterX(error), retries on transient
  ring-full, surfaces budget-exceeded errors.
- `internal/watchdog`: Deployment-shaped reconcile.
