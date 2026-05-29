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
   - **Post-mutation append failure**: if Olric memory has already been
     mutated but the append still fails, `olric-node` treats it as a
     fatal data-plane fault. It revokes its local serving lease and shuts
     down rather than continuing to serve memory that may not have an
     oplog record.
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

Within a running Pod, the sidecar may stop advancing the ring head when
its pending buffer reaches `MAX_PENDING_RECORDS` or `MAX_PENDING_BYTES`.
This is the intended backpressure path: MySQL pressure fills sidecar
pending, sidecar stops draining the ring, ring capacity falls, and node
writes are rejected before mutating DMap memory.

The current implementation still uses a post-mutation append. The
formal next-step design for moving to prepared/committed ring slots is
tracked in [Ring-as-WAL Execution Plan](ring-as-wal-execution-plan.md).

## Tests

- `internal/ring`: round-trip, FIFO order, wrap-around, full back-pressure.
- `internal/oplog`: encode/decode + version & op validation.
- `internal/sidecar`: consumer drain, pending priority on read, partition
  drain, purge, bounded pending backpressure.
- `internal/stringkv`: hook stamps fence in BeforeX, publishes to ring in
  AfterX(success), no-ops on AfterX(error), retries on transient
  ring-full, surfaces budget-exceeded errors, and triggers fail-stop on
  post-mutation publish failure.
- `internal/watchdog`: Deployment-shaped reconcile.
