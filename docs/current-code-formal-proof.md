# Current Code Formal Proof Sketch — Watchdog, Olric Node, WAL, MySQL

**Date:** 2026-05-18
**Basis:** current worktree after the partition-scoped handoff drain,
Watchdog demotion, and query-epoch persistence fixes.
**Scope:** Watchdog topology service, Olric node subscription and lease gate,
forked Olric DMap durable hook, local bbolt WAL, MySQL fenced flush.

This document treats older audit documents as historical indices only. The
claims below are derived from the current code paths.

## 0. Preconditions And Boundary

The durable string-kv service is constructed in `cmd/olric-node/main.go`, but
no external API listener is bound yet. Therefore ingress claims are conditional:
they hold for callers that enter through `stringkv.Service.Get/Set/Delete/
Expire`, not for arbitrary future transports until those transports are wired to
this service.

Production Kubernetes mode is the safety target. Standalone watchdog mode uses
wall-clock generations and is not covered by the monotonic-generation proof.

## 1. State Variables

Persistent state:

- `CM.generation`: ConfigMap `watchdogGeneration`, advanced by
  `ConfigMapEpochStore.NextGeneration`.
- `CM.epoch`: ConfigMap `topologyEpoch`, advanced by
  `ConfigMapEpochStore.SaveEpoch`.
- `SEQ = (G, E, reservedHigh)`: owner-sequence bbolt file.
- `WAL[ref] = EntryRecord`: bbolt dirty bucket, with `WALState` in
  `{prepared, committed, local_refill}`.
- `MYSQL[(dmap,hkey)]`: terminal durable row with fence
  `(generation, epoch, owner_seq, writer_id)`.

In-memory state:

- `W.role`, `W.generation`, `ClusterState.nodes`, `ClusterState.epoch`.
- `LeaseTracker = (generation, epoch, validUntilUnixMs)`.
- Per-fragment `frag.lock` and `frag.mem[hkey]`.

Locks and atomicity:

- `topology.Service.mu`: serializes cluster membership, subscriber map, and
  envelope broadcasts.
- `LeaseTracker.mu`: atomically accepts/revokes lease and snapshots fences.
- `OwnerSequence.mu` plus bbolt transaction: serializes sequence reservation.
- `fragment.RWMutex`: serializes owner-side in-memory mutation and handoff.
- `MySQLStore.mu`: gates closing and hot-path WAL operation entry; bbolt
  transactions provide the actual WAL atomicity.

## 2. Safety Invariants

**S1. A successful acknowledged mutation has durable evidence.**

For every successful `Set/Delete/Expire` that returns nil through the durable
hook path, either a committed WAL record remains for its `ref`, or MySQL already
contains an equal-or-newer row for that `ref`.

**S2. MySQL rows are created only from committed, fenced WAL records.**

`loadFlushableBatch`, `FlushHandoff`, and `FlushHandoffPartition` all pass
records through `isFlushable`, which requires `FlushMySQL`, committed state, and
non-zero generation/owner_seq.

**S3. MySQL never regresses per `(dmap,hkey)`.**

`versionedUpsertClause` updates every persisted column only when the incoming
fence is strictly newer by lexicographic `(G,E,S,W)`.

**S4. Topology envelopes are not ahead of durable epoch.**

The subscriber stream path sends event/prune/bookworm envelopes through
`broadcastLocked`, which calls `saveEpochLocked` before enqueueing. `GetTopology`
also saves the snapshot epoch before returning while primary. On save failure,
no primary envelope is returned or sent.

**S5. New writes require a currently valid primary lease.**

The first gate is `stringkv.Service.requireLease`; the durable gate is
`LeaseGatedStore.PrepareEntry`; the final in-fragment barrier is
`DurableHook.VerifyAfterLock`. `AbortEntry` intentionally bypasses the lease
gate so cleanup can happen after revocation.

**S6. Owner fences are unique enough for durable ordering.**

`OwnerSequence.Stamp` returns monotonically increasing `S` within `(G,E)` and
persists `reservedHigh` before returning any covered sequence. Crash gaps are
allowed; reuse is not. `writer_id` makes equal triples deterministic.

## 3. SET Proof

1. `stringkv.Service.Set` checks `LeaseTracker.ServingAllowed`.
2. The Olric owner computes `hkey` and loads the primary fragment.
3. For non-expire SET, `BeforeSet` runs outside `frag.lock`. It calls
   `SnapshotForWrite`, `OwnerSequence.Stamp`, and `PrepareEntry`. At this point
   WAL has a `prepared` non-flushable fenced record.
4. The fork acquires `frag.lock` and calls `VerifyAfterLock`. If generation
   changed or the lease expired, the fork calls `AfterSet(op, err)`, which
   aborts the prepared record, then returns an error before mutation.
5. Under `frag.lock`, the fork checks write conditions and mutates/replicates
   in-memory state. Any precondition or mutation error routes to `AfterSet` with
   the error, causing `AbortEntry`.
6. On nil mutation error, `AfterSet` calls `CommitEntry`. `CommitEntry` verifies
   the exact `WALSeq`, flips `WALState=committed` and `FlushMySQL=true`, and
   returns before the client sees success.

Therefore success implies a committed fenced WAL record. Later flush may move
the evidence to MySQL, but cannot regress a newer row because of S3.

## 4. DELETE And EXPIRE Proof

DELETE and EXPIRE run their `BeforeX` while holding `frag.lock` because DELETE
depends on lock-protected existence and EXPIRE depends on the resident entry.
They still call `VerifyAfterLock`: `LeaseTracker.mu` is independent of
`frag.lock`, so a subscriber can rotate lease state between `BeforeX` and
mutation. The same commit/abort proof as SET applies.

DELETE on owner miss commits a fenced tombstone. That is correct: a successful
delete of a missing key must still dominate older delayed writes in MySQL.

## 5. GET Miss Proof

1. `stringkv.Service.Get` checks the lease.
2. The fork checks owner/replica memory. On miss, it calls `LoadOnMiss` without
   `frag.lock`.
3. `LoadOnMiss` reads the local WAL first, then MySQL. It does not append WAL,
   does not stamp a fence, and does not mutate MySQL.
4. After load returns, the fork reacquires `frag.lock` and rechecks
   `frag.mem[hkey]`. If a concurrent SET arrived while MySQL was being read,
   the resident value wins and the refill is discarded.

Thus read-through cannot overwrite a fresher in-memory owner write and cannot
produce new durable ordering.

## 6. Watchdog Failover Proof

In Kubernetes mode, a primary obtains `CM.generation+1` through
`NextGeneration` before serving. Nodes accept envelopes with higher generation
and reject lower generation.

On normal streamed updates, membership changes increment `ClusterState.epoch`;
`broadcastLocked` persists that epoch before enqueueing envelopes. If epoch
persistence fails, no envelope is sent, `epochErr` is set, bookworm/reaper skip
broadcasts, health is degraded, and `watchEpochHealth` terminates after the
configured threshold.

Production `OnStoppedLeading` now calls `appConfig.demoteTopology` before
marking health STANDBY and exiting. That invokes
`topologyService.SetLeadership(STANDBY)`, which posts a `PRIMARY_CHANGE`
standby envelope to active subscriber channels, waits for the bounded drain
window, and closes the streams. The production demotion proof is therefore:

- fast path: nodes receive a standby envelope, `LeaseTracker.Apply` rejects it,
  and the subscriber revokes the lease;
- fallback: process exit / gRPC server stop breaks streams, nodes reconnect and
  require a fresh primary envelope;
- final bound: if a stale stream or client request survives briefly,
  `LeaseTracker` stops allowing writes at `validUntil`.

This is safe for writes and restores the intended fast demotion path.

## 7. Crash Recovery Proof

Crash positions:

- Before `PrepareEntry`: no WAL record, no success proof needed.
- After prepare before commit/abort: WAL record is `prepared` and unflushable.
  `Replay`, `PurgeBelowGeneration`, `LoadEntry`, and `isFlushable` all refuse to
  treat it as durable data. `Start` sweeps prepared orphans before flush loop.
- After commit before flush: replay restores live non-tombstone entries to
  memory, and flush loop later writes MySQL.
- After MySQL upsert before `deleteFlushed`: retrying the same committed record
  is idempotent under S3; `deleteFlushed` removes only if `WALSeq` still matches.

Therefore no crash point creates an acknowledged write without either WAL or an
equal-or-newer MySQL row.

## 8. Handoff Proof

`fragment.Move` holds `frag.lock` while calling `DrainForHandoff`. The hook
prefers `FlushHandoffPartition(DMap, PartitionID, PartitionCount)`, which scans
the whole WAL for flushable records in that partition. This is stronger than
resident-hkey draining and covers committed tombstones and evicted-but-dirty
records. If MySQL flush or WAL delete fails, the error aborts migration and the
old owner keeps the fragment.

Records in `prepared` state are skipped. That is correct because their
operation has not been acknowledged and must be resolved by the original
commit/abort path or boot sweep.

## 9. Counterexamples Checked

- **Epoch save fails, envelope still sent:** blocked by `broadcastLocked` early
  return.
- **SET prepared under old lease, mutates after failover:** blocked by
  `VerifyAfterLock` plus abort.
- **Mutation fails after prepare:** blocked by unconditional `AfterX` abort.
- **Abort runs after lease revoked:** allowed intentionally by
  `LeaseGatedStore.AbortEntry`, preventing cleanup deadlock.
- **Old owner delayed flush overwrites new owner:** blocked by MySQL lex fence.
- **Exact `(G,E,S)` collision:** deterministic `writer_id` tiebreaker.
- **Handoff misses tombstone because not resident:** blocked by partition WAL
  scan.
- **Prepared orphan reaches MySQL:** blocked by `isFlushable` and boot sweep.

## 10. Residual Risks / Non-Proved Areas

1. No external string-kv API is bound yet. Future transports must call
   `stringkv.Service`, not raw Olric DMap methods, or they bypass lease gate #1.
   The generated Olric headless Service is annotated as internal-only; a future
   durable client Service should be distinct.
2. Standalone watchdog generation is wall-clock based and outside the
   production monotonic-generation proof.
3. Liveness under long MySQL outage degrades to `ErrQueueFull`, by design.
4. Metrics/exporter wiring for WAL depth, prepared count, and flush lag remains
   an ops gap, not a safety proof gap.

## 11. Verification Run

Targeted suites:

```bash
go test ./internal/store ./internal/stringkv ./internal/node
go test ./internal/topology ./internal/watchdog ./cmd/watchdog
(cd third_party/olric && go test ./internal/dmap)
```
