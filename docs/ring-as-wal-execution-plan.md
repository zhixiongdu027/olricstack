# Ring-as-WAL Execution Plan

## Context

The current sidecar-oplog design treats the mmap ring as the acknowledgement
boundary:

```text
RESP write
  -> DurableHook.BeforeX
  -> Olric DMap mutation
  -> DurableHook.AfterX
  -> ring.Append
  -> client ack
```

This is simple and fast, but the DMap mutation and ring append are not atomic.
If the in-memory mutation succeeds and `ring.Append` fails or the node dies
before append completes, the node can hold a value that was never published to
the durable path.

The existing code mostly protects the client acknowledgement contract:
`ring.Append` must return success before the request returns success. However,
it does not fully protect the in-memory data plane after a post-mutation append
failure. Returning an error to the client is not enough if the dirty value
remains serviceable in Olric memory.

## Design Goals

1. Keep the durable contract easy to explain and audit.
2. Avoid reintroducing a disk WAL or per-write fsync in `olric-node`.
3. Apply backpressure before mutating DMap memory whenever possible.
4. Preserve the current sidecar/MySQL ownership boundary.
5. Prefer fail-stop behavior over speculative recovery for writes that do not
   have a committed durable record.
6. Keep the first implementation small enough to test thoroughly.

## Formal Contract

Use one explicit contract for all later design choices:

```text
A user write may be acknowledged only after a committed WAL record exists in
the Pod-local ring.
```

Definitions:

- `Prepared(tx)`: a ring slot exists and contains the full mutation payload,
  but the mutation outcome is not known.
- `Committed(tx)`: the same slot has been marked committed after the DMap
  mutation succeeded.
- `Aborted(tx)`: the same slot has been marked aborted after the DMap mutation
  failed or was rejected.
- `Acked(tx)`: the user-facing command returned success.
- `Applied(tx)`: the mutation was applied to Olric in-memory state.
- `Flushed(tx)`: sidecar upserted the mutation to MySQL.
- `Recoverable(tx)`: after node restart within the same Pod, sidecar can still
  observe enough information to flush the write.

Required invariants:

```text
I1: Acked(tx)      => Committed(tx)
I2: Committed(tx) => Prepared(tx)
I3: Flushed(tx)   => Committed(tx)
I4: Aborted(tx)   => not Flushed(tx)
I5: Prepared(tx) and not Committed(tx) => not Acked(tx)
I6: Recoverable(tx) => Committed(tx)
```

The current design enforces `Acked => ring.Append ok`, but it cannot state
`Applied => Prepared` because the ring append happens after the DMap mutation.
The target design strengthens the pre-mutation condition to:

```text
Before DMap mutation: Prepared(tx) must already be true.
After successful DMap mutation: Committed(tx) must become true before ack.
```

This converts the unsafe normal failure case from "DMap changed but no ring
space" into "DMap changed but commit marker failed". The remaining case should
be treated as a fatal producer fault, because commit is an in-place state update
on already-reserved WAL space and should not fail under normal backpressure.

## Current Gap

The critical non-atomic window is:

```text
DMap mutation succeeded
  -> ring append has not completed
```

Outcomes:

- If `ring.Append` returns an error, the client gets an error, but DMap memory
  has already changed.
- If `olric-node` crashes before `ring.Append` returns, the client was not
  acknowledged, but the write may have briefly affected memory or replicas.
- If `ring.Append` succeeds, the current contract holds: sidecar can observe
  the entry within the Pod lifetime.

The practical risk is not "acknowledged write lost before ring append"; the
code does not ack before append succeeds. The risk is allowing an unlogged
post-mutation value to continue serving.

## Short-Term Guardrail

Before changing the ring protocol, treat post-mutation append failure as a
fatal data-plane fault:

```text
DMap mutation success + AfterX/ring append failure
  -> revoke local serving lease
  -> mark node unhealthy
  -> stop serving RESP
  -> exit process after best-effort sidecar drain
```

This does not eliminate the crash window, but it keeps the rule clear: a node
that may contain unlogged memory must not continue serving. Unacknowledged
writes are not recovered.

This guardrail is low-cost and should be implemented even if the ring-as-WAL
protocol follows later.

## Target Design: Ring as Pod-Lifetime WAL

Promote the ring from a transport queue to a bounded Pod-lifetime WAL. The
core change is to write a transaction slot before mutating DMap memory, then
commit or abort that same slot after the mutation outcome is known.

```text
BeforeX:
  ring.Prepare(full oplog payload) -> reservation

DMap mutation:
  mutate memory / run quorum path

AfterX:
  mutation success -> reservation.Commit()
  mutation failure -> reservation.Abort()

Client ack:
  only after Commit succeeds

Sidecar:
  process COMMITTED slots only
  skip ABORTED slots
  wait on PREPARED slots
```

Important distinction: `BeforeX` must reserve the durable space. `AfterX`
should only update an in-place state bit. This removes the normal "ring full
after DMap mutation" failure path.

## State Machine

Each write has a single monotonic lifecycle:

```text
NEW
  -> PREPARED
  -> COMMITTED
  -> FLUSHED

NEW
  -> PREPARED
  -> ABORTED
```

Invalid transitions:

```text
NEW       -> COMMITTED
COMMITTED -> ABORTED
ABORTED   -> COMMITTED
FLUSHED   -> ABORTED
```

Client success is allowed only on the `PREPARED -> COMMITTED` transition
after the commit marker is visible. MySQL writes are allowed only from
`COMMITTED`.

This is the core proof shape:

1. `BeforeX` either fails before touching DMap or creates `PREPARED`.
2. DMap mutation can run only with a reservation handle for `PREPARED`.
3. `AfterX(success)` changes that existing slot to `COMMITTED`.
4. The request returns success only if step 3 succeeds.
5. Sidecar flushes only slots whose state is `COMMITTED`.

Therefore `Acked(tx) => Committed(tx)` and `Flushed(tx) => Committed(tx)`.

## Ring Record Shape

The exact binary layout can change during implementation, but the logical
shape should be:

```text
record header:
  magic
  version
  state: PREPARED | COMMITTED | ABORTED
  payload_len
  payload_crc
  txid or slot identity

payload:
  JSON-encoded oplog.Entry
```

Producer rules:

- Write payload and CRC first.
- Publish the slot as `PREPARED`.
- After mutation, atomically change only `state`.
- Never modify payload after `PREPARED` is visible.
- Use release semantics when publishing `COMMITTED` or `ABORTED`.

Consumer rules:

- Use acquire semantics when reading `state`.
- If head is `COMMITTED`, validate CRC, decode payload, add to pending, and
  advance head.
- If head is `ABORTED`, advance head.
- If head is `PREPARED`, stop and retry later.

The first version should stay FIFO. Do not skip over a prepared head entry to
process later committed entries. Skipping makes head reclamation, same-key
ordering, and failure recovery harder to reason about.

## Concurrency Model

`BeforeX -> ring.Prepare` is serialized by the producer lock. This matches the
current ring model: `ring.Producer.Append` is already single-producer and the
hook already serializes append with `appendMu`.

The lock must not be held through DMap mutation:

```text
BeforeX:
  lock producer
  write PREPARED slot
  unlock producer

DMap mutation:
  no producer lock

AfterX:
  atomic state update on the reserved slot
```

This gives:

- serialized WAL reservation,
- concurrent DMap mutations across keys/fragments,
- cheap commit/abort,
- single-threaded sidecar consumption.

The main cost is head-of-line blocking. A slow or stuck mutation that leaves a
PREPARED record at the ring head blocks later committed records from being
drained. This is acceptable for the first version if paired with timeout and
fatal-state handling.

### Same-Key Ordering

The MySQL upsert comparator already orders durable records by
`(generation, epoch, owner_seq, writer_id)`. The ring transaction protocol must
preserve the same rule:

- Allocate `owner_seq` before `Prepare`, not after mutation.
- Include the final fence tuple in the prepared payload.
- Never mutate the payload after prepare.

If two writes to the same key are concurrent, both can be prepared before
either commits. Sidecar FIFO consumption means a later committed slot may wait
behind an earlier prepared slot. This sacrifices some latency but avoids
out-of-order flush surprises. Even if later versions allow out-of-order
committed-slot scanning, MySQL's fence comparator must remain the final
conflict resolver.

### Head-of-Line Blocking

FIFO gives the cleanest proof because the consumer cursor advances only when
the head slot is terminal:

```text
terminal(slot) = COMMITTED or ABORTED
```

A non-terminal `PREPARED` head blocks space reclamation. This is intentional
backpressure, not a correctness bug. The production tradeoff is:

- simple proof and ordered recovery,
- bounded throughput loss when a mutation stalls,
- fail-stop/timeout handling for stuck prepared heads.

## Backpressure

Do not make the ring directly aware of MySQL. Backpressure should flow through
bounded buffers:

```text
MySQL slow/unavailable
  -> sidecar pending reaches limit
  -> sidecar stops advancing ring head
  -> ring free space decreases
  -> node Prepare fails or waits
  -> writes are rejected before DMap mutation
```

Required sidecar changes:

- Add a pending record/byte limit.
- Stop `Pop`/drain when pending is over the limit.
- Resume drain after successful MySQL flush drops pending below the limit.

This preserves layering:

- MySQL health is owned by sidecar.
- Ring exposes capacity/backpressure.
- Node only sees whether WAL space can be reserved.

## Recovery Semantics

The recovery rule must be intentionally conservative:

- `PREPARED` without `COMMITTED`: not acknowledged, do not recover as a write.
- `ABORTED`: skip.
- `COMMITTED`: sidecar may idempotently upsert to MySQL.
- Node crash after DMap mutation but before commit: no commit record, no client
  ack, no recovery.
- Node crash after commit: commit record is durable within the Pod lifetime;
  sidecar drains it if the Pod and shared tmpfs survive.
- Pod reschedule: tmpfs ring is lost; this remains outside the current
  durability scope.

Prepared timeout policy:

- If sidecar sees a `PREPARED` head older than `RING_PREPARE_TIMEOUT`, it should
  treat the producer as unhealthy.
- Initial behavior should be conservative: surface an error/metric and rely on
  node fail-stop rather than silently committing or reordering.
- A later version may abort stale prepared slots for old writer IDs after a node
  restart is detected.

## Failure Matrix

| Failure point | State visible in ring | Client ack? | Recovery behavior | Safety result |
|---|---|---:|---|---|
| Before `Prepare` | none | no | no action | no durable claim |
| During `Prepare` before publish | none or invalid slot | no | consumer ignores invalid/unpublished bytes | no durable claim |
| After `Prepare`, before DMap mutation | `PREPARED` | no | eventually abort/timeout | no false flush |
| DMap mutation fails | `PREPARED -> ABORTED` | no | skip aborted slot | no false flush |
| DMap mutation succeeds, before commit | `PREPARED` | no | timeout/fail-stop; do not recover write | unacked write may be lost |
| Commit marker succeeds | `COMMITTED` | yes | sidecar can flush within Pod lifetime | ack contract holds |
| After commit, before MySQL flush | `COMMITTED` | yes | sidecar flushes if Pod survives | current Pod-lifetime durability |
| Pod reschedule before flush | ring lost | yes possible | not recoverable | accepted durability boundary |
| MySQL flush races with newer owner | `COMMITTED` | yes | MySQL fence comparator resolves | stale write cannot win |

The only remaining acknowledged-write loss is the existing accepted boundary:
the shared-memory ring is lost with the Pod before sidecar flushes to MySQL.
If that is unacceptable, the design must move the WAL to persistent storage or
make MySQL part of the synchronous ack path. Both are intentionally out of
scope for the current performance target.

## Residual Gaps and Production Tradeoffs

The ring-as-WAL protocol reduces the largest local gap:

```text
DMap mutation succeeds but no WAL space exists
```

It does not eliminate every possible loss:

1. **Pod-lifetime durability only.**
   A committed ring record can be lost if the whole Pod and tmpfs disappear
   before MySQL flush. This is already the current architecture's stated
   durability scope.

2. **DMap applied but commit not written.**
   If the process dies after in-memory mutation and before commit, the write is
   unacknowledged and intentionally not recovered. This is the standard
   conservative rule: no commit record means no durable write.

3. **Stuck prepared head.**
   FIFO can block the sidecar behind a prepared slot. The mitigation is
   timeout, metrics, and fail-stop rather than out-of-order processing in the
   first version.

4. **Commit marker write failure.**
   This should be rare because space was reserved and commit is an in-place
   state update. If it happens, treat it as fatal. Do not keep serving from a
   node that may contain applied-but-uncommitted memory.

This is the best production tradeoff if the system values low write latency
and avoids disk fsync in the node. The stronger alternative is synchronous
MySQL commit before ack; the weaker alternative is the current post-mutation
append with fail-stop only.

## Alternative Considered: Commit Intent Before DMap

One tempting alternative is to write the full payload and mark it committed
before mutating DMap. That gives an even simpler durable ack path but creates a
false-positive problem:

```text
ring committed
  -> DMap mutation fails
  -> sidecar flushes a write the cluster never applied
```

This is worse than the current gap because it can make MySQL contain a
successful write for a failed client request. The ring must distinguish intent
from commit, and sidecar must flush only after the DMap outcome is known.

## Implementation Phases

### Phase 0: Document and Test the Existing Contract

- Status: implemented in this plan document and
  [Sidecar Oplog Cutover](sidecar-oplog-cutover.md).
- Tests now pin that post-mutation publish failure triggers a fatal callback.

### Phase 1: Fail-Stop Guardrail

- Status: implemented.
- `stringkv.DurableHook` has `Config.OnFatal`, called once when a mutation has
  already succeeded but durable publish fails.
- `cmd/olric-node` wires `OnFatal` to revoke the local lease and cancel the
  main context, which shuts down RESP/Olric through the normal shutdown path.
- Tests cover fatal-on-publish-failure and no fatal on mutation error.

### Phase 2: Bounded Sidecar Pending

- Status: implemented, except metrics/logging are still future work.
- `sidecar.Config` has `MaxPendingRecords` and `MaxPendingBytes`.
- `cmd/olric-sidecar` exposes `MAX_PENDING_RECORDS` and `MAX_PENDING_BYTES`.
- The sidecar stops draining when pending reaches the configured threshold, so
  the ring head stops advancing and node writes see backpressure.
- `MaxPendingBytes` is an approximate soft threshold: without a ring peek API,
  an entry that crosses the threshold must first be accepted into pending, then
  further drain is stopped. Rejecting it after `Pop` would lose the entry.
- Tests cover record-count backpressure and byte-threshold backpressure.

### Phase 3: Ring Transaction Slots

- Extend `internal/ring` with transaction-slot APIs:

```go
type Reservation struct { /* slot identity */ }

func (p *Producer) Prepare(payload []byte) (*Reservation, error)
func (r *Reservation) Commit() error
func (r *Reservation) Abort() error
```

- Keep old `Append` temporarily for compatibility tests or migrate callers at
  once if the blast radius is small.
- Add ring tests for:
  - prepared slots are not visible as committed,
  - commit makes a slot visible,
  - abort skips a slot,
  - wrap-around with prepared/committed/aborted slots,
  - CRC validation,
  - stale prepared head blocks FIFO consumption.

### Phase 4: Hook Migration

- Move oplog payload construction into `BeforeX`.
- Store reservation identity in `DurableOperation` or a hook-local operation
  map keyed by a generated operation ID.
- In `AfterX`, commit on mutation success and abort on mutation failure.
- Ack only after commit succeeds.
- Keep the fatal guardrail for unexpected commit/abort failures.

### Phase 5: Sidecar Consumer Migration

- Teach sidecar drain logic to consume committed transaction slots.
- Skip aborted slots.
- Wait on prepared slots.
- Add prepared-age metrics and timeout handling.

## Open Decisions

1. Where to store the reservation handle between `BeforeX` and `AfterX`.
   Extending `olricconfig.DurableOperation` is explicit but touches the forked
   interface. A hook-local map avoids interface churn but needs leak cleanup.

2. Whether commit/abort state updates need `msync`.
   Current durability scope is shared-memory Pod lifetime, not disk durability.
   Atomic visibility should be enough unless the scope changes.

3. Prepared timeout behavior.
   First version should fail-stop and alert. Automatic abort of stale prepared
   records is possible later, but should require writer incarnation evidence.

4. Whether sidecar should keep strict FIFO forever.
   FIFO is easiest to prove. Out-of-order scan can improve throughput but should
   be deferred until there is evidence that head-of-line blocking is a real
   bottleneck.

## Recommended Next Step

Phase 1 and Phase 2 are now landed. The next implementation step is Phase 3:
add transaction slots to `internal/ring` behind focused ring tests before
touching the Olric hook path. The protocol change is tractable only if the ring
semantics are pinned independently.

GitHub Actions CI is the authoritative validation path. Local tests are useful
for iteration, but full confidence comes from the CI matrix: unit tests, race
tests, third-party fork tests, host e2e, kind e2e, Docker validation, and
nightly deep suites.

## Current Code Change Map

This section maps the plan onto the current repository layout. It is intended
to guide implementation without requiring another architecture pass.

### `internal/ring/ring.go`

Current role:

- `Producer.Append(payload)` writes one complete payload and advances `tail`.
- `Consumer.Pop()` returns only complete appended payloads.
- There is no transaction state. Once `tail` advances, the consumer treats the
  entry as ready to persist.

Required target changes:

- Add record state to the entry header:

```text
PREPARED = 1
COMMITTED = 2
ABORTED = 3
```

- Add a reservation API:

```go
type Reservation struct {
    // ring mapping, absolute offset, entry size, payload len, maybe txid
}

func (p *Producer) Prepare(payload []byte) (*Reservation, error)
func (r *Reservation) Commit() error
func (r *Reservation) Abort() error
```

- `Prepare` should:
  - reserve capacity exactly like `Append`,
  - write payload and CRC,
  - publish state as `PREPARED`,
  - advance `tail` only after the prepared slot is valid.

- `Commit` and `Abort` should:
  - only update the state field in the reserved slot,
  - use atomic release semantics,
  - never modify payload bytes.

- `Consumer.Pop` should become transaction-aware:
  - head `COMMITTED`: validate CRC, return payload, advance `head`,
  - head `ABORTED`: advance `head`, continue,
  - head `PREPARED`: return empty/not-ready without advancing `head`.

- Keep `Append` temporarily as a compatibility wrapper if useful:

```go
func (p *Producer) Append(payload []byte) error {
    r, err := p.Prepare(payload)
    if err != nil {
        return err
    }
    return r.Commit()
}
```

Implementation caution:

- The current entry header is 8 bytes: `payload_len` + `crc`. It will need a
  versioned header. Do not silently reinterpret old ring files; this ring is
  tmpfs and may be recreated on rollout.
- Preserve the current skip-marker/wrap-around behavior, but ensure skip
  records are terminal and can always be advanced by the consumer.
- Add tests before migrating hook logic.

### `internal/stringkv/durable_hook.go`

Current role:

- `BeforeX` stamps `(generation, epoch)` and pre-checks ring capacity.
- `AfterX(success)` allocates `owner_seq`, builds the oplog entry, and calls
  `ring.Append`.
- `AfterX(error)` does nothing.

Required target changes:

- Move `owner_seq` allocation and oplog payload construction into `BeforeX`.
- Replace capacity pre-check with `ring.Prepare(payload)`.
- Carry the returned reservation from `BeforeX` to `AfterX`.
- `AfterX(success)` commits the reservation.
- `AfterX(error)` aborts the reservation.
- If commit/abort fails, invoke the fatal data-plane fault path.

Reservation storage options:

1. Extend `third_party/olric/config.DurableOperation`.
   - Pros: explicit data flow, easiest to reason about.
   - Cons: touches the Olric fork interface.

2. Store reservations in a hook-local map keyed by operation ID.
   - Pros: avoids putting ring internals into the fork interface.
   - Cons: needs operation ID allocation, cleanup on missed `AfterX`, and leak
     tests.

Recommended first implementation: extend `DurableOperation` with an opaque
field or operation ID. This fork is already internal, and explicit ownership is
better than hidden lifecycle state for this path.

### `third_party/olric/config/durable.go`

Current role:

- Defines `DurableOperation`, `DurableHandoff`, and `DurableHook`.
- The fork passes `DurableOperation` between `BeforeX`,
  `VerifyAfterLock`, and `AfterX`.

Required target changes:

- Add a field that can carry the prepared-ring reservation identity. Avoid
  leaking concrete `internal/ring` types into the third-party package.

Example shape:

```go
type DurableOperation struct {
    ...
    DurableToken any
}
```

or a stricter internal token:

```go
type DurableOperation struct {
    ...
    DurableToken uint64
}
```

If using `any`, only `internal/stringkv` should interpret it. If using `uint64`,
the hook should keep a `map[uint64]*ring.Reservation`.

Recommended first implementation: `DurableToken uint64` plus a hook-local map.
This keeps the fork package free of ring implementation details and gives tests
a stable token to assert.

### `third_party/olric/internal/dmap/put.go`

Current role:

- For normal SET, `BeforeSet` runs before `frag.Lock`.
- `VerifyAfterLock` runs after acquiring the fragment lock.
- `putOnClusterAfterDurable` mutates DMap memory and replicas.
- `AfterSet` runs after mutation and currently appends to the ring.

Required target behavior:

- Keep the current order for SET:

```text
BeforeSet prepares WAL
frag.Lock
VerifyAfterLock
DMap mutation
AfterSet commits or aborts WAL
```

- On any pre-mutation failure after `BeforeSet` succeeded, ensure `AfterSet`
  is called with the failure so the reservation is aborted. Current code
  already routes verify/check/LRU failures through `AfterSet`; keep that
  invariant.

- On `AfterSet` commit failure after mutation success, return the error and
  trigger fatal fault through the hook. Do not keep serving.

### `third_party/olric/internal/dmap/delete.go`

Current role:

- Delete holds `frag.Lock`, calls `BeforeDelete`, verifies fence, performs
  delete or tombstone miss, and calls `AfterDelete`.

Required target behavior:

- `BeforeDelete` prepares a tombstone WAL slot.
- `AfterDelete(nil)` commits the tombstone.
- `AfterDelete(err)` aborts the tombstone.
- Miss-path delete with nil mutation error remains valid: deleting an absent
  key is a successful tombstone operation and should commit.

### `third_party/olric/internal/dmap/get.go`

Current role:

- GET miss calls `DurableHook.LoadOnMiss`.
- It then re-locks the fragment and checks whether a concurrent write arrived
  before refilling memory.

Required target behavior:

- No protocol change required.
- Keep `LoadOnMiss` read-only. It must not prepare, commit, abort, or otherwise
  write to ring/MySQL.
- Add a regression test if possible: read-miss must not allocate a ring
  reservation.

### `third_party/olric/internal/dmap/fragment.go`

Current role:

- `Move` calls `DurableHook.DrainForHandoff` under `frag.Lock` before exporting
  memory.

Required target behavior:

- Keep this ordering.
- Once the sidecar consumer becomes transaction-aware, `DrainPartition` must
  only flush committed records and must not pass a prepared head without a
  timeout/fatal decision.
- If `DrainPartition` encounters a prepared slot for the same partition, it
  should fail the handoff rather than guessing the outcome.

### `internal/sidecar/control.go`

Current role:

- `Run` drains ring entries into `pending` and flushes pending to MySQL.
- `pending` is currently unbounded.
- `LoadFromMySQL` checks pending before backend.
- `DrainPartition` drains ring, flushes matching pending entries.

Required target changes:

- Add bounded pending configuration:

```go
type Config struct {
    BatchSize int
    IdlePoll time.Duration
    FlushTimeout time.Duration
    MaxPendingRecords int
    MaxPendingBytes uint64
}
```

- Stop draining ring when pending is above limit.
- Make `drainRing` transaction-aware through the updated `ring.Consumer`.
- If `Pop` reports head prepared/not-ready, do not treat it as corruption.
  Sleep/retry and expose a metric/log with age if available.
- `DrainPartition` should:
  - drain committed/aborted slots that are currently available,
  - fail or timeout if a prepared head blocks progress,
  - never flush prepared records.

Important: MySQL backpressure should be expressed by not advancing the ring
head, not by draining into unbounded sidecar memory.

### `internal/sidecar/backend_mysql.go`

Current role:

- Provides fence-aware upsert into MySQL.
- Comparator is strict lexicographic
  `(generation, epoch, owner_seq, writer_id)`.

Required target changes:

- No core protocol change required.
- Keep this comparator as the final conflict resolver.
- Consider adding tests where committed records flush out of request order; the
  higher fence must still win.

### `internal/oplog/oplog.go`

Current role:

- Defines the full mutation payload carried in the ring.

Required target changes:

- Include any transaction metadata only if sidecar/MySQL needs it. The ring
  transaction state itself should live in the ring header, not in the JSON
  payload.
- Ensure `owner_seq` and `writer_id` are assigned before `Prepare`.

### `cmd/olric-node/main.go`

Current role:

- Opens ring producer and sidecar control client.
- Builds `DurableHook`.
- Starts RESP server.

Required target changes:

- Wire a fatal fault callback into `DurableHook`.
- On fatal fault:
  - revoke local `LeaseTracker`,
  - stop accepting RESP commands or terminate process,
  - best-effort call sidecar `Shutdown`,
  - exit non-zero so Kubernetes restarts the Pod.

Possible hook config:

```go
stringkv.Config{
    AppendBudget: ...,
    OnFatal: func(error) { ... },
}
```

Even after ring transactions land, keep this path for impossible commit/abort
failures and ring corruption.

### `cmd/olric-sidecar/main.go`

Current role:

- Creates ring file.
- Opens consumer.
- Builds sidecar server.

Required target changes:

- Add env vars:
  - `MAX_PENDING_RECORDS`,
  - `MAX_PENDING_BYTES`,
  - `RING_PREPARE_TIMEOUT`.
- Pass them into `sidecar.Config`.
- Recreate or reject ring files with incompatible transaction-header version.

### Tests to Add or Update

`internal/ring`:

- `TestPrepareDoesNotPopBeforeCommit`
- `TestCommitMakesPreparedEntryVisible`
- `TestAbortSkipsPreparedEntry`
- `TestPreparedHeadBlocksFollowingCommittedEntry`
- `TestWrapAroundWithPreparedCommittedAborted`
- `TestAppendCompatibilityCommitsImmediately`

`internal/stringkv`:

- `TestBeforeSetPreparesRing`
- `TestAfterSetCommitsPreparedReservation`
- `TestAfterSetMutationErrorAbortsReservation`
- `TestCommitFailureTriggersFatal`
- `TestLoadOnMissDoesNotTouchRing`

`internal/sidecar`:

- `TestConsumerIgnoresPreparedHead`
- `TestConsumerSkipsAborted`
- `TestConsumerFlushesCommittedOnly`
- `TestPendingLimitStopsRingDrain`
- `TestDrainPartitionFailsOnPreparedHeadTimeout`

Olric fork tests:

- SET verify failure aborts reservation.
- SET condition failure aborts reservation.
- SET mutation success commits reservation.
- DELETE miss commits tombstone reservation.
- GET miss does not allocate reservation.

E2E:

- MySQL down causes sidecar pending to fill, ring capacity to drop, and new
  writes to fail before DMap mutation.
- Node crash after prepare but before commit does not create MySQL row.
- Node crash after commit but before flush is recovered if the Pod/shared ring
  survives.
