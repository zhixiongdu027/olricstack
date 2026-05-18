# Production Robustness Plan

This document defines the system-level invariants for OlricStack. Local fixes must preserve these invariants before they are considered production-safe.

## Safety Invariants

1. A node must not accept writes unless it holds a non-expired topology lease from the current primary Watchdog.
2. A topology envelope must be fenced by a monotonically increasing Watchdog generation and a monotonically increasing topology epoch.
3. A Watchdog that loses leadership must become unready, break existing topology streams, and exit the process.
4. Kubernetes Pod observations may enrich or exclude heartbeat-owned members, but they must not create members by themselves.
5. A MySQL flush must never overwrite a value written by a newer fenced owner version.
6. A local WAL entry that has been accepted before acknowledging a write must survive process restart and be flushed later.
7. A client-visible write path must have one active owner for a key at a time. Last-write-wins in MySQL is a fallback fence, not a replacement for data-plane ownership.

## Control Plane

The Watchdog owns stack-local topology. Only the elected primary starts gRPC topology service, topology reaping/bookworm loops, and Olric workload reconciliation.

Leadership fencing must not depend on local wall-clock time. The primary allocates its `watchdog_generation` from a stack-local persistent monotonic store before serving. If generation allocation fails, the Watchdog must fail startup instead of serving with an ambiguous fence.

Topology epoch persistence is also part of the fence. If epoch persistence fails, the service must surface the condition and should fail readiness before publishing further topology changes in a hardened production profile.

## Data Plane

Every read/write entry point must depend on the node lease tracker. The minimum policy is:

- Writes require a valid topology lease.
- Reads that may return local dirty state require a valid topology lease unless the API explicitly exposes stale reads.
- MySQL write-behind versions should be derived from topology ownership or a domain fencing version, not local time.

The current `olric-node` binary is still a bootstrap shell. When the real Olric server entry points are added, they must use the lease-gated store path rather than calling the raw MySQL store directly.

## Kubernetes State

Kubernetes readiness is a routing hint, not the membership source of truth. Heartbeats create membership. Pod terminal states remove membership. Sustained NotReady should eventually remove a node from write topology after a configured grace window, but transient readiness flaps should not immediately churn topology.

Watchdog Pods expose readiness only while primary. Olric node readiness should be added and tied to:

- WAL availability.
- MySQL store initialization.
- Valid topology lease for write-serving mode.
- Local Olric server health.

## Persistence

The WAL stores the latest dirty value per key. This is appropriate for cache-value semantics but not for operation logs, counters, append-only streams, or multi-step transactional semantics. Features needing those semantics must add an operation log or CAS layer instead of reusing coalesced WAL entries.

MySQL conflict resolution must be treated as a final defense. Correctness should come from data-plane ownership and fencing before writes reach the store.

## Owner-Side Durable Hook Audit

The current durable data-plane direction is:

1. The public string KV API checks the serving lease.
2. The API calls Olric DMap operations.
3. Olric performs the only owner routing decision.
4. The owner-side DMap business path invokes the durable hook.
5. The durable hook writes owner-local WAL.
6. WAL flusher exports committed records to MySQL.

The system must not reintroduce proxy-side owner arbitration or proxy-side WAL
writes. A non-owner ingress node may call Olric, but it must not create durable
records for the key. Durable records belong to the node that Olric currently
routes to as the primary owner.

### Formal Safety Targets

The production implementation must satisfy these invariants:

1. If a client receives write success, the owner has a committed durable recovery
   record before the response returns.
2. If a client receives write error, the failed write must not become visible in
   MySQL unless the API returns an explicit outcome-unknown error.
3. For a given `(dmap, hkey)`, MySQL accepts only records from the newest fenced
   owner epoch. Wall-clock last-write-wins is not a correctness mechanism.
4. Read-through refill must never overwrite a concurrent successful owner write.
5. Ownership transfer must not complete until the new owner has an equivalent
   durable recovery source for the transferred fragment.
6. A previous owner that lost its fence must not be able to overwrite a newer
   owner's MySQL state through delayed WAL flush.

### Known Production Gaps

1. Owner-side hooks currently write WAL before the Olric memory/quorum mutation
   has fully succeeded. This can produce a durable ghost write if WAL append
   succeeds and Olric mutation later fails.
2. MySQL read-through refill currently needs a conditional recheck under the
   fragment lock. A stale MySQL value loaded on an earlier miss must not refill
   over a newer concurrent write.
3. Local-only refill records share the same coalesced WAL keyspace as flushable
   dirty writes. A refill must not overwrite a flushable dirty record for the
   same `(dmap, hkey)`.
4. MySQL versions currently rely on local time plus writer ID. Production
   fencing must use topology/owner epochs and per-owner monotonic sequences.
5. Fragment migration/rebalance does not yet define durable WAL handoff. Memory
   transfer without durable transfer can lose unflushed owner-local WAL state if
   the old owner disappears. **(Closed by Phase 4 option A — see below.)**
6. WAL replay and serving lease checks need separate semantics. Local recovery
   should not be blocked by a normal serving lease, but replayed records must
   still be validated against ownership/fence state before serving.

### Mainline Hardening Tasks

#### Phase 1: WAL Commit Protocol

- Add WAL record states: `prepared`, `committed`, `local_refill`, and
  `tombstone`.
- Let owner-side hooks create prepared records before mutation.
- Mark records committed only after Olric mutation/quorum success.
- Restrict MySQL flusher to committed records.
- Add tests proving failed Olric writes cannot flush to MySQL.

#### Phase 2: Read-Through Refill Safety

- Recheck memory under fragment lock before applying a MySQL refill.
- Add version/fence comparison before refill.
- Use singleflight for concurrent loads of the same `(dmap, hkey)`.
- Prevent local-only refill records from replacing flushable dirty WAL records.
- Add cleanup/checkpoint rules for local-only refill records.

#### Phase 3: Owner-Fenced MySQL Versioning

- Replace wall-clock versioning with owner-fenced versions.
- Include stack ID, partition ID, Watchdog generation, topology epoch, member
  incarnation, and per-owner sequence.
- Ensure stale previous-owner flushes lose to newer owner writes even with clock
  skew.
- Add MySQL conflict tests for old-owner delayed flush, equal sequence conflicts,
  tombstone ordering, and TTL update ordering.

#### Phase 4: Durable Ownership Handoff

- Define what happens to dirty WAL records during fragment migration.
- Either flush old-owner committed WAL before handoff, transfer WAL state with
  the fragment, or force the new owner to create an equivalent durable checkpoint
  before it becomes write-ready.
- Make owner readiness depend on durable handoff completion.
- Add migration/rebalance crash tests.

**Status (option A — synchronous drain before handoff)**: implemented. The
`config.DurableHook` interface gained `DrainForHandoff(ctx, handoff)`, the
fork's `fragment.Move` calls it under the fragment lock before exporting,
the owner-side hook routes to `MySQLStore.FlushHandoff`, and a drain failure
aborts the migration so the old owner keeps serving until retry. See
`docs/formal-interaction-model.md §8.6 / F13` for the full proof walk and
test list. Options B and C are not currently planned.

#### Phase 5: Operational Backpressure And Observability

- Expose dirty WAL depth, committed/uncommitted counts, local-refill count,
  flusher lag, last successful flush time, and worker errors.
- Define write rejection policy when WAL capacity is exhausted.
- Define read/write behavior during MySQL outage, WAL fsync latency spikes, and
  lease loss.
- Add readiness gates for WAL availability, owner fence validity, durable handoff
  completion, and flusher health.

## Failure Scenarios To Test

- Primary Watchdog loses the Kubernetes Lease while topology streams are active.
- New primary starts after node has seen a higher generation from a clock-skewed old process.
- Epoch ConfigMap writes fail while membership is changing.
- Node loses Watchdog connectivity but continues receiving client writes.
- MySQL is unavailable for longer than WAL capacity can absorb.
- Pod IP is reused while an old heartbeat stream or stale Pod observation exists.
- Node pauses longer than `EXPIRE_AFTER` and then resumes with the same incarnation.
- Multiple nodes attempt to flush the same key with different owner versions.
- WAL append succeeds but Olric owner mutation fails.
- MySQL read-through races with a concurrent owner write.
- A local-only refill races with a flushable dirty WAL record.
- A previous owner flushes after a newer owner has accepted writes.
- Fragment migration occurs while dirty WAL records are unflushed.

## Hardening Order

1. Enforce node-side lease gates for all store and future Olric entry points.
2. Allocate Watchdog generation from persistent monotonic state.
3. Make epoch persistence failure affect readiness or topology publishing.
4. Add Olric node readiness/liveness and expose lease/store health.
5. Add WAL commit states and make the flusher ignore uncommitted records.
6. Harden read-through refill against stale overwrite and local-only WAL
   overwrite.
7. Replace MySQL wall-clock versions with owner-fenced versions.
8. Define durable handoff for fragment migration/rebalance.
9. Add chaos/e2e tests for leadership loss, network partitions, MySQL outage,
   Pod IP reuse, stale owner flush, and dirty WAL migration.
