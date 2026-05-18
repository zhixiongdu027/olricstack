# Formal Interaction Model — OlricStack Four-Way Timing Audit

**Audited revision:** `2d55fec` (2026-05-18)
**Components in scope:** Watchdog control plane · `stringkv` ingress proxy · Olric fork (`third_party/olric/internal/dmap`) · Durable store (`internal/store` WAL + MySQL).

This document is a state-machine-level recipe-and-proof of the four-way timing contract. Every claim cites a code line. The structure is:

1. Methodology (§0)
2. Component boundaries and state variables (§1–§3)
3. Safety / liveness invariants (§4–§5)
4. Forward proofs of four core sequences (§6)
5. Counterexample-driven proofs against the 13 failure scenarios from `docs/production-robustness-plan.md` (§7)
6. Identified weaknesses with mitigation paths (§8)
7. TLA+ migration roadmap and test gaps (§9)

---

## §0 Method

We do **not** write a TLA+ specification yet. The runtime is a mix of gRPC streams, bbolt transactions, fragment locks and Kubernetes leases — encoding all of this in TLA+ is a 3–5× effort and most of the bugs are sequencing bugs that show up in five-step traces. We use a lighter scheme that is still rigorous:

- **State variable enumeration**: each component owns a finite set of variables; persistent ones are flagged.
- **Atomic events**: each transition between states is an atomic event tagged with the lock or transaction it runs under.
- **Invariants**: stated as `∀` predicates over reachable states.
- **Forward proof for golden paths**: walk the event chain, show invariants are preserved at every step.
- **Counterexample-driven proof for failures**: assume an invariant is violated, enumerate the minimal trace that would produce the violation, point to the code line that aborts it.

Every proof step is annotated with `path/to/file.go:Lline` so the proof can be re-validated against future commits.

§9 lists the predicates that should be lifted to a real TLA+ model first when we cross that bridge.

### Performance optimizations applied since the initial audit

This document was first written at revision `aacaeee` against `9d440be`. The optimizations below were applied afterward and the proofs were re-walked to confirm invariants:

| Commit | Optimization | Impact on invariants | Where proof changed |
|---|---|---|---|
| `7537a86` | #1 — `bbolt.Batch` for WAL writes (`appendDirty`, `commitDirty`, `abortDirty`) | None. Idempotency analyzed in store.go:600-617 comment. | §3 E9/E13/E14 atomicity unchanged in spec — bbolt.Batch is functionally identical to Update with shared fsync. |
| `cf6aa45` | #2 — `OwnerSequence` reservation windows | None. Strict-lex MySQL upsert is gap-tolerant. | §3 E23 added; §8.9 new weakness entry. |
| `2d55fec` | #4 — `BeforeSet` outside fragment lock + `VerifyAfterLock` barrier | S5 proof now requires the in-lock verify step. | §3 E8a/E8b/E_v; §4 S5 augmented; §6.1 rewritten; §8.10 new weakness entry. |

Optimization #3 (merging owner_seq.bolt and cache.wal into one bbolt) is deferred until #1/#2/#4 prove stable in production.

---

## §1 Component Boundary

```
                ┌──────────────────────┐
                │  Watchdog (PRIMARY)  │   K8s Lease + ConfigMap
                │  topology.Service    │   <stack>-topology
                └──────┬──────────┬────┘
            gRPC Watch │          │ ConfigMap GET/UPDATE
                       │          ▼
                       │   ┌──────────────┐
                       │   │ epoch_store  │  persistent
                       │   └──────────────┘
                       ▼
            ┌──────────────────────────────────────────────┐
            │              Olric Node                      │
            │  ┌────────────────────────────────────────┐  │
            │  │  node.Subscriber  →  LeaseTracker      │  │  process-local
            │  └────────────────────────────────────────┘  │
            │  ┌────────────────────────────────────────┐  │
            │  │  stringkv.Service (lease gate #1)      │  │
            │  │       │                                │  │
            │  │       ▼                                │  │
            │  │  Olric DMap.Put / Get / Delete         │  │
            │  │       │                                │  │
            │  │       ▼ fragment lock                  │  │
            │  │  fork dmap.put / get / delete          │  │
            │  │       │                                │  │
            │  │       ▼ DurableHook.BeforeX            │  │
            │  │  stringkv.DurableHook                  │  │
            │  │       │                                │  │
            │  │       ▼  (lease gate #2 + fence stamp) │  │
            │  │  LeaseGatedStore  ──┐                  │  │
            │  │                     │                  │  │
            │  │                     ▼                  │  │
            │  │  store.MySQLStore  WAL (bbolt)         │  │  persistent
            │  │       │                                │  │
            │  │       ▼ flushLoop                      │  │
            │  └───────┼────────────────────────────────┘  │
            └──────────┼──────────────────────────────────┘
                       ▼
                  ┌─────────┐
                  │  MySQL  │   persistent, fenced upsert
                  └─────────┘
```

The four interaction edges audited here:

| Edge | Direction | Code anchor |
|---|---|---|
| W↔N | Watchdog → Node (envelope), Node → Watchdog (heartbeat) | `internal/topology/service.go:181`, `internal/node/subscriber.go:72` |
| Ingress→Olric | `stringkv.Service` → Olric `DMap` | `internal/stringkv/service.go:51`, `olric_adapter.go:39` |
| Olric→Hook | Fork `dmap.put/get/delete` → `DurableHook` | `third_party/olric/internal/dmap/put.go:319`, `delete.go:135`, `get.go:379` |
| Hook→MySQL | `DurableHook` → WAL → `flushLoop` → MySQL | `internal/stringkv/durable_hook.go:74`, `internal/store/store.go:485,547` |

---

## §2 State Variables

Persistent variables (survive process restart) are flagged ⚑. Everything else is in-memory only.

### 2.1 Watchdog

| Symbol | Type | Description | Persistence |
|---|---|---|---|
| `W.role` | `{PRIMARY, STANDBY}` | leader-election state | in-memory; reset to STANDBY at startup (`health.go:21`) |
| `W.gen` | `ℕ⁺` | watchdog generation, monotonic per stack | ⚑ ConfigMap `watchdogGeneration` (`epoch_store.go:87`) |
| `W.epoch[stack]` | `ℕ` | topology epoch | ⚑ ConfigMap `topologyEpoch` + in-memory `ClusterState.epoch` |
| `W.epochErr` | error | last epoch persistence error | in-memory; gates broadcasts (`service.go:586`) |
| `W.subs[stack][nodeID]` | `chan envelope` | active gRPC streams | in-memory, capacity 8 (`service.go:195`) |
| `W.cluster[stack].nodes[nodeID]` | `NodeState` | per-node lifecycle FSM | in-memory (`watchdog/state.go:50`) |

### 2.2 Node Lease (per process)

| Symbol | Type | Description | Persistence |
|---|---|---|---|
| `L.gen` | `ℕ` | last accepted watchdog generation | in-memory (`subscriber.go:147`) |
| `L.epoch` | `ℕ` | last accepted topology epoch | in-memory |
| `L.validUntil` | `unix_ms` | lease deadline | in-memory |
| `seq.persisted` | `(G, E, S)` | last allocated owner sequence | ⚑ bbolt `owner_seq` bucket (`owner_seq.go:74`) |

### 2.3 Olric Fork (per fragment)

| Symbol | Type | Description | Persistence |
|---|---|---|---|
| `frag.lock` | `sync.RWMutex` | per-fragment lock | in-memory |
| `frag.mem[hkey]` | `Entry` | in-memory key/value | in-memory |
| `routing` | `RoutingTable` | partition→owner map | in-memory, gossip-driven |

### 2.4 Durable Store (per node)

| Symbol | Type | Description | Persistence |
|---|---|---|---|
| `wal.dirty[walKey(ref)]` | `dirtyEntry` | WAL record | ⚑ bbolt `dirty` bucket |
| `record.WALSeq` | `ℕ⁺` | local monotonic, prepared↔commit binding | ⚑ |
| `record.WALState` | `{Prepared, Committed, Local}` | record state | ⚑ |
| `record.fence` | `(G, E, S)` | owner fence triple | ⚑ |
| `record.FlushMySQL` | `bool` | committed-and-eligible flag | ⚑ |
| `wal.seq` | `ℕ⁺` | bbolt-stored monotonic, allocates `record.WALSeq` | ⚑ (`store.go:777`) |
| `mysql[(dmap,hkey)]` | row + fence triple | durable source of truth | ⚑ MySQL |

### 2.5 Derived predicates

We will use these abbreviations in invariant statements:

```
LeaseValid(L, t)     ≡  L.validUntil > t                                                (subscriber.go:208)
FenceLE(a, b)        ≡  a.G < b.G  ∨  (a.G = b.G ∧ a.E < b.E)
                          ∨  (a.G = b.G ∧ a.E = b.E ∧ a.S < b.S)                        (store.go:78)
Flushable(r)         ≡  r.FlushMySQL ∧ r.WALState = Committed
                          ∧ r.fence.G > 0 ∧ r.fence.S > 0                               (store.go:847)
PrimaryEnvelope(e)   ≡  e.role = PRIMARY  ∧  e.validUntil > now()                       (subscriber.go:170)
```

---

## §3 Atomic Events

Listed in causal order so subsequent sections can name them by ID.

| ID | Event | Triggered by | Atomic w.r.t. |
|---|---|---|---|
| E1 | `NextGeneration(stack)` | New PRIMARY startup | ConfigMap optimistic-lock loop (`epoch_store.go:87`) |
| E2 | `BroadcastEnvelope(stack, reason)` | reaper / bookworm / heartbeat | `Service.mu` + `epoch_store.SaveEpoch` (`service.go:356`) |
| E3 | `Subscriber.Apply(envelope)` | Stream Recv | `LeaseTracker.mu` (`subscriber.go:170`) |
| E4 | `Subscriber.Revoke()` | Apply error | `LeaseTracker.mu` (`subscriber.go:202`) |
| E5 | `stringkv.requireLease()` | Service entry | `LeaseTracker.RLock` |
| E6 | Olric routing decision | `dm.put/get/delete` ingress | RoutingTable read (`put.go:dm.put`) |
| E7 | `frag.Lock()` | owner-side enter | per-fragment mutex |
| E8a | `BeforeSet(op)` **outside** `frag.Lock` | client_set ingress | `LeaseTracker.RLock` + bbolt Batch (`put.go:putOnCluster step 5`) |
| E8b | `BeforeDelete/Expire(op)` **inside** `frag.Lock` | client_delete / client_expire | E7 + bbolt Batch (`delete.go:deleteKey`, `put.go:isExpire branch`) |
| E9 | `PrepareEntry(record)` | hook | bbolt `Batch` tx — coalesces fsyncs (`store.go:appendDirty`) |
| E_v | `VerifyAfterLock(op)` | fork inside `frag.Lock` | `LeaseTracker.RLock` (`durable_hook.go:VerifyAfterLock`, `put.go:after f.Lock`) |
| E10 | `putEntryOnFragment` / `f.storage.Delete` / `UpdateTTL` | fork mutation | inside E7 |
| E11 | `syncPutOnCluster` quorum | replica writes | inside E7 (`put.go:syncPutOnCluster`) |
| E12 | `AfterSet/Delete/Expire(op, mutErr)` | fork (unconditional) | inside E7 (`put.go:end of putOnCluster`) |
| E13 | `CommitEntry(ref, walSeq)` | hook with `mutErr=nil` | bbolt `Batch` tx (`store.go:commitDirty`) |
| E14 | `AbortEntry(ref, walSeq)` | hook with `mutErr≠nil` | bbolt `Batch` tx (`store.go:abortDirty`) |
| E15 | `flushLoop` tick | `notify` chan / interval | `s.mu` + GORM batch (`store.go:flushLoop`) |
| E16 | `versionedUpsert` | flusher | MySQL row-level lock + `ON CONFLICT` (`store.go:versionedUpsertClause`) |
| E17 | `deleteFlushed` | post-upsert | bbolt `Update` tx (`store.go:deleteFlushed`) |
| E18 | `LoadOnMiss(op)` | fork get-miss | bbolt View + GORM `First` (`durable_hook.go:LoadOnMiss`) |
| E19 | Refill re-lock + check | fork after E18 | second `f.Lock()` (`get.go:after LoadOnMiss`) |
| E20 | `closeSubscribersForLeadershipChange` | demote | two-phase: `Service.mu` then 50 ms drain (`service.go:closeSubscribersForLeadershipChange`) |
| E21 | `PurgeBelowGeneration(minGen)` | new envelope arrival | bbolt `Update` tx (`store.go:PurgeBelowGeneration`) |
| E22 | `WAL Replay` | `Engine.Start` | bbolt `View` (`store.go:Replay`) |
| E23 | `OwnerSequence.Stamp` | inside hook fence stamp | `OwnerSequence.mu` + amortized fsync (`owner_seq.go:Stamp`) |

---

## §4 Safety Invariants

Stated as predicates over all reachable states. Each is proved (forward, golden path) in §6 and tested against attacks (§7).

### S1 — Acknowledged write has a committed durable record

```
∀ acknowledged client SET/EXPIRE/DEL response r:
    ∃ wal.dirty[walKey(r.ref)] = R such that
        R.WALState = Committed  ∧  R.fence > 0
    OR  ∃ mysql row m for (dmap, hkey) with fence ≥ R.fence
```

**Hook chain enforcing it:** E5 (lease #1) → E6 → E7 → E8 → E9 → E10/E11 → E12 → E13 (only if mutErr = nil). The fork explicitly runs E12 *unconditionally* even when E10/E11 fails, so E14 cleans up on error (`put.go:354-367`, `delete.go:168-176`).

### S2 — Every MySQL row originates from a Committed WAL record with a non-zero fence

```
∀ MySQL row m persisted by flusher:
    ∃ R ∈ wal.dirty during E15 such that
        R.WALState = Committed  ∧  R.FlushMySQL = true
        ∧ R.fence.G > 0  ∧  R.fence.S > 0
```

**Enforcer:** `isFlushable` (`store.go:847`). The flusher batch is filtered by `loadFlushableBatch` (`store.go:732`) which calls `isFlushable` for each entry. Records in `Prepared`, `Local`, or with a zero fence cannot reach MySQL.

### S3 — MySQL upsert is strictly fence-monotonic per (dmap, hkey)

```
∀ (dmap, hkey), ∀ consecutive states m_i → m_{i+1} of mysql[(dmap, hkey)]:
    FenceLE(m_i.fence, m_{i+1}.fence)
```

**Enforcer:** `versionedUpsertClause` (`store.go:799`). Every column is wrapped in a `CASE WHEN <newer> THEN VALUES(...) ELSE <self> END`, where `<newer>` is the strict lex predicate on `(generation, epoch, owner_seq)`. SQLite path is symmetric (`store.go:820`).

### S4 — Envelope durability precedes delivery

```
∀ envelope e ∈ {sent on any subscriber stream}:
    e.epoch ≤ persisted W.epoch[e.stack_id] AT THE INSTANT OF SEND
```

**Enforcer:** `broadcastLocked` calls `saveEpochLocked` and **early-returns on error** (`service.go:358-364`). `sendLatestLocked` early-returns on `EpochError` (`service.go:374`). `standbyEnvelope` does not carry an epoch (its only `Reason` is `PRIMARY_CHANGE`) so it bypasses S4 by construction (`service.go:401`).

### S5 — Active lease implies envelope provenance from current PRIMARY

```
∀ Node N with LeaseValid(L_N, now()):
    L_N.gen ≥ W.gen of the current PRIMARY at the moment of last apply
    ∧  L_N.gen was once the value of W.gen for some PRIMARY since this stack started
```

**Enforcer chain:**

- `LeaseTracker.Apply` rejects `WatchdogRole ≠ PRIMARY` (`subscriber.go:171`).
- Apply rejects `validUntil ≤ now()` (`subscriber.go:174`).
- Apply rejects `gen < L.gen` (`subscriber.go:182`).
- `closeSubscribersForLeadershipChange` posts a `PRIMARY_CHANGE` envelope before tearing the stream, which Apply rejects → `Revoke()` (`subscriber.go:171,245`).
- Generation source is `NextGeneration` in ConfigMap, which is monotonic by optimistic-locked increment (`epoch_store.go:87`).

**Two-phase fence check (post-`2d55fec`)**: client_set prepares the durable record outside the fragment lock to coalesce fsyncs (Optimization #4 in commit `2d55fec`). This widens the time window between fence stamp (E8a) and the in-memory mutation (E10), so a second barrier is required:

- `BeforeSet` snapshots `(L.gen, L.epoch)` inside `LeaseTracker.RLock` and writes it into the prepared record AND into the returned `op.FenceGeneration / op.FenceEpoch` (`durable_hook.go:74-91`).
- The fork then acquires `frag.Lock()` and immediately calls `VerifyAfterLock(op)` (`put.go:339-345`).
- `VerifyAfterLock` re-reads `(L.gen, L.epoch, L.validUntil)` and rejects if **any** of: lease has expired, `L.gen ≠ op.FenceGeneration`, or `L.epoch < op.FenceEpoch` (`subscriber.go:262-275`, `durable_hook.go:148-156`).
- A non-nil verify result routes the fork to `AfterSet(op, vErr)` which calls `AbortEntry`. The prepared record is removed before any in-memory or replica mutation runs.

This preserves S5 by reduction: even though the fence was stamped without the fragment lock, no acknowledged write exists without the fork holding `frag.Lock()` AND the lease still owning the same `(L.gen, L.epoch)` it had at stamp time. The post-stamp window is bounded by the verify check, not by hope.

`client_delete` and `client_expire` keep the original in-lock `BeforeX` ordering (delete because tombstone fence depends on lock-protected `f.storage.Check`; expire because TTL fence depends on lock-protected `f.storage.Get`). They still call `VerifyAfterLock` defensively — a no-op when the fence was stamped under the same lock, but cheap insurance against future refactors.

### S6 — One owner-fenced write per acknowledged primary mutation

This follows from S1 + S3 + uniqueness of `(G, E, S)` for a successful E13. The fence sequencer (`OwnerSequence.Stamp`, `owner_seq.go:75`) guarantees:

- distinct `(G, E)` reset `S` to 1;
- same `(G, E)` increments `S`;
- regression on `(G, E)` returns `ErrOwnerSequenceRollback`.

Persisted before return (`owner_seq.go:98`), so the same `S` cannot be reused after a crash between E9 and E10.

---

## §5 Liveness Invariants

### L1 — Demote → ingress freezes within bounded time

```
After W.role transitions PRIMARY → STANDBY at time t₀,
    ∀ Node N: N rejects new SET/DEL/EXPIRE before t₀ + Δ_demote
    where Δ_demote = max(50 ms drain window, leaseTTL)
```

**Enforcers:**

- E20 fast path: `closeSubscribersForLeadershipChange` posts a standby envelope and closes the stream within 50 ms (`service.go:528-530`).
- Slow path: even if the standby envelope is dropped, the lease times out after `leaseTTL` (default 30 s, `topology.Config.LeaseTTL`). `ServingAllowed` returns false past that (`subscriber.go:208`).

### L2 — New PRIMARY → ingress recovers within bounded time

```
After a new W' with W'.gen > previous W.gen begins broadcasting,
    ∀ Node N: N accepts SET/DEL/EXPIRE within Δ_promote
    where Δ_promote = bookwormInterval + reconnectInterval
```

**Enforcers:**

- `runTopologySubscription` reconnect loop with jittered `WATCHDOG_RECONNECT_INTERVAL` (default 3 s, `cmd/olric-node/main.go:210-221`).
- N1 generation-bump branch in `LeaseTracker.Apply`: a higher `gen` resets epoch baseline unconditionally (`subscriber.go:184-191`). No deadlock from epoch ordering across primary changes.
- `RunBookworm` posts a snapshot every `BOOKWORM_INTERVAL` (default 10 s, `service.go:127`).
- `PurgeBelowGeneration` runs on the first envelope of the new generation (`cmd/olric-node/main.go:148-163`), so writes resumed under `W'.gen` cannot collide with stale WAL state in MySQL upserts.

---

## §6 Forward Proof — Four Core Sequences

Each sequence is a totally-ordered event chain. After each step we record what changes in the state and confirm which invariants still hold.

Notation: `[E1; loc] description // inv-checks` reads "event E1 at the cited location, then human description, then the invariants verified at that step".

### 6.1 Happy SET on the partition owner

Pre-state: `L_N` valid with `L_N.gen = G_p, L_N.epoch = E_p`. `seq.persisted = (G_p, E_p, S_p)` with reserved ceiling `R_p ≥ S_p` (post-Optimization #2). No prior record for `(dmap, hkey)` in WAL.

The event chain after Optimization #4 (`2d55fec`) splits into a lock-free preparation phase and an in-lock mutation phase. The two phases are reconciled by `VerifyAfterLock`.

```
Step 1   [E5; service.go:108]
         stringkv.Service.requireLease() reads ServingAllowed(L_N, now())
         → true. Routes to DMapProvider.
         // S5 holds (lease still valid)

Step 2   [E6; put.go:421]
         dm.put computes hkey, checks routing. If owner ≠ self → RPC to owner;
         we assume owner = self for this sequence.

Step 3   [put.go:294]
         dm.loadOrCreateFragment returns f. NO LOCK YET.

Step 4   [put.go:316-318]
         e.timeout defaults seeded; nt = prepareEntry(e). prepareEntry only
         allocates an Entry struct via storage.NewEntry — pure function on
         the input, safe outside the lock.

Step 5   [E8a; put.go:329-348]
         BeforeSet(op) called OUTSIDE f.Lock.

Step 6   [E9 inside Before; durable_hook.go:74-91]
         6a. fencedRecord builds EntryRecord with origin=client_set, FlushMySQL=true.
         6b. stampFence:
             - LeaseTracker.SnapshotForWrite(now) atomically reads L.gen, L.epoch
               under L.mu while re-asserting validUntil>now (subscriber.go:217).
               OK → returns (G_p, E_p, true).
             - OwnerSequence.Stamp(G_p, E_p):
               case (G_p, E_p) = persisted:
                 if S_p+1 ≤ R_p: bump in-memory seq, no fsync (post-#2).
                 else: extend reservation by N, fsync, then bump.
               // returned (G_p, E_p, S_p+1) is guaranteed durable on disk.
         6c. PrepareEntry writes WAL with WALState=Prepared, FlushMySQL=false,
             fence=(G_p, E_p, S_p+1), WALSeq=next via bbolt.Batch (post-#1).
             // op.Version ← WALSeq.
             // op.FenceGeneration/Epoch/OwnerSeq ← stamped values.

         Invariants at Step 6 end:
         // S2 holds: record is Prepared ⇒ isFlushable=false (store.go:851).
         // S6: fence triple unique by construction of Stamp.
         // S5: NOT YET fully proved — there is now a window before the lock.

Step 7   [E7; put.go:338]
         f.Lock() acquired.

Step 8   [E_v; put.go:339-345, durable_hook.go:148-156]
         VerifyAfterLock(op) re-reads (L.gen, L.epoch, L.validUntil) under
         L.mu. Passes iff:
           validUntil > now          (lease still alive)
           AND L.gen == op.FenceGeneration   (no leadership change)
           AND L.epoch >= op.FenceEpoch      (only forward epoch progress)
         If NOT passes → AfterSet(op, vErr) routes to AbortEntry, return error.
         // S5 fully restored: any subsequent step happens under a fence that
         // was just verified to match the active lease.

Step 9   [put.go:347-356]
         checkPutConditions (NX/XX) under lock — uses f.storage state.
         setLRUEvictionStats under lock if configured.
         Failures here also route through AfterSet(op, err) → AbortEntry.

Step 10  [E10; put.go:395 → put.go:411-428]
         putOnClusterAfterDurable. Replicas=1 → putEntryOnFragment.
         storage.Put writes in-memory entry under frag lock.

Step 11  [E11 (only if ReplicaCount>1); put.go:174-209]
         syncPutOnCluster fans out PutEntry to backups. Skipped at ReplicaCount=1.

Step 12  [E12 / AfterSet; put.go:396-403]
         AfterSet(op, mutErr) called unconditionally.

Step 13a [E13; durable_hook.go:213-229, store.go:651-680]
         (Case mutErr = nil)
         CommitEntry(ref, WALSeq) via bbolt.Batch (post-#1).
         Flips WALState=Committed, FlushMySQL=true.
         notifyFlush() if depth ≥ BatchSize.
         // S1 satisfied: dirty[ref].WALState = Committed before return.
         // S2: record now Flushable.

Step 13b [E14; durable_hook.go:220-225, store.go:682-704]
         (Case mutErr ≠ nil)
         AbortEntry(ref, WALSeq) via bbolt.Batch.
         WALSeq match required, Committed state rejected (store.go:699).
         // S1 still preserved: client receives mutErr, no ack was given.

Step 14  [E12 returns]
         If mutErr ≠ nil, putOnCluster returns mutErr to client.
         Else returns hookErr (typically nil).
         // Client ack iff CommitEntry succeeded ⇒ S1 holds.

Step 15  [f.Unlock at function exit]

Step 16+ [E15..E17 flushLoop]
         Identical to pre-#4: loadFlushableBatch + GORM upsert + deleteFlushed.
         // S3 holds via versionedUpsertClause.
```

**Concurrency improvement from #4**: between Step 5 and Step 7, no goroutine holds `f.Lock`. Concurrent SETs on different keys of the *same* fragment can therefore interleave their `PrepareEntry` fsyncs through bbolt.Batch (Optimization #1) instead of serializing on the fragment mutex. The window where the fragment lock is contended shrinks from "prepare + mutate + commit" to "verify + check + mutate", removing one fsync from the critical section.

**Concurrency cost from #4**: the verify step (Step 8) adds a single `LeaseTracker.RLock` acquisition. It is uncontested 99% of the time; under a primary change the lease is being mutated under `LeaseTracker.mu.Lock` on the subscriber goroutine (`subscriber.go:178`), and verify must wait for that write lock to release. The added latency is bounded by the duration of one `Apply` call (~µs).

**Conclusion for 6.1**: Client receives success ⟺ Steps 8 + 13a both executed ⟺ S1 + S5 hold for this write. S2 + S3 + S6 hold by construction at each step. The post-#4 ordering preserves every invariant from the pre-#4 proof while removing the WAL fsync from the fragment lock.

### 6.2 Durable GET miss → MySQL refill

Pre-state: `L_N` valid; key persisted in MySQL with fence `(G_m, E_m, S_m)`; `frag.mem[hkey]` empty (LRU evicted earlier or never resident).

```
Step 1   [stringkv.Service.Get; service.go:51-67]
         requireLease → true. Calls dm.Get → embeddedClient.Get → fork Get.

Step 2   [get.go:372-377]
         dm.Get computes hkey, owner = self. Calls getOnCluster.

Step 3   [get.go:330-348, lookupOnOwners / lookupOnReplicas]
         RLock fragment, storage.Get(hkey) → ErrKeyNotFound.
         (No previous owners in single-node case → versions=[nil].)
         // sanitizeAndSortVersions returns empty → ErrKeyNotFound.

Step 4   [get.go:378-388]
         Fork branches: DurableHook != nil → LoadOnMiss.
         op.Entry = engine.NewEntry()  (template the hook will Decode into).

Step 5   [E18; durable_hook.go:143-161]
         LoadOnMiss runs WITHOUT the fragment lock (get.go documents this).
         - committer.LoadEntry → MySQLStore.LoadEntry:
             a. loadDirty (bbolt View) → no dirty entry for this ref.
             b. db.First by (dmap, hkey) → MySQL row found, fence (G_m, E_m, S_m).
             c. Tombstone/TTL gate (store.go:338-344): both pass.
             d. Returns rec.Clone().
         - entry.Decode(record.EncodedEntry).
         // No mutation, no WAL append, no fence stamp.

Step 6   [E19; get.go:399-417]
         Fork re-Lock()s the primary fragment AFTER LoadOnMiss.
         Re-checks f.storage.Get(hkey):
         - If a concurrent SET landed during Step 5 (mid-LoadOnMiss), existing is
           returned as the result; LoadOnMiss output discarded.
           // S3 holds: stale MySQL refill cannot overwrite the just-written
           // in-memory value.
         - Else putEntryOnFragment writes the refilled entry. Engine.Put writes
           memory only; backing CacheStore is NOT called (olricstore/engine.go:84-91).
           // No WAL record, no MySQL flush, by design.

Step 7   [get.go:418-428]
         Returns the entry to the client.
```

**Conclusion for 6.2**: A miss-refill creates **zero** durable side effects beyond the already-existing MySQL row. Concurrency safety with a winning SET is enforced by the second `f.Lock()` window (Step 6). Note `Engine.Get` (`olricstore/engine.go:101`) is **never** called from a path that would write to backing store on miss — refill is exclusively in the fork's `Get` path. This matches `docs/durable-string-kv-design.md §"Why Generic Storage Engine Hooks Are Insufficient"`.

### 6.3 PRIMARY demote → promote (failover)

Pre-state: PRIMARY `W₁` with gen `G₁`, epoch `E₁`. Node N has `L_N = (G₁, E₁, *)`. STANDBY `W₂` is alive but not leading.

```
Step 1   [K8s leader election; cmd/watchdog/main.go:138]
         W₁ loses lease → OnStoppedLeading callback.

Step 2   [E20; service.go:500-534]
         W₁ calls health.SetLeadership(STANDBY, 0) at main.go:139.
         topologyService also gets SetLeadership(STANDBY, …) via observer chain.
         closeSubscribersForLeadershipChange runs:
         - Phase 1 under s.mu: for each (stack, nodeID), drain sub.ch, push a
           standbyEnvelope (reason=PRIMARY_CHANGE, no epoch), remove sub from map.
         - Drain window: time.Sleep(50ms).  ← weak point #2, §8.
         - Phase 2: close(sub.done).

Step 3   [Watch goroutine; service.go:209-222]
         W₁'s Watch loop sees sub.ch has the standbyEnvelope queued.
         stream.Send(standbyEnvelope) flushed to N.

Step 4   [Subscriber.Run; subscriber.go:121-137]
         N's stream.Recv returns standbyEnvelope.
         lease.Apply rejects: envelope.WatchdogRole = STANDBY → ErrTopologyNotPrimary.
         shouldRevokeLease(err) = true → lease.Revoke() → L_N.validUntil = 0.
         Run returns err.
         // S5 holds: lease no longer valid against any future write.

Step 5   [runTopologySubscription; main.go:211-221]
         Outer loop logs "topology subscription ended", waits jittered
         WATCHDOG_RECONNECT_INTERVAL (~3s).
         // Client SET/DEL/EXPIRE during this window:
         //   - stringkv.Service.requireLease → ErrLeaseExpired (service.go:108)
         //   - or, race: durable_hook.SnapshotForWrite returns ok=false →
         //     ErrLeaseExpired (durable_hook.go:185-194).
         //   ⇒ Writes are rejected. L1 satisfied.

Step 6   [main.go:113-149; new W₂ wins lease]
         W₂.OnStartedLeading invokes runPrimary.
         allocateWatchdogGeneration → epoch_store.NextGeneration:
         - ConfigMap Update with optimistic lock increments generation.
         - Returns G₂ = G₁ + 1.
         topologyService.SetEpochStore + SetLeadership(PRIMARY, G₂).
         RunReaper + RunBookworm + watchEpochHealth go.
         // S5 monotonicity preserved: G₂ > G₁ persisted in ConfigMap BEFORE W₂ serves.

Step 7   [subscribeOnce reconnects]
         N dials watchdog Service. Watch stream opens. First heartbeat sent.

Step 8   [registerHeartbeat; service.go:253-289]
         W₂ receives heartbeat. loadEpochLocked: state.cluster.epoch bumped to
         ConfigMap-persisted value (which is ≥ E₁; W₁ persisted last epoch in E2
         before sending). ApplyHeartbeat creates node entry → epoch increments.
         broadcastLocked(EVENT):
         - saveEpochLocked succeeds → state.epoch persisted.
         - Envelope with (stackID, members=[N], epoch=E₂, gen=G₂, role=PRIMARY).
         // S4 satisfied (epoch persisted before send).

Step 9   [Subscriber.Apply; subscriber.go:170-200]
         N receives envelope with gen=G₂ > L.gen=G₁.
         Switch hits the "generation > L.gen" branch (subscriber.go:184-191):
           L.gen, L.epoch, L.validUntil ← (G₂, E₂, validUntil).
         // N1 in the comment: new gen resets epoch baseline. No deadlock.
         observedEpoch.Store(envelope.Epoch).
         Joiner.Join called.

Step 10  [olricJoiner.Join; main.go:135-177]
         envelope.WatchdogGeneration (G₂) > j.lastPurgedGen (0 on fresh process
         or G₁ from prior session).
         PurgeBelowGeneration(G₂):
         - Walks dirty bucket, deletes records with fence.G < G₂ AND
           WALState ≠ Prepared (store.go:240-263).
         // S3 protected: stale fenced WAL records pre-failover cannot win the
         // upsert if they were never flushed before demote.
         lastPurgedGen ← G₂.
         olricDB.Join(peers) reattaches memberlist if needed.

Step 11  [Next client SET]
         requireLease → true (L_N now valid). Hook flows through 6.1.
         // L2 satisfied: write acceptance within
         // jitter(reconnectInterval, 0.2) + handshake + envelope arrival.
```

**Conclusion for 6.3**: There exists no instant where a write is accepted under `G₁` after `t₀` *and* a new write under `G₂` could overwrite it with a smaller fence. Combined with S3, the fence triple guarantees writes from `W₂` always win at MySQL.

**Caveat surfaced for §8.2**: The 50 ms drain window is a heuristic. A network stall longer than 50 ms but shorter than `leaseTTL` (30 s) would force N to wait for the slow path. This is L1-compliant (bounded), but the optical user-visible window is wider than the fast path implies.

### 6.4 Node crash recovery

Pre-state: Node N had a stream of writes under `(G_p, E_p)`. Crashed between two arbitrary events. WAL/seq files persisted on the local volume. Process restarts.

We enumerate the possible crash positions Cα through Cδ:

| Crash point | State of WAL record | Recovery action |
|---|---|---|
| Cα: before E9 commits | no record | nothing to do; client got an error or timeout |
| Cβ: after E9, before E13/E14 | `Prepared`, `FlushMySQL=false` | record persists across restart |
| Cγ: after E13, before E15 flush | `Committed`, `FlushMySQL=true` | flusher resumes after Start |
| Cδ: after E15 but before E17 | `Committed`, possibly already in MySQL row | upsert is idempotent under S3 |

```
Step 1   [main.go:30-71 startup]
         buildCacheStore opens MySQLStore (which opens bbolt WAL). WAL persists.
         NewOwnerSequence reopens bbolt; seq.persisted restored (owner_seq.go:46-62).
         // No data loss: ⚑ files survive.

Step 2   [olric.Start → Engine.Start; olricstore/engine.go:36-60]
         Backing store implements ReplayStore → Replay called.
         Replay iterates dirty bucket (store.go:270-308):
         - WALState == Prepared → continue. // Cβ records skipped.
         - Tombstones → continue.
         - Expired → delete and skip.
         - Otherwise → f(record) loads into in-memory engine.

         The fork then calls store.Start to begin flushLoop (engine.go:53,
         store.go:199-211).
         // Cγ records: present in WAL, eligible for flush. flushLoop will
         // pick them up next tick.
         // Cδ records: still present in WAL. flusher will re-upsert; S3
         // makes this idempotent because record.fence is unchanged.

Step 3   [runTopologySubscription resumes]
         N subscribes to Watchdog. First envelope received.
         L.gen ← envelope.gen (likely > G_p if there was a primary change during
         the outage). PurgeBelowGeneration runs (see 6.3 Step 10).
         // Cβ records belonging to a stale fence will NOT be purged because the
         // purge skips WALState=Prepared (store.go:249-251). They remain orphaned;
         // they cannot reach MySQL (isFlushable=false) and cannot be reused (the
         // next write to the same key overwrites them via appendDirty).
         //   → S2 protected. Weakness #1 in §8: orphan retention.

Step 4   [next write to same key, if any]
         appendDirty (store.go:600-636) overwrites the prior bbolt entry with a
         fresh prepared one (the conflict check at store.go:611 only preserves
         current if shouldPreserveDirtyRecord, which is false for a Prepared
         incoming over a Prepared current). Old WALSeq is gone.
         // Garbage-collects Cβ orphan iff this key sees another write.
```

**Conclusion for 6.4**: No invariants are violated by any crash position. S1 is vacuously satisfied for unacknowledged writes (Cα, Cβ — client never got ack). S2 + S3 hold for Cγ, Cδ via flusher resumption + idempotent fenced upsert.

---

## §7 Counterexample-Driven Proofs — 13 Failure Scenarios

The 13 scenarios are stated verbatim from `docs/production-robustness-plan.md §"Failure Scenarios To Test"`. For each:

- **Threat**: which invariant could be violated?
- **Counterexample trace**: minimal sequence of events that would cause the violation if no defense existed.
- **Defense**: code line(s) that block the trace.
- **Residual risk**: edge cases not closed by current code (forwarded to §8 if material).

### F1 — Primary Watchdog loses the K8s Lease while topology streams are active

**Threat**: S5 (active leases tied to non-current PRIMARY).

**Trace**:
1. `W₁` running PRIMARY, gen `G₁`. Node N has a healthy stream with `L_N.gen = G₁`.
2. K8s revokes `W₁`'s Lease (network partition, Pod eviction, etc.).
3. `W₁` keeps its in-memory `cfg.Role = PRIMARY` for some interval.
4. New `W₂` wins lease, allocates `G₂ = G₁ + 1`, starts serving.
5. `W₁` continues to broadcast envelopes with `gen=G₁` to N — N now sees envelopes from BOTH primaries.

**Defense**:
- `OnStoppedLeading` callback (`cmd/watchdog/main.go:138-141`) calls `cfg.terminate(1)` which exits the process. The standalone path can't deadlock because there's no leader-election machinery to begin with.
- Before exit, `health.SetLeadership(STANDBY, 0)` flips gRPC health to NOT_SERVING, so the K8s Service stops routing new connections to `W₁` even before its process dies.
- `ReleaseOnCancel: true` (`cmd/watchdog/main.go:131`) makes `W₁` voluntarily release the Lease so `W₂` can acquire it without waiting for full TTL expiry.
- Even if `W₁` lingered before exit and managed to broadcast a stale envelope, N1 in `LeaseTracker.Apply` rejects `gen < L.gen` (`subscriber.go:182`). Once N has accepted `W₂`'s `G₂`, an old envelope from `W₁` with `G₁` is dropped.

**Residual**: between K8s revoking the Lease and `W₁` observing `OnStoppedLeading`, there is a window (≤ `LeaseDuration`, default 15 s) during which `W₁` may still send envelopes. As long as `W₂` has not yet started serving, the system is safe (still one PRIMARY in N's view). When `W₂` does start, S5 monotonicity holds via N1.

### F2 — New primary starts after node has seen a higher generation from a clock-skewed old process

**Threat**: S5 (PRIMARY's gen not actually monotonic if old process used wall-clock).

**Trace**:
1. Standalone-mode `W₁` (no Kubernetes) initialized `cfg.Generation` from `time.Now().UnixNano()` (`topology/service.go:54`).
2. `W₁` advertises `gen=10²⁰` (in nanos).
3. `W₁` dies. `W₂` starts in K8s mode and asks `epoch_store.NextGeneration(stack)`.
4. ConfigMap is empty → `NextGeneration` returns 1 (`epoch_store.go:99,107`).
5. `W₂` broadcasts `gen=1`. Node sees 1 < 10²⁰, rejects with `ErrStaleWatchdogGeneration`.

**Defense**:
- The standalone path is documented as non-production (`README.md` "in Kubernetes leader-election mode this is allocated from stack-local persistent state; standalone mode falls back to process start timestamp").
- In K8s, `allocateWatchdogGeneration` in `cmd/watchdog/main.go:250-260` always goes through `ConfigMapEpochStore.NextGeneration`. Generation is therefore single-sourced, persistent, monotonic.
- Mixed mode (some primaries standalone, others K8s) is excluded by deployment design — the operator only deploys K8s-mode Watchdogs.

**Residual**: if an operator manually upgrades a standalone deployment to K8s without truncating the on-disk state, F2 is real. The `allocateWatchdogGeneration` function should arguably check that the persisted value is reachable; currently it returns `1` on a fresh ConfigMap. Forwarded to §8.6 (low-priority hardening).

### F3 — Epoch ConfigMap writes fail while membership is changing

**Threat**: S4 (envelopes delivered with non-durable epoch).

**Trace**:
1. `W` is PRIMARY. Membership change occurs → `pruneExpiredLocked` runs → `broadcastLocked`.
2. `saveEpochLocked` fails (transient API server error / quota / NetworkPolicy).
3. Old code paths: envelope still sent. Subscribers update `L.epoch` to a value not durable in ConfigMap.
4. `W` crashes/loses lease. `W'` starts, reads ConfigMap → epoch is the old (lower) value.
5. `W'` broadcasts envelope with `epoch < L.epoch` of nodes that saw the lost envelope. Nodes reject with `ErrStaleTopologyEpoch` → cluster deadlock.

**Defense**:
- `broadcastLocked` calls `saveEpochLocked` and **early-returns on error** (`internal/topology/service.go:358-364`). Envelope is not sent.
- `setEpochError` flips `epochErr`. `RunReaper` (`service.go:119`), `RunBookworm` (`service.go:137`), `sendLatestLocked` (`service.go:374`) all skip work while degraded.
- `LeadershipHealth` observers receive `SetDegraded(err)` (`service.go:597-602`), gRPC health flips to NOT_SERVING (`watchdog/health.go:46`).
- `watchEpochHealth` accumulates consecutive failures; at threshold (`WATCHDOG_EPOCH_FAILURE_THRESHOLD`, default 3 polls × 5 s = 15 s) calls `cfg.terminate(1)` → Pod restarts → standby promotes (`cmd/watchdog/main.go:210-235`).

**Residual**: while degraded, membership changes are invisible to nodes. Nodes rely on `expireAfter` (default 30 s) and `leaseTTL` (default 30 s) to fail closed — both ≥ the 15 s self-kill window, so writes do not silently succeed against a removed peer. Coupling is tight; documented as Weakness #3 in §8.

### F4 — Node loses Watchdog connectivity but continues receiving client writes

**Threat**: S5 (writes accepted past lease validity).

**Trace**:
1. N has valid lease, `L.validUntil = t₀ + 30 s`.
2. Network partition between N and W. Heartbeats never arrive → W reaps N at t₀ + 30 s.
3. N never sees a fresh envelope. `validUntil` does not advance.
4. Client connects directly to N (e.g., via Service DNS that did not yet remove this Pod). Calls SET.
5. If N served the write, S5 violated.

**Defense**:
- `stringkv.Service.requireLease` calls `lease.ServingAllowed(now())` which compares `validUntil > now().UnixMilli()` (`service.go:108`, `subscriber.go:208`).
- After `t₀ + 30 s` the predicate is false → `ErrLeaseExpired` returned to client.
- Even if the request races past gate #1 into the fork, `BeforeSet` performs a second `SnapshotForWrite` inside the fragment lock (`durable_hook.go:185-194`) which fails the same predicate.

**Residual**: there is a small window between `validUntil` and the client receiving the rejection where the client's request is in-flight. This is not a safety issue (the request fails closed), but operators should be aware reads are also gated (`stringkv.Service.Get` calls `requireLease`, `service.go:52`). For "stale read OK" use cases the API would need to expose a separate `GetStale`. Forwarded to §8 as a future-work note.

### F5 — MySQL is unavailable for longer than WAL capacity can absorb

**Threat**: S1 (acks given without durable record).

**Trace**:
1. MySQL is down. Flusher fails, `s.workerErr` set, `nextFlushAt` deferred.
2. Writes continue. Each calls `appendDirty` (`store.go:600`).
3. `dirty.Stats().KeyN >= QueueSize` (default 1024) → `appendDirty` returns `ErrQueueFull` (`store.go:621`).
4. `PrepareEntry` propagates `ErrQueueFull` upward → `BeforeSet` returns it → fork's `putOnCluster` returns it → client gets an error.

**Defense**:
- `ErrQueueFull` is the explicit backpressure signal (`store.go:23`).
- The fork's `BeforeSet` failure path skips `putOnClusterAfterDurable` entirely (`put.go:348-352` — `if err … return err`), so no in-memory mutation happens, no replicas are touched, and no `AfterSet` call occurs (no phantom prepared record to leak).
- After MySQL recovers, the next flusher tick clears the WAL and queue capacity returns.

**Residual**: the cluster is in soft-degraded state (returning errors) for the duration of the outage past WAL capacity. This is correct behavior per the agreed contract (`docs/production-robustness-plan.md §"Persistence"`: "When the WAL dirty set exceeds threshold, directly trigger 503"). No invariant is at risk. Operators may want a more graceful degradation curve (separate read/write modes), but that's a product question.

### F6 — Pod IP is reused while an old heartbeat stream or stale Pod observation exists

**Threat**: S5 (a stale stream's identity getting accepted as a current member).

**Trace**:
1. Pod `olric-0` had IP `10.1.0.5`. It crashed; K8s reuses IP for a different stack's Pod (or for `olric-0` rebooted with a new incarnation).
2. The old heartbeat stream is still alive on the gRPC server side (TCP keepalive grace).
3. Watchdog reconciler observes a Pod with IP `10.1.0.5` but a different name/UID.

**Defense**:
- `ApplyPodObservations` matches by `PodName` first, only falling back to `PodIP` when `PodName == ""` (`internal/watchdog/state.go:142-146`). A reused IP under a different `PodName` is treated as an unrelated Pod, not as a state change for the original member.
- Heartbeat-side: each heartbeat carries `node_id` + `incarnation`. `registerHeartbeat` validates identity stability per stream (`service.go:232`). A new process restarts with a fresh `incarnation` (`cmd/olric-node/main.go:193` defaults to `time.Now().UnixNano()`).
- `ApplyHeartbeat` (`state.go:92-119`): a smaller `incarnation` is rejected as stale; a larger one rebuilds the lifecycle FSM. So a stale stream with old `incarnation` cannot mask the new process's heartbeats.
- The old Pod's stream eventually fails on the next `stream.Recv()` (TCP RST or keepalive timeout) → `Watch` returns → subscriber unregistered.

**Residual**: in the brief window between `incarnation` reuse and the old stream tearing down, there could be two streams claiming the same `node_id`. `registerHeartbeat` calls `closeSubscriber(current)` if a different `sub` has the slot (`service.go:276-278`), so the new stream wins.

### F7 — Node pauses longer than `EXPIRE_AFTER` and then resumes with the same incarnation

**Threat**: S5 (resurrected node serving stale state).

**Trace**:
1. N is heartbeating with `incarnation = I`. Process pauses (GC, OS swap, hypervisor stall) for > 30 s.
2. Watchdog prunes N at `EXPIRE_AFTER` → membership FSM transitions to `UNREACHABLE` → `REMOVED` → deleted from `state.nodes`. Epoch increments. Other nodes get an envelope without N.
3. Lease validity at N: process was paused → time effectively didn't pass for N's view, but `validUntil` is wall-clock — when N resumes, `time.Now()` jumps forward → lease has already expired.
4. N resumes. Subscriber.Run continues to attempt `stream.Send` heartbeats; gRPC stream is still attached (server side rejected the next message).
5. Client lands on N before reconnect.

**Defense**:
- `requireLease` uses wall-clock; lease is expired → write rejected (S5 holds, F4 defense).
- N's heartbeat side: `stream.Send` likely returns an error (the gRPC server-side stream context was canceled when N was reaped). `subscribeOnce` returns the error, outer loop reconnects.
- On reconnect: `validateHeartbeat` passes (incarnation > 0). `registerHeartbeat` calls `state.cluster.ApplyHeartbeat`. N is not in `state.nodes` → fresh insert → epoch++ → `broadcastLocked(EVENT)`. This is a normal rejoin path; `incarnation = I` is fine because it is greater than the absence of any record.
- Even if the same `I` is reused, the lifecycle FSM rebuilds via `setLifecycle` (`state.go:79-87`), starting in `READY`.

**Residual**: there is no defense against N having arbitrary stale **memory** — but memory is not durable and the durable contract is MySQL. The next client `GET` either reads the in-memory value (acceptable for the read-through contract — `Get` is gated by lease, lease is expired so read is rejected) or, after reconnect, may read a stale entry briefly. The fork's `LoadOnMiss` re-checks under the fragment lock (§6.2 Step 6) so a concurrent fresher write wins. No invariant violation, but a brief stale-read window during reconnect is operationally visible.

### F8 — Multiple nodes attempt to flush the same key with different owner versions

**Threat**: S3 (newer fence overwritten by older).

**Trace**:
1. Owner of partition P moves from N₁ to N₂ during membership change. Both nodes hold dirty WAL records for `(dmap, hkey)` from successive client writes.
2. N₁ fence: `(G_p, E_p, S_a)`. N₂ fence: `(G_p, E_p+1, S_b)` (after epoch bump from owner change).
3. Both flush concurrently. Without fence-aware upsert, N₁'s row could land second and overwrite N₂'s.

**Defense**:
- `versionedUpsertClause` (`store.go:799`). MySQL `ON DUPLICATE KEY UPDATE … CASE WHEN <newer> THEN VALUES(...) ELSE <self> END` is row-level atomic in InnoDB. So whichever batch lands second still loses on every column if its fence is smaller.
- `<newer>` predicate is strict lex on `(G, E, S)` (`store.go:801-803`).
- N₁ and N₂ stamp from their own `OwnerSequence`, but the comparison happens at MySQL row scope, so per-key ordering is global.

**Residual**: this is the core S3 protection. As long as the fence triple is computed correctly, no scenario violates S3. The risk concentrates on whether the fence is *correctly populated*, not on the MySQL clause. F11 / F13 look at that side.

### F9 — WAL append succeeds but Olric owner mutation fails

**Threat**: S1 (durable record appears for a write that the client thinks failed) and the dual: ack given but record is not Committed.

**Trace**:
1. `BeforeSet` succeeds → `Prepared` record at `WALSeq=K`.
2. `putOnClusterAfterDurable` fails (quorum loss, replica unavailable, fragment migration race).
3. Without an unconditional `AfterSet`, the record stays `Prepared` → never reaches MySQL (good for one direction) but also never gets cleaned up.
4. Worse: if `AfterSet` were called only on success, a future call with the same `WALSeq` could succeed and accidentally commit a record from a failed write.

**Defense**:
- The fork calls `AfterSet` **unconditionally** (`put.go:354-367` and matching `delete.go:168-176`). Comment explicitly states "AfterX MUST run regardless of mutErr".
- The hook's `finalize` routes to Abort on `mutErr ≠ nil` and Commit on `mutErr = nil` (`durable_hook.go:197-213`).
- Abort uses `context.Background()` so even if the request context is canceled, cleanup happens (`durable_hook.go:206-209`).
- `AbortEntry` is bounded: WALSeq mismatch ⇒ no-op; Committed state ⇒ rejected (`store.go:683-688`). It cannot accidentally delete a committed record from a successful prior write that re-prepared the same key.
- This is the FT-1/FT-2 scenario from `docs/durable-string-kv-design.md`. Tested by `TestDurableHookEndToEndAbortPreventsMySQLLeak` (`internal/stringkv/durable_hook_e2e_test.go:23`).

**Residual**: see Weakness #1 in §8 — orphan `Prepared` records if the process crashes between E9 and E12.

### F10 — MySQL read-through races with a concurrent owner write

**Threat**: S3 (refill overwrites a fresher in-memory value, indirectly causing a smaller fence to look like the truth on this owner; MySQL's S3 is unaffected, but the in-memory→MySQL flush could lose a write).

**Trace**:
1. N is owner. Client SET arrives, takes fragment lock, prepares WAL with fence `(G, E, S+1)`, mutates memory, commits WAL. Lock released. Flusher tick is later.
2. Concurrently: a GET on the same key found `frag.mem[hkey]` empty (entry was LRU-evicted, then refilled but evicted again before the SET). It calls `LoadOnMiss` which loads the previous MySQL row with fence `(G, E, S)`. Without the re-lock, GET would put `(G, E, S)`'s entry into memory, overwriting the just-mutated `(G, E, S+1)` value.
3. Subsequent flusher reads `frag.mem` (NO — flusher reads `wal.dirty`, not memory). So MySQL is fine. But subsequent client GET on N could now see the stale value until LRU evicts again or the flusher runs and someone re-reads MySQL.

**Defense**:
- `Engine.Get` is the in-memory read; it doesn't re-load MySQL by itself.
- The fork's `Get` re-locks the fragment after `LoadOnMiss` and re-checks `f.storage.Get(hkey)` (`get.go:399-417`). If a concurrent SET landed (took the lock between LoadOnMiss's start and the re-lock), `existing` is returned; the LoadOnMiss output is discarded.
- The flusher reads `wal.dirty` exclusively (`store.go:732`), not memory. The committed record at `(G, E, S+1)` reaches MySQL via the normal flush path.

**Residual**: the re-check is a critical-section protected by `f.Lock()`. If LoadOnMiss were modified to also write to MySQL or WAL, that would break the design. Currently `LoadOnMiss` only reads — `committer.LoadEntry` is read-only, no `StoreEntry`/`PrepareEntry`/etc. So far so good. Add a regression check: any future change to `LoadOnMiss` must preserve the read-only contract.

### F11 — A local-only refill races with a flushable dirty WAL record

**Threat**: S2 (a `Local` refill record could mask or replace a `Committed` flushable record before the flusher sees the latter).

**Trace**:
1. WAL has `dirty[ref] = R_committed` with fence `(G, E, S+1)`, `WALState=Committed`, `FlushMySQL=true`.
2. A `mysql_refill` operation tries to write to `dirty[ref]` with `WALState=Local`, `FlushMySQL=false`. Without protection it would overwrite `R_committed` → MySQL never sees the committed write → S2 reachable but incorrectly empty.

**Defense**:
- `appendDirty` calls `shouldPreserveDirtyRecord(current, incoming)` (`store.go:614-619`).
- `shouldPreserveDirtyRecord` returns true iff `incoming` is local refill AND `current` is flushable-or-prepared (`store.go:861`).
- When true, `entry.Record = current.Record.Clone()` — i.e., the dirty bucket keeps its committed record; the local refill is silently swallowed (which is semantically correct: refill is purely a memory hint).

**Residual**: the comment doc in `store.go:861` could be clearer. Not a code bug. Note however that **WAL replay does not re-create a Local record** because Replay writes to the engine's in-memory store, not to WAL (see §6.4 Step 2). So Local records cannot accumulate on restart unless something explicitly puts them there — `LoadOnMiss` does NOT call `StoreEntry`; the actual refill is `putEntryOnFragment` (in-memory only). Therefore the only producers of `Local` records are paths that explicitly set `WALStateLocal`, which the current code does only inside `StoreEntry` when `FlushMySQL=false` (`store.go:355`). That path is not currently exercised by any production code — `stringkv.DurableHook` always sets `FlushMySQL=true` for client writes and never calls `StoreEntry` at all. So in the present revision F11 is *prevented* but largely *defended against a non-existent producer*. Worth keeping the defense for forward compatibility.

### F12 — A previous owner flushes after a newer owner has accepted writes

**Threat**: S3 (delayed cross-owner flush overwrites newer state).

**Trace**:
1. Partition P has owner N₁ at gen `G₁`. N₁ writes, gets `(G₁, E₁, S₁)` durable record, but flusher is paused (network blip to MySQL).
2. Cluster reshuffles. Owner P moves to N₂ at gen `G₁` epoch `E₂ > E₁`.
3. N₂ takes a client write at fence `(G₁, E₂, S₂)`. Flushes successfully. MySQL row now `(G₁, E₂, S₂)`.
4. N₁'s flusher resumes and emits its old fence `(G₁, E₁, S₁)`.
5. Without S3, MySQL would regress. With S3, the upsert clause sees `(G₁, E₁, S₁) < (G₁, E₂, S₂)` → no column updated → silent skip.

**Defense**:
- `versionedUpsertClause` strict lex on `(G, E, S)`. The smaller fence loses every column comparison. MySQL row stays at `(G₁, E₂, S₂)`.
- Independent guard: `PurgeBelowGeneration` (`store.go:224`) deletes WAL records with `fence.G < envelope.gen` after a gen bump. If the move from N₁ → N₂ is accompanied by a gen bump (typical: the move corresponds to a watchdog primary change), N₁'s stale records are purged before its flusher runs.

**Residual**: epoch-only changes (same gen, larger epoch) do NOT trigger purge — only generation comparisons. So in the trace above, the purge does not fire, but S3 in the upsert clause still defends. This is correct: the fence triple is the source of truth, not the cleanup helper.

A subtle hazard: if `OwnerSequence.Stamp` is called with `(G₁, E₂)` on N₂ and `(G₁, E₂)` later returns to N₁ (highly unusual but possible during epoch ping-pong), each owner has its own sequence counter for `(G₁, E₂)`. Two writes could end up with the same `S` from different processes. `versionedUpsertClause` has no equal-fence tiebreaker — the second to land wins by virtue of `>` failing for both, falling through to the existing row. So we have last-MySQL-write-wins-when-fence-equal semantics. **This is a S3 corner case** (see §8.5 — equal-fence MySQL conflict, currently relies on cluster never producing two simultaneous owners under same `(G, E)`).

### F13 — Fragment migration occurs while dirty WAL records are unflushed

**Threat**: S1 (acked write effectively lost — committed in old owner's WAL but new owner does not have the record; if old owner's bbolt is destroyed the record is gone before MySQL flush).

**Trace**:
1. N₁ owner of P. Client SET acked → committed WAL record fence `(G, E, S)`.
2. Migration moves P to N₂ before flusher runs.
3. N₁ no longer owns the partition; it stops accepting client writes. But its bbolt WAL still contains the committed record.
4. If N₁ shuts down or rebalances WAL purging (e.g., a future cleanup), the record could be lost before flush.
5. Client thinks the write succeeded; MySQL has nothing; new GET on N₂ misses, LoadOnMiss returns ErrKeyNotFound → S1 violated.

**Defense**:
- **Currently incomplete**, acknowledged in `docs/production-robustness-plan.md §"Phase 4: Durable Ownership Handoff"` as Known Production Gap #5. There is no migration handoff for dirty WAL records.
- **Partial protection**: N₁ keeps its bbolt WAL across membership changes — there is no automatic clear on migration. The flusher continues running on N₁ and will drain to MySQL on its own schedule. So provided N₁ stays alive long enough for one flush tick, the record reaches MySQL even after migration.
- **Failure mode**: if N₁ restarts AND its WAL is on ephemeral storage, the record is lost. The Operator deploys Olric as a StatefulSet with PVCs (`internal/workloads/workloads.go` builds `VolumeClaimTemplates`), so this is mitigated in production-grade deployments.

**Residual**: this is the biggest known production gap. Forwarded to §8.6 (fragment migration WAL handoff). The §9 TLA+ migration list has this scenario flagged for explicit modeling once Phase 4 is implemented.

---

## §8 Identified Weaknesses

Each weakness is graded **Severity** (impact on safety / liveness if exploited), **Detectability** (how visible to operators), and **Effort** (rough hardening cost).

### 8.1 Orphan `Prepared` WAL records after process crash mid-mutation

- **Severity**: Low. Records cannot reach MySQL (`isFlushable`), cannot be reused by Replay, and are overwritten on the next write to the same key. No invariant violated.
- **Detectability**: Low. There is no metric for `WALStatePrepared` count. Bbolt growth is the only indirect signal.
- **Trace**: §6.4 Cβ. Process dies after `PrepareEntry` (`store.go:600`) but before `CommitEntry` or `AbortEntry` (`store.go:639`/`670`).
- **Defenses already in place**: `Replay` skips Prepared (`store.go:283`), `PurgeBelowGeneration` skips Prepared (`store.go:249`), `isFlushable` rejects Prepared (`store.go:851`).
- **Hardening**:
  1. **Boot-time sweep**: at `MySQLStore.Start`, walk dirty bucket once and Abort all `Prepared` records older than some retention (their fragment was never reached by `AfterSet` for this process; safe to drop).
  2. **Metric**: expose `wal_prepared_count` so operators can alert on stuck preparations.
  3. **TTL on Prepared**: stamp `PreparedAt` and reject Commit/Abort past TTL. Risky — would have to tie into request timeout.
- **Recommendation**: implement #1 + #2. #3 introduces failure modes worth more than the marginal safety win.

### 8.2 50 ms drain window in `closeSubscribersForLeadershipChange`

- **Severity**: Low for safety (S5 holds via leaseTTL fallback). Medium for liveness — the user-facing demote latency could swing from ~50 ms to `leaseTTL` (default 30 s) if the fast path is missed.
- **Detectability**: Medium. Visible as elongated tail latency on the next client write after a primary change.
- **Trace**: `internal/topology/service.go:528-530`. `time.Sleep(50ms)` between channel push and `close(sub.done)`. If the receiver goroutine is descheduled during this window, the standby envelope may not flush to the wire before `done` is closed and `Watch` returns.
- **Defenses in place**:
  - `sub.ch` is buffered (capacity 8) and we drain it before posting standby (`service.go:513-516`), so the standby envelope is the only thing in the channel.
  - The actual stream `Send` happens in the `Watch` for-loop (`service.go:217-220`), which is a separate goroutine from the closer.
- **Hardening**:
  1. Replace `Sleep(50ms)` with explicit per-subscriber acknowledgement: track which `sub.ch` was drained-by-Watch via a confirmation channel.
  2. Or: increase `sub.ch` capacity is irrelevant; the issue is scheduling, not capacity.
  3. Or: send standby envelope **before** demoting the role (so it gets through under PRIMARY auth path), then close. The current code already sends before close; the gap is the goroutine race.
- **Recommendation**: #1, but trade off complexity. Acceptable to leave as-is until benchmarks show the fast path failing.

### 8.3 Coupling between `epochErr` self-kill threshold and lease/expire windows

- **Severity**: Medium. While epoch persistence is degraded, membership changes are invisible to nodes for up to ~15 s (default threshold × interval). Lease and `expireAfter` are configured to absorb this, but the windows are tightly coupled — changing one without the other could open a real safety gap.
- **Detectability**: High. `WATCHDOG_EPOCH_FAILURE_THRESHOLD` and `WATCHDOG_EPOCH_HEALTH_INTERVAL` are explicit envs; gRPC health flips to NOT_SERVING.
- **Defenses in place**: §7 F3 chain.
- **Hardening**:
  1. **Document the coupling** explicitly: `threshold * interval ≤ min(leaseTTL, expireAfter) - safetyBudget`. Add a startup-time validation in `cmd/watchdog/main.go` that fails fast if envs violate this.
  2. **Fail readiness immediately** on first epoch error rather than counting failures. Trade off: transient ConfigMap blips would cause leadership churn. The current 3-strikes design absorbs them; a stricter policy would not.
- **Recommendation**: #1. Self-kill is already a strong signal; the validation closes the foot-gun at deployment.

### 8.4 Standalone-mode generation source

- **Severity**: Low (standalone is documented dev-only).
- **Trace**: §7 F2. `topology.Config.withDefaults` at `service.go:54` uses `time.Now().UnixNano()`. If a stack is migrated from standalone to K8s with persisted state, the K8s generation can be smaller than node-cached `L.gen`.
- **Hardening**: refuse to serve in standalone unless an explicit env (`OLRIC_STANDALONE=1`) is set, AND log a loud warning. Or remove standalone entirely once K8s test infrastructure is consolidated.
- **Recommendation**: gate behind explicit env; cost is one if-check.

### 8.5 Equal-fence MySQL conflict has no tiebreaker

- **Severity**: Low under current invariants (cluster never produces two simultaneous owners under same `(G, E)`), Medium if F12's epoch ping-pong scenario becomes real.
- **Trace**: §7 F12 residual. `versionedUpsertClause` lex strict `>`; equal triples fall through to "keep existing", giving last-MySQL-batch-wins semantics for equal fences.
- **Defenses in place**: depends on cluster model not generating two writers at the same `(G, E)`.
- **Hardening**:
  1. Add a final tiebreaker column (e.g., monotonic node UUID) to the comparator.
  2. Prove the no-equal-fence invariant formally: `Stamp` only ever returns `(G, E, S)` with strict `S` increment per `(G, E)` per node, but two nodes with the same `(G, E)` can independently produce `S=1`.
- **Recommendation**: #1 — add a `writer_id` column, include in comparator. ~30 min change. Note `writer_id` is already in the schema design (`docs/durable-string-kv-design.md §"MySQL Record"`) but not yet in the upsert comparator.

### 8.6 Fragment migration WAL handoff not implemented (Phase 4)

- **Severity**: High in pathological deployments (ephemeral WAL volume + rapid migration), Low in StatefulSet-with-PVC default deployments.
- **Trace**: §7 F13. No mechanism to flush old-owner committed WAL before handoff or transfer WAL state with the fragment.
- **Defenses in place**: PVC-backed WAL persists across Pod restart; flusher resumes after restart and drains to MySQL.
- **Hardening**: implement Phase 4 from `docs/production-robustness-plan.md`. Three options listed there; "force new owner to create equivalent durable checkpoint before becoming write-ready" is the simplest to bolt on.
- **Recommendation**: this is the only weakness here that could cause silent data loss under expected deployment shapes. Prioritize for the next hardening sprint.

### 8.7 No singleflight for concurrent `LoadOnMiss` on the same key

- **Severity**: Low (correctness preserved by re-lock + re-check), Medium for throughput under hot-key cache stampede.
- **Trace**: 100 concurrent GETs on the same evicted key all run their own `MySQLStore.LoadEntry` → MySQL receives 100 identical row reads. `Engine` then does 100 `f.Lock()`s, each re-checking and 99 of them dropping their result.
- **Defenses in place**: re-lock + re-check (`get.go:399-417`). No correctness issue.
- **Hardening**: wrap `LoadOnMiss` in `golang.org/x/sync/singleflight` keyed by `(dmap, hkey)`. Deduplicates concurrent loads.
- **Recommendation**: cheap (~20 lines) and improves observable cold-start latency. Listed as a Phase 3 task in the production plan; not urgent.

### 8.8 No backpressure metrics

- **Severity**: Low for correctness, High for ops.
- **Trace**: WAL depth, prepared/committed ratios, flusher lag, last-flush time, queue full events — none are exposed. Operators cannot diagnose F5 outages until clients start failing.
- **Hardening**: add Prometheus metrics. Phase 5 in the production plan.

### 8.9 OwnerSequence post-restart seq gap (introduced by Optimization #2)

- **Severity**: Low. Strict-lex MySQL upsert is gap-tolerant by design.
- **Trace**: under reservation window N, a crash can lose up to N-1 unused seq values per `(G, E)`. The next Stamp resumes at `reservedHigh+1`.
- **Impact on invariants**: zero — S3 requires only strict lex monotonicity, never contiguity. S6 requires only that no two records share a fence triple, which the new persistence model still guarantees because every issued seq is < reservedHigh and reservedHigh is durable before issue.
- **Observable effect**: row `owner_seq` column in MySQL has holes. Audit tooling that assumes contiguous sequences (e.g., "count writes per primary") must be updated to use `MAX(owner_seq) - MIN(owner_seq)` instead of `COUNT(*)`.

### 8.10 VerifyAfterLock widens the durable-hook interface (introduced by Optimization #4)

- **Severity**: Low. The new method has a default no-op for hooks that don't perform out-of-lock prepare.
- **Trace**: any third-party Olric fork user implementing `config.DurableHook` will fail to compile until they add `VerifyAfterLock`. This is a breaking interface change scoped to our internal fork; no external consumer exists.
- **Recommendation**: when rebasing Olric onto a newer upstream, keep the interface addition in `config/durable.go` co-located with the rest of the durable-hook surface so the rebase diff stays minimal.

---

## §9 TLA+ Migration Roadmap and Test Gaps

This document covers timing correctness via state-machine reasoning. To upgrade specific invariants to machine-checked TLA+ models, prioritize as follows:

### 9.1 First TLA+ targets

**S3 + S5 + S6 fence monotonicity**

- Modeling target: 1 watchdog (with primary/standby cycles), N nodes, per-key fence triple ledger, MySQL upsert as a function over fences.
- Predicates to check: `[]<>FenceMonotonic`, `[]<>NoFenceRollback`.
- Effort: 2-3 days. The model is small; the value is high (this is the heart of the durable contract).

**S4 envelope durability under W₁→W₂ transitions**

- Model: ConfigMap as a single shared variable; primary as a process that may fail at any point in {pre-save, mid-save, post-save, mid-broadcast}; subscribers that update L.gen/L.epoch on receipt.
- Check: `[](∀ N : N.L.epoch ≤ ConfigMap.epoch)`.
- Effort: 1-2 days. Small but instructive.

### 9.2 Deferred until Phase 4 lands

- F13 / Weakness 8.6 (fragment migration handoff): no point modeling in TLA+ until the protocol exists. Once the design is decided, model the handoff and check S1 holds across migration.

### 9.3 Test gap analysis vs. §6/§7 coverage

| Scenario | Current test | Gap |
|---|---|---|
| 6.1 Happy SET | `TestDurableHookEndToEndAbortPreventsMySQLLeak` (commit path), `internal/e2e/host_mysql_stack_test.go` | none |
| 6.2 GET miss read-through | covered by host e2e | concurrent SET + LoadOnMiss race not stress-tested |
| 6.3 Demote→promote | `TestHostOnlyWatchdogDemotionBlocksAndPromotionRestoresWrites` | does not exercise the 50 ms drain edge case |
| 6.4 Crash recovery | `internal/store/store_test.go` Replay tests | no test for orphan `Prepared` records (Weakness 8.1) |
| F1 PRIMARY loses Lease | host-only failover tests | covered |
| F2 Clock-skewed gen | none | gap — add a test that explicitly seeds a higher generation and verifies new K8s primary is rejected (or the suggested 8.4 hardening) |
| F3 Epoch CM failure | unit tests in `internal/topology/service_test.go` | covered for early-return; lacks self-kill timing |
| F4 Network partition | none | gap — host test with iptables drop on watchdog connection |
| F5 MySQL outage past WAL cap | `internal/store/store_test.go` queue full tests | covered for `ErrQueueFull`; lacks recovery-after-MySQL-comes-back |
| F6 Pod IP reuse | `internal/watchdog/state_test.go` | covered |
| F7 Long pause | none | gap — simulate paused process via clock skew or stop-the-world |
| F8 Concurrent flush diff fences | `internal/store/store_test.go` versioned upsert tests | covered |
| F9 Mutation fail post-prepare | `TestDurableHookEndToEndAbortPreventsMySQLLeak` | covered |
| F10 LoadOnMiss vs concurrent SET | unit fork test | gap — need stress concurrency test |
| F11 Local refill vs flushable | `internal/store/store_test.go` shouldPreserveDirtyRecord | covered (defended against non-existent producer) |
| F12 Previous owner delayed flush | `internal/store/store_test.go` | covered for fence comparison; F12 residual (equal fence) not covered |
| F13 Fragment migration | none | gap — Phase 4 prerequisite |

**High-priority gaps to close**: F4, F7, F10, F13 (after Phase 4).

### 9.4 Re-validation procedure when code changes

When any of the following files change, re-walk the impacted §6/§7 sections:

- `internal/topology/service.go` → §6.3, F1, F3
- `internal/node/{subscriber,gated_store,owner_seq}.go` → §6 all, F4, F6, F7
- `internal/store/store.go` → §6.1, §6.4, F5, F8, F9, F11, F12
- `internal/stringkv/durable_hook.go` → §6.1, §6.2, F9, F10
- `third_party/olric/internal/dmap/{put,delete,get}.go` → §6 all, F9, F10
- `internal/watchdog/{controller,state,epoch_store}.go` → F1, F3, F6

Each section ends with a "code anchors" implicit list via the `path:line` cites; running `git blame` on any cite that no longer exists is the trigger for re-validation.

---

*End of document.*
