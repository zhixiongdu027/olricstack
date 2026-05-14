# Durable String KV Design

This document defines the target data-plane design for OlricStack after narrowing
the product contract to a string key-value API. It supersedes designs that wire
MySQL load directly into the generic Olric storage engine.

## Product Boundary

OlricStack provides a MySQL-backed durable string KV service. Olric remains the
distributed in-memory engine that owns routing, partition ownership, backup
replication, read-repair, migration, and eviction. MySQL is the source of truth
for durable string KV state in durable modes, but MySQL must not participate in
Olric quorum, backup ownership, read-repair, or routing decisions.

The supported business API is intentionally narrow:

- `GET dmap key`
- `SET dmap key string_value`
- `DEL dmap key`
- `EXPIRE dmap key ttl`

Full native Olric DMap semantics are not part of the durable MySQL contract. If
the native Olric protocol remains exposed for development or compatibility, those
paths must be treated as best-effort Olric cache operations unless they are routed
through the string KV business layer described here.

## Working Modes

### 1. Memory Only

Olric is the only state holder.

- No local WAL.
- No MySQL.
- LRU or TTL eviction is business-visible.
- Process or Pod restart may lose data.
- No durable read-through guarantee.

This mode is useful for pure cache deployments and development.

### 2. Evictable Cache

Olric is a cache. MySQL/WAL may be enabled for write-behind durability or audit,
but the business contract accepts cache misses after eviction.

- Local WAL can be used as an ack-before-flush durability boundary.
- MySQL can receive primary-origin writes.
- LRU eviction is business-visible.
- `GET` does not have to refill from MySQL.
- A missing in-memory key can be returned as not found.

This mode is operationally simpler, but it is not a durable read-through KV.

### 3. Durable Read-Through String KV

Olric may still use LRU internally, but LRU eviction is invisible to the business
API. MySQL is the durable source of truth.

- Local WAL is the ack-before-flush durability boundary.
- Client primary `SET`, `DEL`, and `EXPIRE` are persisted to WAL and flushed to
  MySQL.
- Olric LRU may evict an entry from memory.
- A client `GET` miss on the current primary owner may load from MySQL, refill
  Olric, and return the value.
- Delete tombstones and TTL metadata are persisted in MySQL.

Only this mode provides the "secretly persist to MySQL and refill on read" user
experience.

## Core Safety Invariants

1. MySQL must not be a transparent Olric replica.
2. MySQL load must not be performed by generic storage-engine `Get`.
3. MySQL load may only happen in the current primary owner's business `GET` miss
   path.
4. Backup apply, read-repair, migration import, and previous-owner cleanup must
   not flush global MySQL records.
5. Client primary `DEL` must write a tombstone even if the key is not currently
   resident in Olric memory.
6. Client primary `EXPIRE` must persist TTL updates.
7. A node must not serve client reads or writes without a valid serving lease,
   unless the API explicitly exposes stale reads.
8. MySQL conflict resolution is only a final persistence guard. Correctness must
   come from Olric routing and one active primary owner per key.

## Why Generic Storage Engine Hooks Are Insufficient

Olric's storage engine interface sees operations such as `Get(hkey)`,
`Put(hkey, entry)`, `PutRaw(hkey, raw)`, `Delete(hkey)`, and
`UpdateTTL(hkey, entry)`. It does not know enough to decide global persistence
semantics.

The same storage methods are used by multiple paths:

- client primary `GET`
- internal `GetEntry`
- replica lookup
- previous-owner lookup
- read-repair
- backup apply
- fragment migration and merge
- eviction and maintenance checks

Therefore, wiring MySQL read-through into `Engine.Get` can pollute read-repair,
replica reads, previous-owner lookups, and migration with MySQL values. Wiring
MySQL flush into every `Put`/`PutRaw` can make backup owners, read-repair, or
migration writes overwrite the global source of truth.

The storage engine can safely provide local memory and local WAL behavior. Global
MySQL source-of-truth behavior needs operation context from the DMap/business
layer.

## Owner-Side Durable Hook Boundary

The preferred implementation keeps Olric's routing and partition ownership as the
single authority. The client-facing string KV layer receives the request and
enforces the serving lease, but it must not write the WAL directly. The request is
then applied through Olric's DMap API. If the first ingress node is not the
primary owner for `(dmap, key)`, Olric routes the operation to the owner through
its internal protocol.

The WAL/MySQL contract starts only inside owner-side DMap business paths. This is
the boundary that prevents WAL drift on a non-owner ingress node. Native Olric
APIs can still exist for cache/development use, but durable MySQL semantics only
apply to operations that reach these owner-side durable hooks.

Write ordering for durable operations:

1. Validate the topology lease.
2. Apply the operation through Olric's DMap API.
3. Olric routes the operation to the current primary owner.
4. The owner-side durable hook records the operation in local WAL.
5. Olric applies the in-memory mutation and replication/quorum path.
6. Acknowledge the client only after the owner-side path returns success.
7. Flush MySQL asynchronously from WAL.

This ordering still needs a commit protocol. A pre-mutation WAL append can create
a durable record for an Olric mutation that later fails. The production target is
therefore a two-phase WAL state such as `prepared -> committed`, where the
flusher only exports committed records. Until this exists, the implementation is
a correctness prototype rather than a production-complete durable write protocol.

`GET` read-through also belongs in the owner-side miss path. On an owner-side
Olric miss, the durable hook may load MySQL by `(dmap, hkey)`, reject tombstones
or expired records, and refill Olric without marking the refill as
MySQL-flushable. The refill must be conditional so that stale MySQL data cannot
overwrite a concurrent successful write.

## Minimal Olric Fork Boundary

Forking Olric is acceptable, but the fork must avoid algorithm changes. The fork
should only add context and hooks around existing algorithm decisions.

### Required Context

Every storage operation that can affect persistence should carry:

- `dmap`
- `key`
- `hkey`
- `partition_id`
- `fragment_kind`: `primary` or `backup`
- `origin`
- serving/lease validity or a callback to check it

Operation origins:

- `client_set`
- `client_delete`
- `client_expire`
- `backup_apply`
- `read_repair`
- `migration_import`
- `previous_owner_cleanup`
- `mysql_refill`
- `eviction`
- `maintenance`

This context does not alter Olric ownership, quorum, migration, or read-repair
algorithms. It only lets the persistence layer classify side effects correctly.

### Hook Points

The fork should add hooks at the DMap/business layer, not inside low-level storage
`Get`.

Required hooks:

- before/around client primary `SET`
- before/around client primary `DEL`
- before/around client primary `EXPIRE`
- after client primary `GET` receives `ErrKeyNotFound`
- storage operation metadata propagation for backup/read-repair/migration

The fork should keep Olric routing and ownership decisions as the authority. It
should not add MySQL quorum, MySQL backup ownership, owner terms, or watchdog
topology injection into Olric's routing table.

## Persistence Matrix

| Origin | Write Olric memory | Write local WAL | Flush MySQL |
|---|---:|---:|---:|
| Client `SET` on primary | yes | yes | yes |
| Client `DEL` on primary | yes, if resident | yes | yes, tombstone |
| Client `EXPIRE` on primary | yes | yes | yes |
| Backup apply | yes | yes | no |
| Read-repair apply | yes | yes | no |
| Migration import | yes | yes | no |
| Previous-owner cleanup | yes | yes | no |
| MySQL refill | yes | yes | no |
| LRU eviction | delete memory only | optional local marker | no |

Notes:

- Client `DEL` must persist a tombstone even when Olric memory misses.
- MySQL refill must not be flushed back to MySQL, otherwise an old source record
  can generate a newer write-back cycle.
- LRU eviction is not a delete in Durable Read-Through mode.

## Durable Read-Through `GET`

The durable `GET` path is:

1. Client request enters the string KV API.
2. Serving lease is checked.
3. Request is routed to the current Olric primary owner for `(dmap, key)`.
4. Olric performs its normal read path, including quorum/read-repair if enabled.
5. If Olric returns a value, return it.
6. If Olric returns an error other than not found, return that error.
7. If Olric returns not found:
   - confirm this node is still the primary owner;
   - confirm serving lease is still valid;
   - load MySQL by `(dmap, key/hkey)`;
   - if tombstone, expired, or not found, return not found;
   - refill Olric using origin `mysql_refill`;
   - return the string value.

Internal `GetEntry`, replica lookup, previous-owner lookup, read-repair, migration,
and maintenance reads must not execute step 7.

## `SET`, `DEL`, and `EXPIRE`

### SET

Client primary `SET` must:

1. Check serving lease.
2. Enter Olric's normal write path.
3. Route to the current primary owner through Olric if ingress is not the owner.
4. Build a string KV record with value, TTL, timestamp, and tombstone=false in
   the owner-side hook.
5. Append a prepared WAL record before the memory mutation.
6. Commit the WAL record only after the Olric write/quorum path succeeds.
7. Let the asynchronous flusher write committed primary-origin records to MySQL.

### DEL

Client primary `DEL` must:

1. Check serving lease.
2. Enter Olric's normal delete path.
3. Route to the current primary owner through Olric if ingress is not the owner.
4. Append a tombstone WAL record on the owner even if Olric memory misses.
5. Delete from Olric memory and replicas through the normal Olric path if present.
6. Commit and flush the tombstone to MySQL asynchronously.

This prevents MySQL refill from resurrecting a previously deleted key.

### EXPIRE

Client primary `EXPIRE` must:

1. Check serving lease.
2. Enter Olric's normal expire path.
3. Route to the current primary owner through Olric if ingress is not the owner.
4. Persist the TTL update in owner-local WAL only after confirming the key is
   resident.
5. Update Olric TTL.
6. If Olric misses but MySQL has a live key, either persist the new TTL directly
   or refill then update TTL. The behavior must be explicit and tested.

## WAL Model

The local WAL is the ack-before-flush durability boundary. In the current string
KV contract, the WAL may coalesce to the latest operation per `(dmap, hkey)`,
because supported operations are last-write-wins string value updates, tombstones,
and TTL updates.

The WAL must record:

- operation origin;
- operation state (`prepared`, `committed`, `local_refill`, or `tombstone`);
- dmap/key/hkey;
- value for string SET/refill;
- TTL and timestamp;
- tombstone flag;
- local WAL sequence;
- owner fence (`watchdog_generation`, topology epoch, partition ID, owner ID);
- MySQL flush eligibility.

WAL replay should restore only local state that belongs to the local fragment
context. A fork must provide enough fragment context to avoid replaying one
fragment's WAL entries into another fragment.

## MySQL Record

The durable string record should contain:

- `dmap`
- `key`
- `hkey`
- `value`
- `ttl`
- `timestamp`
- `tombstone`
- `version`
- `writer_id`
- `updated_at`

`version` must be derived from owner fencing, not local wall-clock time. The
minimum production fence is:

- stack ID;
- partition ID;
- Watchdog generation;
- topology epoch;
- owner/member incarnation;
- per-owner monotonic write sequence.

MySQL last-write-wins is only valid inside this fenced ordering.

## Serving Lease And Read Safety

The serving lease must gate the client-facing string KV API, not just the backing
store. Otherwise, a demoted or disconnected node can still return memory-resident
dirty values without touching WAL/MySQL.

Minimum policy:

- Client `SET`, `DEL`, and `EXPIRE` require a valid lease.
- Durable client `GET` requires a valid lease.
- Internal Olric traffic should follow Olric's normal algorithm, but must not
  trigger global MySQL load/flush unless explicitly classified as a primary
  business operation.

## Implementation Phases

### Phase 0: Safety Stopgap

- Disable MySQL load from generic storage `Get`.
- Prevent backup/read-repair/migration writes from flushing MySQL.
- Keep local WAL replay and local storage behavior.
- Document that native Olric APIs are cache/development paths unless routed
  through the durable string KV proxy.

### Phase 1: Owner-Side Durable Hook API

- Add the string KV business entry point.
- Add serving lease checks at the business entry point.
- Add owner-side `GET` miss MySQL refill hook.
- Add owner-side `SET/DEL/EXPIRE` persistence hooks.
- Add origin metadata and persistence eligibility.

### Phase 2: Owner Context Hardening And WAL State

- Keep Olric routing as the only owner routing authority.
- Prove non-owner ingress does not write WAL.
- Add WAL `prepared -> committed` state.
- Ensure only committed records are MySQL-flushable.
- Ensure failed Olric writes cannot later become visible in MySQL.
- Ensure WAL replay is fragment-scoped.
- Keep backup/read-repair/migration operations local-only.
- Keep Olric routing/quorum/read-repair/migration algorithms unchanged.

### Phase 3: Read-Through And Refill Hardening

- Make MySQL refill conditional on the key still being absent under the fragment
  lock.
- Prevent local-only refill records from overwriting flushable dirty WAL records.
- Add singleflight for concurrent MySQL loads.
- Add checkpoint cleanup for local-only refill records.

### Phase 4: Owner Fence And Handoff

- Replace wall-clock MySQL versions with owner-fenced versions.
- Prevent stale previous-owner flushes from overwriting newer owner writes.
- Define durable handoff during fragment migration/rebalance.
- Ensure new owner readiness requires equivalent durable recovery source.

### Phase 5: Failure Testing

Add tests for:

- LRU evicts a key and durable `GET` refills from MySQL.
- Client `DEL` on an in-memory miss writes a MySQL tombstone.
- Backup apply does not update MySQL.
- Read-repair does not update MySQL.
- Migration import does not update MySQL.
- Old owner with memory-resident data cannot serve client `GET` after lease loss.
- MySQL outage after accepted WAL write does not lose the write.
- MySQL refill does not participate in read quorum as an extra replica.
- Failed Olric mutation after prepared WAL does not flush to MySQL.
- Read-through refill cannot overwrite a concurrent successful write.
- Previous owner delayed flush cannot overwrite a newer fenced owner write.
- Fragment migration cannot lose unflushed owner-local WAL state.

## Non-Goals

- Do not replace Olric routing.
- Do not inject Watchdog topology into Olric ownership.
- Do not make MySQL a quorum participant.
- Do not make MySQL an Olric backup owner.
- Do not implement arbitrary Olric DMap value persistence.
- Do not add a new distributed consistency algorithm in the first durable string
  KV implementation.
