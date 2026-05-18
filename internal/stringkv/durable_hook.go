package stringkv

import (
	"context"
	"errors"
	"fmt"
	"time"

	olricconfig "github.com/olric-data/olric/config"
	olricstorage "github.com/olric-data/olric/pkg/storage"
	"github.com/zhixiongdu/olricstack/internal/node"
	"github.com/zhixiongdu/olricstack/internal/store"
)

// FenceLease is the subset of LeaseTracker the durable hook needs to stamp
// fence triples. It exists so tests can inject a deterministic snapshot
// without spinning up a full topology subscription.
type FenceLease interface {
	SnapshotForWrite(now time.Time) (generation, epoch int64, ok bool)
	// VerifyFence re-validates a previously snapshotted fence under the
	// current lease. It is the in-lock barrier used by the fork after it
	// acquires the per-fragment write lock and before it performs the
	// in-memory mutation. See LeaseTracker.VerifyFence.
	VerifyFence(generation, epoch int64, now time.Time) bool
}

// FenceSequencer issues monotonic owner sequences within a fence
// (generation, epoch). It must persist its state so owner sequences cannot
// regress across process restarts.
type FenceSequencer interface {
	Stamp(generation, epoch int64) (g, e, s int64, err error)
}

// DurableHook is the owner-side implementation of olricconfig.DurableHook.
//
// Invariants enforced here:
//
//  1. Before* operations stamp a fence triple (G, E, S) into the prepared WAL
//     record. The fence is read from the lease tracker and advanced by the
//     persistent OwnerSequence. The lease check inside Before* is the LAST
//     barrier between a still-valid serving lease and a durable record — it
//     runs inside the Olric fragment lock, after the higher-level service
//     gate, to close any window where the lease lapsed mid-call.
//
//  2. After*(op, mutErr) runs unconditionally:
//        mutErr == nil ⇒ CommitEntry(WALSeq)
//        mutErr != nil ⇒ AbortEntry(WALSeq)
//     This makes prepared records that never reached cluster-wide success
//     unflushable to MySQL, satisfying FT-1 and FT-2.
//
//  3. LoadOnMiss reads MySQL through the gated store. Fork-side fragment
//     re-locking (see internal/dmap/get.go) discards a stale refill if a
//     concurrent owner Set has already populated storage, satisfying FT-4.
type DurableHook struct {
	committer store.CommitStore
	lease     FenceLease
	sequence  FenceSequencer
	now       func() time.Time
}

func NewDurableHook(committer store.CommitStore, lease FenceLease, sequence FenceSequencer) (*DurableHook, error) {
	if committer == nil {
		return nil, errors.New("commit store is nil")
	}
	if lease == nil {
		return nil, errors.New("fence lease is nil")
	}
	if sequence == nil {
		return nil, errors.New("fence sequencer is nil")
	}
	return &DurableHook{
		committer: committer,
		lease:     lease,
		sequence:  sequence,
		now:       time.Now,
	}, nil
}

func (h *DurableHook) BeforeSet(ctx context.Context, op olricconfig.DurableOperation) (olricconfig.DurableOperation, error) {
	if op.Entry == nil {
		return op, errors.New("durable set entry is nil")
	}
	record, err := h.fencedRecord(op, op.Entry, "client_set", true)
	if err != nil {
		return op, err
	}
	prepared, err := h.committer.PrepareEntry(ctx, record)
	if err != nil {
		return op, err
	}
	op.Version = prepared.WALSeq
	op.FenceGeneration = record.Generation
	op.FenceEpoch = record.Epoch
	op.FenceOwnerSeq = record.OwnerSeq
	return op, nil
}

func (h *DurableHook) AfterSet(ctx context.Context, op olricconfig.DurableOperation, mutationErr error) error {
	return h.finalize(ctx, op, mutationErr)
}

func (h *DurableHook) BeforeDelete(ctx context.Context, op olricconfig.DurableOperation) (olricconfig.DurableOperation, error) {
	g, e, s, err := h.stampFence()
	if err != nil {
		return op, err
	}
	record := store.EntryRecord{
		DMap:       op.DMap,
		Key:        op.Key,
		HKey:       op.HKey,
		Tombstone:  true,
		Origin:     "client_delete",
		FlushMySQL: true,
		Generation: g,
		Epoch:      e,
		OwnerSeq:   s,
		UpdatedAt:  h.now().UTC(),
	}
	prepared, err := h.committer.PrepareEntry(ctx, record)
	if err != nil {
		return op, err
	}
	op.Version = prepared.WALSeq
	op.FenceGeneration = g
	op.FenceEpoch = e
	op.FenceOwnerSeq = s
	return op, nil
}

func (h *DurableHook) AfterDelete(ctx context.Context, op olricconfig.DurableOperation, mutationErr error) error {
	return h.finalize(ctx, op, mutationErr)
}

func (h *DurableHook) BeforeExpire(ctx context.Context, op olricconfig.DurableOperation) (olricconfig.DurableOperation, error) {
	if op.Entry == nil {
		return op, errors.New("durable expire entry is nil")
	}
	record, err := h.fencedRecord(op, op.Entry, "client_expire", true)
	if err != nil {
		return op, err
	}
	prepared, err := h.committer.PrepareEntry(ctx, record)
	if err != nil {
		return op, err
	}
	op.Version = prepared.WALSeq
	op.FenceGeneration = record.Generation
	op.FenceEpoch = record.Epoch
	op.FenceOwnerSeq = record.OwnerSeq
	return op, nil
}

func (h *DurableHook) AfterExpire(ctx context.Context, op olricconfig.DurableOperation, mutationErr error) error {
	return h.finalize(ctx, op, mutationErr)
}

// VerifyAfterLock re-validates the fence previously stamped by BeforeSet /
// BeforeDelete / BeforeExpire. It is called by the fork inside the per-
// fragment lock immediately before the in-memory mutation. The check is the
// last barrier preserving S5: if leadership moved or the lease expired
// between the prepared WAL append and the lock acquisition, the prepared
// record must be aborted (the fork follows a non-nil return with an
// AfterX(op, err) call which routes through finalize -> AbortEntry).
//
// Hooks that did not stamp a fence (op.FenceGeneration == 0) get a no-op:
// VerifyAfterLock only matters when there is a prepared record to abort.
func (h *DurableHook) VerifyAfterLock(ctx context.Context, op olricconfig.DurableOperation) error {
	if op.FenceGeneration == 0 {
		return nil
	}
	if !h.lease.VerifyFence(op.FenceGeneration, op.FenceEpoch, h.now()) {
		return ErrLeaseExpired
	}
	return nil
}

func (h *DurableHook) LoadOnMiss(ctx context.Context, op olricconfig.DurableOperation) (olricstorage.Entry, error) {
	// LoadOnMiss runs unlocked from the fork's perspective. Fork-side
	// re-locking + storage re-check (internal/dmap/get.go) discards this
	// entry if a concurrent Set has already populated the fragment, so we
	// do not need to re-read the lease here.
	record, err := h.committer.(store.CacheStore).LoadEntry(ctx, store.EntryRef{DMap: op.DMap, Key: op.Key, HKey: op.HKey})
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return nil, olricstorage.ErrKeyNotFound
		}
		return nil, fmt.Errorf("durable load miss: %w", err)
	}
	entry := op.Entry
	if entry == nil {
		return nil, errors.New("durable load miss entry template is nil")
	}
	entry.Decode(record.EncodedEntry)
	return entry, nil
}

func (h *DurableHook) fencedRecord(op olricconfig.DurableOperation, entry olricstorage.Entry, origin string, flush bool) (store.EntryRecord, error) {
	g, e, s, err := h.stampFence()
	if err != nil {
		return store.EntryRecord{}, err
	}
	now := h.now()
	return store.EntryRecord{
		DMap:         op.DMap,
		Key:          op.Key,
		HKey:         op.HKey,
		EncodedEntry: entry.Encode(),
		TTL:          entry.TTL(),
		Timestamp:    entry.Timestamp(),
		Origin:       origin,
		FlushMySQL:   flush,
		Generation:   g,
		Epoch:        e,
		OwnerSeq:     s,
		UpdatedAt:    now.UTC(),
	}, nil
}

func (h *DurableHook) stampFence() (int64, int64, int64, error) {
	g, e, ok := h.lease.SnapshotForWrite(h.now())
	if !ok {
		return 0, 0, 0, ErrLeaseExpired
	}
	gen, ep, seq, err := h.sequence.Stamp(g, e)
	if err != nil {
		return 0, 0, 0, fmt.Errorf("fence stamp: %w", err)
	}
	return gen, ep, seq, nil
}

func (h *DurableHook) finalize(ctx context.Context, op olricconfig.DurableOperation, mutationErr error) error {
	if op.Version == 0 {
		// BeforeX never returned a WALSeq (e.g. the prepare itself errored
		// before the WAL append). Nothing to commit or abort.
		return mutationErr
	}
	ref := store.EntryRef{DMap: op.DMap, Key: op.Key, HKey: op.HKey}
	if mutationErr != nil {
		// Use background context: the request context may already be
		// cancelled, but the WAL must be cleaned up regardless.
		if err := h.committer.AbortEntry(context.Background(), ref, op.Version); err != nil {
			return fmt.Errorf("abort prepared record after mutation failure (%v): %w", mutationErr, err)
		}
		return nil
	}
	return h.committer.CommitEntry(ctx, ref, op.Version)
}

// Compile-time guards: the hook satisfies the fork's contract, and node-side
// types satisfy the fence dependencies.
var (
	_ olricconfig.DurableHook = (*DurableHook)(nil)
	_ FenceLease              = (*node.LeaseTracker)(nil)
	_ FenceSequencer          = (*node.OwnerSequence)(nil)
)
