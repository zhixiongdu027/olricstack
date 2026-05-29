package stringkv

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	olricconfig "github.com/olric-data/olric/config"
	olricstorage "github.com/olric-data/olric/pkg/storage"
	"github.com/zhixiongdu/olricstack/internal/node"
	"github.com/zhixiongdu/olricstack/internal/oplog"
	"github.com/zhixiongdu/olricstack/internal/ring"
	"github.com/zhixiongdu/olricstack/internal/store"
)

// FenceLease is the subset of LeaseTracker the durable hook needs to stamp
// fence triples. It exists so tests can inject a deterministic snapshot
// without spinning up a full topology subscription.
type FenceLease interface {
	SnapshotForWrite(now time.Time) (generation, epoch int64, ok bool)
	VerifyFence(generation, epoch int64, now time.Time) bool
}

// FenceSequencer issues monotonic owner sequences within a fence window.
type FenceSequencer interface {
	Stamp(generation, epoch int64) (g, e, s int64, err error)
}

// RingProducer is the subset of ring.Producer the hook needs. It is a tiny
// interface so tests can substitute a recording double.
type RingProducer interface {
	Append(payload []byte) error
	Pending() uint64
	Capacity() uint64
}

// SidecarControl is the subset of the sidecar gRPC client the hook depends on
// for the read-miss and handoff paths.
type SidecarControl interface {
	Notify(ctx context.Context, pending uint64) error
	LoadFromMySQL(ctx context.Context, dmap, key string, hkey uint64) (record store.EntryRecord, found bool, err error)
	DrainPartition(ctx context.Context, dmap string, partitionID, partitionCount uint64) error
}

// DurableHook is the node-side owner hook in the shm-oplog architecture.
//
// Invariants:
//
//  1. BeforeX stamps the fence (G, E) into op so VerifyAfterLock can verify
//     it is still current after the fork acquires the fragment lock. It also
//     pre-checks ring space by estimating the encoded oplog size and
//     comparing against free capacity (under appendMu for accuracy).
//     Rejects writes before the in-memory mutation when the ring is
//     near full, holding a 64KB concurrent-write buffer.
//
//  2. AfterX(op, mutErr): if mutErr != nil, do nothing — there is no prepared
//     state to roll back. If mutErr == nil, stamp the owner sequence S,
//     encode the oplog entry, and Append to the shm ring. Append returning
//     success IS the durable acknowledgement returned to the RESP client. The
//     ring being full triggers bounded retry (see appendWithBudget); a final
//     failure surfaces to the caller.
//
//  3. LoadOnMiss is delegated to the sidecar over the control RPC. The hook
//     does not produce any ring or WAL traffic for read-miss refills.
//
//  4. DrainForHandoff calls sidecar.DrainPartition synchronously. The fork
//     holds the per-fragment write lock for the duration.
type DurableHook struct {
	ring         RingProducer
	control      SidecarControl
	lease        FenceLease
	sequence     FenceSequencer
	writerID     string
	now          func() time.Time
	appendBudget time.Duration
	onFatal      func(error)
	fatalCalled  atomic.Bool
	appendMu     sync.Mutex
}

// Config tunes hook behavior. Zero values use sensible defaults.
type Config struct {
	// AppendBudget caps the total time AfterX will spend retrying ring
	// Append when the ring is full. Returning before AppendBudget elapsed
	// means either success or a non-retryable error. Default: 5s.
	AppendBudget time.Duration
	Now          func() time.Time
	// OnFatal is called once when a mutation has already succeeded but the
	// durable publish path cannot record it. The node must stop serving after
	// this point because memory may contain an unlogged value.
	OnFatal func(error)
}

func (c Config) withDefaults() Config {
	if c.AppendBudget <= 0 {
		c.AppendBudget = 5 * time.Second
	}
	if c.Now == nil {
		c.Now = time.Now
	}
	return c
}

func NewDurableHook(ring RingProducer, control SidecarControl, lease FenceLease, sequence FenceSequencer, writerID string, cfg Config) (*DurableHook, error) {
	if ring == nil {
		return nil, errors.New("ring producer is nil")
	}
	if lease == nil {
		return nil, errors.New("fence lease is nil")
	}
	if sequence == nil {
		return nil, errors.New("fence sequencer is nil")
	}
	if writerID == "" {
		return nil, errors.New("writer id is required")
	}
	cfg = cfg.withDefaults()
	return &DurableHook{
		ring:         ring,
		control:      control,
		lease:        lease,
		sequence:     sequence,
		writerID:     writerID,
		now:          cfg.Now,
		appendBudget: cfg.AppendBudget,
		onFatal:      cfg.OnFatal,
	}, nil
}

func (h *DurableHook) BeforeSet(_ context.Context, op olricconfig.DurableOperation) (olricconfig.DurableOperation, error) {
	if op.Entry == nil {
		return op, errors.New("durable set entry is nil")
	}
	return h.stamp(op)
}

func (h *DurableHook) AfterSet(ctx context.Context, op olricconfig.DurableOperation, mutationErr error) error {
	if mutationErr != nil {
		return nil
	}
	if err := h.publish(ctx, op, oplog.OpSet); err != nil {
		h.fatal(fmt.Errorf("durable set publish after mutation: %w", err))
		return err
	}
	return nil
}

func (h *DurableHook) BeforeDelete(_ context.Context, op olricconfig.DurableOperation) (olricconfig.DurableOperation, error) {
	return h.stamp(op)
}

func (h *DurableHook) AfterDelete(ctx context.Context, op olricconfig.DurableOperation, mutationErr error) error {
	if mutationErr != nil {
		return nil
	}
	if err := h.publish(ctx, op, oplog.OpDelete); err != nil {
		h.fatal(fmt.Errorf("durable delete publish after mutation: %w", err))
		return err
	}
	return nil
}

func (h *DurableHook) BeforeExpire(_ context.Context, op olricconfig.DurableOperation) (olricconfig.DurableOperation, error) {
	if op.Entry == nil {
		return op, errors.New("durable expire entry is nil")
	}
	return h.stamp(op)
}

func (h *DurableHook) AfterExpire(ctx context.Context, op olricconfig.DurableOperation, mutationErr error) error {
	if mutationErr != nil {
		return nil
	}
	if err := h.publish(ctx, op, oplog.OpExpire); err != nil {
		h.fatal(fmt.Errorf("durable expire publish after mutation: %w", err))
		return err
	}
	return nil
}

func (h *DurableHook) fatal(err error) {
	if err == nil || h.onFatal == nil {
		return
	}
	if h.fatalCalled.CompareAndSwap(false, true) {
		h.onFatal(err)
	}
}

// VerifyAfterLock re-checks that the fence stamped by BeforeX is still valid
// under the current lease. A failure here forces the fork to skip the
// mutation; AfterX will then run with mutationErr != nil and we will not
// publish to the ring.
func (h *DurableHook) VerifyAfterLock(_ context.Context, op olricconfig.DurableOperation) error {
	if op.FenceGeneration == 0 {
		return nil
	}
	if !h.lease.VerifyFence(op.FenceGeneration, op.FenceEpoch, h.now()) {
		return ErrLeaseExpired
	}
	return nil
}

// LoadOnMiss serves the fork's miss-path read by asking the sidecar.
func (h *DurableHook) LoadOnMiss(ctx context.Context, op olricconfig.DurableOperation) (olricstorage.Entry, error) {
	if h.control == nil {
		return nil, olricstorage.ErrKeyNotFound
	}
	record, found, err := h.control.LoadFromMySQL(ctx, op.DMap, op.Key, op.HKey)
	if err != nil {
		return nil, fmt.Errorf("durable load miss: %w", err)
	}
	if !found || record.Tombstone {
		return nil, olricstorage.ErrKeyNotFound
	}
	entry := op.Entry
	if entry == nil {
		return nil, errors.New("durable load miss entry template is nil")
	}
	if len(record.EncodedEntry) == 0 {
		return nil, olricstorage.ErrKeyNotFound
	}
	entry.Decode(record.EncodedEntry)
	return entry, nil
}

// DrainForHandoff blocks until the sidecar has flushed every pending entry
// for the migrating fragment to MySQL.
func (h *DurableHook) DrainForHandoff(ctx context.Context, handoff olricconfig.DurableHandoff) error {
	if h.control == nil {
		return nil
	}
	if handoff.PartitionCount == 0 {
		return nil
	}
	if err := h.control.DrainPartition(ctx, handoff.DMap, handoff.PartitionID, handoff.PartitionCount); err != nil {
		return fmt.Errorf("drain partition %d for %s: %w", handoff.PartitionID, handoff.DMap, err)
	}
	return nil
}

// estimateOplogSize returns a conservative upper bound on the encoded
// oplog payload size for this operation. It overestimates to avoid
// falsely rejecting valid writes.
func (h *DurableHook) estimateOplogSize(op olricconfig.DurableOperation) uint64 {
	const jsonOverhead = 256 // JSON framing, field names, digits, braces
	size := uint64(len(op.DMap)+len(op.Key)+len(h.writerID)) + jsonOverhead
	if op.Entry != nil {
		raw := uint64(len(op.Entry.Encode()))
		// base64 expansion: ceil(raw/3)*4, conservatively use raw*2
		size += raw * 2
	}
	return size
}

func (h *DurableHook) stamp(op olricconfig.DurableOperation) (olricconfig.DurableOperation, error) {
	g, e, ok := h.lease.SnapshotForWrite(h.now())
	if !ok {
		return op, ErrLeaseExpired
	}
	op.FenceGeneration = g
	op.FenceEpoch = e

	// Pre-check ring space before allowing the in-memory mutation.
	// Hold appendMu for an accurate snapshot of pending bytes and
	// account for the estimated entry size plus a concurrent-write
	// buffer. This prevents dirtying the in-memory engine for writes
	// that have a high chance of failing to reach the oplog.
	estimatedSize := h.estimateOplogSize(op)
	h.appendMu.Lock()
	free := h.ring.Capacity() - h.ring.Pending()
	// Reserve room for this entry plus 64KB for concurrent writes
	// that may slip in between this check and the actual Append.
	const concurrentBuffer = 64 * 1024
	if free < estimatedSize+concurrentBuffer {
		h.appendMu.Unlock()
		return op, ErrRingAlmostFull
	}
	h.appendMu.Unlock()
	return op, nil
}

func (h *DurableHook) publish(ctx context.Context, op olricconfig.DurableOperation, kind oplog.Op) error {
	g, e, s, err := h.sequence.Stamp(op.FenceGeneration, op.FenceEpoch)
	if err != nil {
		return fmt.Errorf("stamp owner seq: %w", err)
	}
	entry := oplog.Entry{
		Op:                kind,
		DMap:              op.DMap,
		Key:               op.Key,
		HKey:              op.HKey,
		Generation:        g,
		Epoch:             e,
		OwnerSeq:          s,
		WriterID:          h.writerID,
		UpdatedAtUnixNano: h.now().UTC().UnixNano(),
	}
	if op.Entry != nil {
		entry.EncodedEntry = op.Entry.Encode()
		entry.TTL = op.Entry.TTL()
		entry.Timestamp = op.Entry.Timestamp()
	}
	payload, err := oplog.Encode(entry)
	if err != nil {
		return fmt.Errorf("encode oplog: %w", err)
	}
	if err := h.appendWithBudget(ctx, payload); err != nil {
		return err
	}
	if h.control != nil {
		_ = h.control.Notify(ctx, uint64(len(payload)))
	}
	return nil
}

// appendWithBudget retries ring.Append while the ring reports full, up to
// h.appendBudget total wall time. The retry budget is bounded because the
// caller's request context (RESP timeout, e.g.) is also bounded.
func (h *DurableHook) appendWithBudget(ctx context.Context, payload []byte) error {
	deadline := h.now().Add(h.appendBudget)
	backoff := time.Millisecond
	for {
		h.appendMu.Lock()
		err := h.ring.Append(payload)
		h.appendMu.Unlock()
		if err == nil {
			return nil
		}
		if !errors.Is(err, ErrRingFull) {
			return fmt.Errorf("ring append: %w", err)
		}
		if h.now().After(deadline) {
			return fmt.Errorf("ring append: %w (budget exceeded)", err)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(backoff):
		}
		if backoff < 20*time.Millisecond {
			backoff *= 2
		}
	}
}

// ErrRingFull is re-exported from internal/ring so callers don't need to
// import the ring package just to test for the back-pressure condition.
var ErrRingFull = ring.ErrRingFull

// ErrRingAlmostFull is returned before mutation when the ring is near
// capacity, so that the in-memory engine is not dirtied by a write that
// has a high chance of failing to reach the oplog.
var ErrRingAlmostFull = errors.New("stringkv: ring near full, rejecting write before mutation")

// Compile-time guards.
var (
	_ olricconfig.DurableHook = (*DurableHook)(nil)
	_ FenceLease              = (*node.LeaseTracker)(nil)
)
