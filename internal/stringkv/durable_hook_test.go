package stringkv

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	olricconfig "github.com/olric-data/olric/config"
	olricstorage "github.com/olric-data/olric/pkg/storage"
	"github.com/zhixiongdu/olricstack/internal/oplog"
	"github.com/zhixiongdu/olricstack/internal/ring"
	"github.com/zhixiongdu/olricstack/internal/store"
)

// recordingRing captures every payload Append received plus an optional
// canned error sequence. It is goroutine-safe so tests can simulate ring-full
// retries from multiple goroutines.
type recordingRing struct {
	mu        sync.Mutex
	payloads  [][]byte
	errs      []error
	notifyCh  chan struct{}
	failCount int
}

func newRecordingRing() *recordingRing {
	return &recordingRing{notifyCh: make(chan struct{}, 64)}
}

func (r *recordingRing) Append(p []byte) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.errs) > 0 {
		err := r.errs[0]
		r.errs = r.errs[1:]
		if err != nil {
			r.failCount++
			return err
		}
	}
	cp := make([]byte, len(p))
	copy(cp, p)
	r.payloads = append(r.payloads, cp)
	select {
	case r.notifyCh <- struct{}{}:
	default:
	}
	return nil
}

func (r *recordingRing) snapshot() [][]byte {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([][]byte, len(r.payloads))
	for i, p := range r.payloads {
		cp := make([]byte, len(p))
		copy(cp, p)
		out[i] = cp
	}
	return out
}

// stubControl records sidecar RPC calls and serves canned responses.
type stubControl struct {
	mu             sync.Mutex
	notifyCalls    int
	loadCalls      int
	loadResp       store.EntryRecord
	loadFound      bool
	loadErr        error
	drainCalls     []drainCall
	drainErr       error
	drainPartCount uint64
}

type drainCall struct {
	dmap           string
	partitionID    uint64
	partitionCount uint64
}

func (c *stubControl) Notify(_ context.Context, _ uint64) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.notifyCalls++
	return nil
}

func (c *stubControl) LoadFromMySQL(_ context.Context, _ string, _ string, _ uint64) (store.EntryRecord, bool, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.loadCalls++
	return c.loadResp, c.loadFound, c.loadErr
}

func (c *stubControl) DrainPartition(_ context.Context, dmap string, partitionID, partitionCount uint64) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.drainCalls = append(c.drainCalls, drainCall{dmap, partitionID, partitionCount})
	return c.drainErr
}

func newTestDurableHook(t *testing.T, r RingProducer, c SidecarControl, lease *fakeFenceLease) *DurableHook {
	t.Helper()
	hook, err := NewDurableHook(r, c, lease, newFakeFenceSequencer(), "test-writer", Config{AppendBudget: time.Second})
	if err != nil {
		t.Fatalf("new durable hook: %v", err)
	}
	return hook
}

// fenceAt mirrors the helper from previous test files. g=0 means the lease is
// not currently servable.
func fenceAt(g, e int64) *fakeFenceLease {
	return &fakeFenceLease{g: g, e: e, ok: g > 0}
}

type fakeFenceLease struct {
	g, e int64
	ok   bool
}

func (l *fakeFenceLease) SnapshotForWrite(time.Time) (int64, int64, bool) {
	return l.g, l.e, l.ok
}

func (l *fakeFenceLease) VerifyFence(g, e int64, _ time.Time) bool {
	if !l.ok {
		return false
	}
	if l.g != g {
		return false
	}
	if l.e < e {
		return false
	}
	return true
}

type fakeFenceSequencer struct {
	g, e, s int64
	calls   int
}

func newFakeFenceSequencer() *fakeFenceSequencer { return &fakeFenceSequencer{} }

func (s *fakeFenceSequencer) Stamp(g, e int64) (int64, int64, int64, error) {
	s.calls++
	if g > s.g || (g == s.g && e > s.e) {
		s.g = g
		s.e = e
		s.s = 1
	} else if g == s.g && e == s.e {
		s.s++
	}
	return s.g, s.e, s.s, nil
}

func newTestEntry(key, value string, ttl, timestamp int64) *testEntry {
	return &testEntry{key: key, value: []byte(value), ttl: ttl, timestamp: timestamp}
}

func decodeRing(t *testing.T, payload []byte) oplog.Entry {
	t.Helper()
	e, err := oplog.Decode(payload)
	if err != nil {
		t.Fatalf("decode oplog: %v", err)
	}
	return e
}

func TestBeforeSetStampsFenceWithoutPublishing(t *testing.T) {
	t.Parallel()
	r := newRecordingRing()
	c := &stubControl{}
	hook := newTestDurableHook(t, r, c, fenceAt(7, 3))

	op, err := hook.BeforeSet(context.Background(), olricconfig.DurableOperation{
		DMap: "users", Key: "alice", HKey: HKey("users", "alice"),
		Entry: newTestEntry("alice", "A", 0, 1),
	})
	if err != nil {
		t.Fatalf("before set: %v", err)
	}
	if op.FenceGeneration != 7 || op.FenceEpoch != 3 {
		t.Fatalf("expected fence (7,3), got (%d,%d)", op.FenceGeneration, op.FenceEpoch)
	}
	if got := r.snapshot(); len(got) != 0 {
		t.Fatalf("BeforeSet must not append to ring, got %d entries", len(got))
	}
}

func TestBeforeSetRejectsExpiredLease(t *testing.T) {
	t.Parallel()
	r := newRecordingRing()
	hook := newTestDurableHook(t, r, &stubControl{}, &fakeFenceLease{})

	_, err := hook.BeforeSet(context.Background(), olricconfig.DurableOperation{
		DMap: "users", Key: "alice", HKey: HKey("users", "alice"),
		Entry: newTestEntry("alice", "A", 0, 1),
	})
	if !errors.Is(err, ErrLeaseExpired) {
		t.Fatalf("expected ErrLeaseExpired, got %v", err)
	}
	if got := r.snapshot(); len(got) != 0 {
		t.Fatalf("expected no ring writes on expired lease, got %d", len(got))
	}
}

func TestAfterSetSuccessPublishesToRing(t *testing.T) {
	t.Parallel()
	r := newRecordingRing()
	c := &stubControl{}
	hook := newTestDurableHook(t, r, c, fenceAt(7, 3))

	op, err := hook.BeforeSet(context.Background(), olricconfig.DurableOperation{
		DMap: "users", Key: "alice", HKey: 42,
		Entry: newTestEntry("alice", "A", 0, 1),
	})
	if err != nil {
		t.Fatalf("before set: %v", err)
	}
	if err := hook.AfterSet(context.Background(), op, nil); err != nil {
		t.Fatalf("after set: %v", err)
	}
	got := r.snapshot()
	if len(got) != 1 {
		t.Fatalf("expected one ring entry, got %d", len(got))
	}
	entry := decodeRing(t, got[0])
	if entry.Op != oplog.OpSet || entry.Key != "alice" || entry.HKey != 42 {
		t.Fatalf("unexpected entry: %+v", entry)
	}
	if entry.Generation != 7 || entry.Epoch != 3 || entry.OwnerSeq != 1 {
		t.Fatalf("expected fence (7,3,1), got (%d,%d,%d)", entry.Generation, entry.Epoch, entry.OwnerSeq)
	}
	if entry.WriterID != "test-writer" {
		t.Fatalf("expected writer test-writer, got %q", entry.WriterID)
	}
	if c.notifyCalls == 0 {
		t.Fatalf("expected sidecar Notify to be called after Append")
	}
}

func TestAfterSetWithMutationErrorPublishesNothing(t *testing.T) {
	t.Parallel()
	r := newRecordingRing()
	c := &stubControl{}
	hook := newTestDurableHook(t, r, c, fenceAt(7, 3))

	op, err := hook.BeforeSet(context.Background(), olricconfig.DurableOperation{
		DMap: "users", Key: "alice", HKey: 42,
		Entry: newTestEntry("alice", "A", 0, 1),
	})
	if err != nil {
		t.Fatalf("before set: %v", err)
	}
	mutErr := errors.New("quorum loss")
	if err := hook.AfterSet(context.Background(), op, mutErr); err != nil {
		t.Fatalf("after set returned error on mutation failure: %v", err)
	}
	if got := r.snapshot(); len(got) != 0 {
		t.Fatalf("expected no ring entries on failed mutation, got %d", len(got))
	}
	if c.notifyCalls != 0 {
		t.Fatalf("expected no Notify on failed mutation, got %d", c.notifyCalls)
	}
}

func TestAfterDeletePublishesTombstone(t *testing.T) {
	t.Parallel()
	r := newRecordingRing()
	hook := newTestDurableHook(t, r, &stubControl{}, fenceAt(11, 4))

	op, err := hook.BeforeDelete(context.Background(), olricconfig.DurableOperation{
		DMap: "users", Key: "alice", HKey: 42,
	})
	if err != nil {
		t.Fatalf("before delete: %v", err)
	}
	if err := hook.AfterDelete(context.Background(), op, nil); err != nil {
		t.Fatalf("after delete: %v", err)
	}
	got := r.snapshot()
	if len(got) != 1 {
		t.Fatalf("expected one tombstone in ring, got %d", len(got))
	}
	entry := decodeRing(t, got[0])
	if entry.Op != oplog.OpDelete {
		t.Fatalf("expected OpDelete, got %q", entry.Op)
	}
}

func TestAfterExpirePublishesExpire(t *testing.T) {
	t.Parallel()
	r := newRecordingRing()
	hook := newTestDurableHook(t, r, &stubControl{}, fenceAt(2, 9))

	op, err := hook.BeforeExpire(context.Background(), olricconfig.DurableOperation{
		DMap: "users", Key: "alice", HKey: 99,
		Entry: newTestEntry("alice", "A", time.Now().Add(time.Hour).UnixMilli(), 1),
	})
	if err != nil {
		t.Fatalf("before expire: %v", err)
	}
	if err := hook.AfterExpire(context.Background(), op, nil); err != nil {
		t.Fatalf("after expire: %v", err)
	}
	got := r.snapshot()
	if len(got) != 1 {
		t.Fatalf("expected one expire in ring, got %d", len(got))
	}
	entry := decodeRing(t, got[0])
	if entry.Op != oplog.OpExpire {
		t.Fatalf("expected OpExpire, got %q", entry.Op)
	}
}

func TestVerifyAfterLockPassesOnUnchangedFence(t *testing.T) {
	t.Parallel()
	hook := newTestDurableHook(t, newRecordingRing(), &stubControl{}, fenceAt(7, 3))
	op, err := hook.BeforeSet(context.Background(), olricconfig.DurableOperation{
		DMap: "users", Key: "alice", HKey: 1, Entry: newTestEntry("alice", "A", 0, 1),
	})
	if err != nil {
		t.Fatalf("before set: %v", err)
	}
	if err := hook.VerifyAfterLock(context.Background(), op); err != nil {
		t.Fatalf("verify: %v", err)
	}
}

func TestVerifyAfterLockRejectsLeaseRevocation(t *testing.T) {
	t.Parallel()
	lease := fenceAt(7, 3)
	hook := newTestDurableHook(t, newRecordingRing(), &stubControl{}, lease)
	op, err := hook.BeforeSet(context.Background(), olricconfig.DurableOperation{
		DMap: "users", Key: "alice", HKey: 1, Entry: newTestEntry("alice", "A", 0, 1),
	})
	if err != nil {
		t.Fatalf("before set: %v", err)
	}
	lease.ok = false
	if err := hook.VerifyAfterLock(context.Background(), op); !errors.Is(err, ErrLeaseExpired) {
		t.Fatalf("expected ErrLeaseExpired, got %v", err)
	}
}

func TestVerifyAfterLockRejectsGenerationChange(t *testing.T) {
	t.Parallel()
	lease := fenceAt(7, 3)
	hook := newTestDurableHook(t, newRecordingRing(), &stubControl{}, lease)
	op, err := hook.BeforeSet(context.Background(), olricconfig.DurableOperation{
		DMap: "users", Key: "alice", HKey: 1, Entry: newTestEntry("alice", "A", 0, 1),
	})
	if err != nil {
		t.Fatalf("before set: %v", err)
	}
	lease.g = 8
	lease.e = 0
	if err := hook.VerifyAfterLock(context.Background(), op); !errors.Is(err, ErrLeaseExpired) {
		t.Fatalf("expected ErrLeaseExpired on generation change, got %v", err)
	}
}

func TestLoadOnMissDelegatesToSidecar(t *testing.T) {
	t.Parallel()
	c := &stubControl{
		loadFound: true,
		loadResp: store.EntryRecord{
			DMap: "users", Key: "alice", HKey: 1,
			EncodedEntry: newTestEntry("alice", "A", 0, 1).Encode(),
		},
	}
	hook := newTestDurableHook(t, newRecordingRing(), c, fenceAt(1, 1))
	template := newTestEntry("", "", 0, 0)
	got, err := hook.LoadOnMiss(context.Background(), olricconfig.DurableOperation{
		DMap: "users", Key: "alice", HKey: 1, Entry: template,
	})
	if err != nil {
		t.Fatalf("load on miss: %v", err)
	}
	if got.Key() != "alice" || string(got.Value()) != "A" {
		t.Fatalf("unexpected loaded entry: key=%q value=%q", got.Key(), got.Value())
	}
}

func TestLoadOnMissPropagatesNotFoundFromSidecar(t *testing.T) {
	t.Parallel()
	hook := newTestDurableHook(t, newRecordingRing(), &stubControl{}, fenceAt(1, 1))
	_, err := hook.LoadOnMiss(context.Background(), olricconfig.DurableOperation{
		DMap: "users", Key: "missing", HKey: 1, Entry: newTestEntry("", "", 0, 0),
	})
	if !errors.Is(err, olricstorage.ErrKeyNotFound) {
		t.Fatalf("expected ErrKeyNotFound, got %v", err)
	}
}

func TestDrainForHandoffCallsDrainPartition(t *testing.T) {
	t.Parallel()
	c := &stubControl{}
	hook := newTestDurableHook(t, newRecordingRing(), c, fenceAt(1, 1))
	if err := hook.DrainForHandoff(context.Background(), olricconfig.DurableHandoff{
		DMap: "users", PartitionID: 7, PartitionCount: 271,
	}); err != nil {
		t.Fatalf("drain: %v", err)
	}
	if len(c.drainCalls) != 1 {
		t.Fatalf("expected one drain call, got %d", len(c.drainCalls))
	}
	got := c.drainCalls[0]
	if got.dmap != "users" || got.partitionID != 7 || got.partitionCount != 271 {
		t.Fatalf("unexpected drain call: %+v", got)
	}
}

func TestDrainForHandoffSurfacesError(t *testing.T) {
	t.Parallel()
	c := &stubControl{drainErr: errors.New("mysql is down")}
	hook := newTestDurableHook(t, newRecordingRing(), c, fenceAt(1, 1))
	err := hook.DrainForHandoff(context.Background(), olricconfig.DurableHandoff{
		DMap: "users", PartitionID: 0, PartitionCount: 1,
	})
	if err == nil || !errors.Is(err, c.drainErr) {
		t.Fatalf("expected wrapped drain error, got %v", err)
	}
}

func TestDrainForHandoffNoOpOnZeroPartitionCount(t *testing.T) {
	t.Parallel()
	c := &stubControl{}
	hook := newTestDurableHook(t, newRecordingRing(), c, fenceAt(1, 1))
	if err := hook.DrainForHandoff(context.Background(), olricconfig.DurableHandoff{DMap: "users"}); err != nil {
		t.Fatalf("drain: %v", err)
	}
	if len(c.drainCalls) != 0 {
		t.Fatalf("expected no drain calls when PartitionCount=0, got %d", len(c.drainCalls))
	}
}

func TestAppendRetriesOnTransientFull(t *testing.T) {
	t.Parallel()
	r := newRecordingRing()
	r.errs = []error{ring.ErrRingFull, ring.ErrRingFull, nil}
	hook := newTestDurableHook(t, r, &stubControl{}, fenceAt(1, 1))
	op, err := hook.BeforeSet(context.Background(), olricconfig.DurableOperation{
		DMap: "users", Key: "k", HKey: 1, Entry: newTestEntry("k", "v", 0, 1),
	})
	if err != nil {
		t.Fatalf("before set: %v", err)
	}
	if err := hook.AfterSet(context.Background(), op, nil); err != nil {
		t.Fatalf("after set: %v", err)
	}
	if r.failCount != 2 {
		t.Fatalf("expected 2 transient retries, got %d", r.failCount)
	}
	if len(r.snapshot()) != 1 {
		t.Fatalf("expected eventual append success")
	}
}

func TestAppendBudgetExceededReturnsError(t *testing.T) {
	t.Parallel()
	r := newRecordingRing()
	for i := 0; i < 100; i++ {
		r.errs = append(r.errs, ring.ErrRingFull)
	}
	hook, err := NewDurableHook(r, &stubControl{}, fenceAt(1, 1), newFakeFenceSequencer(), "test-writer", Config{AppendBudget: 50 * time.Millisecond})
	if err != nil {
		t.Fatalf("new hook: %v", err)
	}
	op, err := hook.BeforeSet(context.Background(), olricconfig.DurableOperation{
		DMap: "users", Key: "k", HKey: 1, Entry: newTestEntry("k", "v", 0, 1),
	})
	if err != nil {
		t.Fatalf("before set: %v", err)
	}
	err = hook.AfterSet(context.Background(), op, nil)
	if err == nil || !errors.Is(err, ring.ErrRingFull) {
		t.Fatalf("expected ring full error after budget exceeded, got %v", err)
	}
}

// countingSequencer wraps fakeFenceSequencer and counts Stamp invocations so
// the test can pin that AfterX did not allocate an owner sequence when the
// mutation reported failure.
type countingSequencer struct {
	*fakeFenceSequencer
	stamps int
}

func (s *countingSequencer) Stamp(g, e int64) (int64, int64, int64, error) {
	s.stamps++
	return s.fakeFenceSequencer.Stamp(g, e)
}

// TestAfterXDoesNotPublishOnMutationError pins the AfterX contract documented
// at durable_hook.go:53-58: when the fork reports a mutation error, AfterX
// must NOT stamp owner_seq, NOT append to the ring, and NOT notify the
// sidecar. There is no prepared state to roll back.
func TestAfterXDoesNotPublishOnMutationError(t *testing.T) {
	t.Parallel()
	mutErr := errors.New("simulated fork mutation failure")

	type op string
	const (
		opSet    op = "set"
		opDelete op = "delete"
		opExpire op = "expire"
	)

	cases := []struct {
		name string
		op   op
	}{
		{"AfterSet", opSet},
		{"AfterDelete", opDelete},
		{"AfterExpire", opExpire},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			r := newRecordingRing()
			c := &stubControl{}
			seq := &countingSequencer{fakeFenceSequencer: newFakeFenceSequencer()}
			hook, err := NewDurableHook(r, c, fenceAt(1, 1), seq, "test-writer", Config{})
			if err != nil {
				t.Fatalf("new durable hook: %v", err)
			}

			ctx := context.Background()
			base := olricconfig.DurableOperation{
				DMap: "users", Key: "alice", HKey: HKey("users", "alice"),
			}

			var (
				stamped olricconfig.DurableOperation
				afterFn func(context.Context, olricconfig.DurableOperation, error) error
			)
			switch tc.op {
			case opSet:
				base.Entry = newTestEntry("alice", "A", 0, 1)
				stamped, err = hook.BeforeSet(ctx, base)
				afterFn = hook.AfterSet
			case opDelete:
				// BeforeDelete tolerates a nil entry per the production code.
				stamped, err = hook.BeforeDelete(ctx, base)
				afterFn = hook.AfterDelete
			case opExpire:
				base.Entry = newTestEntry("alice", "A", time.Now().Add(time.Hour).UnixMilli(), 1)
				stamped, err = hook.BeforeExpire(ctx, base)
				afterFn = hook.AfterExpire
			}
			if err != nil {
				t.Fatalf("before %s: %v", tc.name, err)
			}
			if stamped.FenceGeneration != 1 || stamped.FenceEpoch != 1 {
				t.Fatalf("before %s expected fence (1,1), got (%d,%d)",
					tc.name, stamped.FenceGeneration, stamped.FenceEpoch)
			}

			if err := afterFn(ctx, stamped, mutErr); err != nil {
				t.Fatalf("%s with mutation error must be a silent no-op, got %v", tc.name, err)
			}

			if got := r.snapshot(); len(got) != 0 {
				t.Fatalf("%s must not append to ring on mutation error, got %d entries", tc.name, len(got))
			}
			if c.notifyCalls != 0 {
				t.Fatalf("%s must not notify sidecar on mutation error, got %d calls", tc.name, c.notifyCalls)
			}
			if seq.stamps != 0 {
				t.Fatalf("%s must not allocate owner_seq on mutation error, got %d Stamp calls", tc.name, seq.stamps)
			}
		})
	}
}

func TestFenceSequenceIsMonotonic(t *testing.T) {
	t.Parallel()
	r := newRecordingRing()
	hook := newTestDurableHook(t, r, &stubControl{}, fenceAt(1, 1))
	for i := 0; i < 3; i++ {
		op, err := hook.BeforeSet(context.Background(), olricconfig.DurableOperation{
			DMap: "users", Key: "alice", HKey: 1, Entry: newTestEntry("alice", "A", 0, int64(i)),
		})
		if err != nil {
			t.Fatalf("before set %d: %v", i, err)
		}
		if err := hook.AfterSet(context.Background(), op, nil); err != nil {
			t.Fatalf("after set %d: %v", i, err)
		}
	}
	got := r.snapshot()
	if len(got) != 3 {
		t.Fatalf("expected 3 entries, got %d", len(got))
	}
	for i, payload := range got {
		entry := decodeRing(t, payload)
		if entry.OwnerSeq != int64(i+1) {
			t.Fatalf("entry %d: expected OwnerSeq=%d, got %d", i, i+1, entry.OwnerSeq)
		}
	}
}
