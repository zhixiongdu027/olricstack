package stringkv

import (
	"context"
	"errors"
	"testing"
	"time"

	olricconfig "github.com/olric-data/olric/config"
	olricstorage "github.com/olric-data/olric/pkg/storage"
	"github.com/zhixiongdu/olricstack/internal/store"
)

func TestDurableHookBeforeSetStampsFenceAndPreparesRecord(t *testing.T) {
	backing := newRecordingCommitStore()
	hook := newTestDurableHook(t, backing, fenceAt(7, 3))
	entry := newTestEntry("alice", "A", time.Now().Add(time.Minute).UnixMilli(), 42)

	op, err := hook.BeforeSet(context.Background(), olricconfig.DurableOperation{
		DMap:  "users",
		Key:   "alice",
		HKey:  HKey("users", "alice"),
		Entry: entry,
	})
	if err != nil {
		t.Fatalf("before set: %v", err)
	}
	if op.Version == 0 {
		t.Fatal("expected non-zero op.Version (WALSeq) after prepare")
	}
	record := backing.records[HKey("users", "alice")]
	if record.WALState != store.WALStatePrepared || record.FlushMySQL {
		t.Fatalf("expected prepared unflushable record, got %#v", record)
	}
	if record.Generation != 7 || record.Epoch != 3 || record.OwnerSeq != 1 {
		t.Fatalf("expected fence (7,3,1), got (%d,%d,%d)", record.Generation, record.Epoch, record.OwnerSeq)
	}
	if record.Origin != "client_set" || !recordHasFlushIntent(record) {
		t.Fatalf("expected client_set flushable-intent record, got %#v", record)
	}
}

func TestDurableHookBeforeSetRejectsExpiredLease(t *testing.T) {
	backing := newRecordingCommitStore()
	lease := &fakeFenceLease{}
	seq := newFakeFenceSequencer()
	hook, err := NewDurableHook(backing, lease, seq)
	if err != nil {
		t.Fatalf("new hook: %v", err)
	}

	op, err := hook.BeforeSet(context.Background(), olricconfig.DurableOperation{
		DMap:  "users",
		Key:   "alice",
		HKey:  HKey("users", "alice"),
		Entry: newTestEntry("alice", "A", 0, 1),
	})
	if !errors.Is(err, ErrLeaseExpired) {
		t.Fatalf("expected ErrLeaseExpired, got %v", err)
	}
	if op.Version != 0 {
		t.Fatal("op.Version must remain 0 when prepare is denied")
	}
	if len(backing.records) != 0 {
		t.Fatalf("no WAL record may exist when lease is invalid, got %v", backing.records)
	}
	if seq.calls != 0 {
		t.Fatalf("expected no fence stamp on lease denial, got %d", seq.calls)
	}
}

func TestDurableHookAfterSetCommitsOnSuccess(t *testing.T) {
	backing := newRecordingCommitStore()
	hook := newTestDurableHook(t, backing, fenceAt(7, 3))
	entry := newTestEntry("alice", "A", 0, 42)

	op, err := hook.BeforeSet(context.Background(), olricconfig.DurableOperation{
		DMap: "users", Key: "alice", HKey: HKey("users", "alice"), Entry: entry,
	})
	if err != nil {
		t.Fatalf("before set: %v", err)
	}
	if err := hook.AfterSet(context.Background(), op, nil); err != nil {
		t.Fatalf("after set: %v", err)
	}
	record := backing.records[HKey("users", "alice")]
	if record.WALState != store.WALStateCommitted || !record.FlushMySQL {
		t.Fatalf("expected committed flushable record, got %#v", record)
	}
	if backing.committedCount != 1 || backing.abortedCount != 0 {
		t.Fatalf("expected exactly one commit, no abort; got commits=%d aborts=%d",
			backing.committedCount, backing.abortedCount)
	}
}

func TestDurableHookAfterSetAbortsOnMutationError(t *testing.T) {
	backing := newRecordingCommitStore()
	hook := newTestDurableHook(t, backing, fenceAt(7, 3))
	entry := newTestEntry("alice", "A", 0, 42)

	op, err := hook.BeforeSet(context.Background(), olricconfig.DurableOperation{
		DMap: "users", Key: "alice", HKey: HKey("users", "alice"), Entry: entry,
	})
	if err != nil {
		t.Fatalf("before set: %v", err)
	}
	mutErr := errors.New("olric quorum failed")
	if err := hook.AfterSet(context.Background(), op, mutErr); err != nil {
		t.Fatalf("after set with mutation error: %v", err)
	}
	if _, exists := backing.records[HKey("users", "alice")]; exists {
		t.Fatalf("aborted prepare must remove WAL record, still present")
	}
	if backing.abortedCount != 1 || backing.committedCount != 0 {
		t.Fatalf("expected exactly one abort, no commit; got commits=%d aborts=%d",
			backing.committedCount, backing.abortedCount)
	}
}

func TestDurableHookBeforeDeleteWritesFencedTombstone(t *testing.T) {
	backing := newRecordingCommitStore()
	hook := newTestDurableHook(t, backing, fenceAt(11, 4))

	op, err := hook.BeforeDelete(context.Background(), olricconfig.DurableOperation{
		DMap: "users", Key: "alice", HKey: HKey("users", "alice"),
	})
	if err != nil {
		t.Fatalf("before delete: %v", err)
	}
	if op.Version == 0 {
		t.Fatal("expected non-zero op.Version after prepare-delete")
	}
	record := backing.records[HKey("users", "alice")]
	if !record.Tombstone {
		t.Fatalf("expected tombstone, got %#v", record)
	}
	if record.Generation != 11 || record.Epoch != 4 || record.OwnerSeq != 1 {
		t.Fatalf("expected fence (11,4,1) on tombstone, got (%d,%d,%d)",
			record.Generation, record.Epoch, record.OwnerSeq)
	}
	if record.WALState != store.WALStatePrepared {
		t.Fatalf("expected prepared tombstone state, got %q", record.WALState)
	}
}

func TestDurableHookAfterDeleteAbortsOnMutationError(t *testing.T) {
	backing := newRecordingCommitStore()
	hook := newTestDurableHook(t, backing, fenceAt(11, 4))

	op, err := hook.BeforeDelete(context.Background(), olricconfig.DurableOperation{
		DMap: "users", Key: "alice", HKey: HKey("users", "alice"),
	})
	if err != nil {
		t.Fatalf("before delete: %v", err)
	}
	mutErr := errors.New("delete quorum failed")
	if err := hook.AfterDelete(context.Background(), op, mutErr); err != nil {
		t.Fatalf("after delete: %v", err)
	}
	if _, exists := backing.records[HKey("users", "alice")]; exists {
		t.Fatal("aborted tombstone prepare must vanish")
	}
}

func TestDurableHookBeforeExpireWritesFencedFlushableRecord(t *testing.T) {
	backing := newRecordingCommitStore()
	hook := newTestDurableHook(t, backing, fenceAt(2, 9))
	entry := newTestEntry("alice", "A", time.Now().Add(time.Hour).UnixMilli(), 43)

	op, err := hook.BeforeExpire(context.Background(), olricconfig.DurableOperation{
		DMap: "users", Key: "alice", HKey: HKey("users", "alice"), Entry: entry,
	})
	if err != nil {
		t.Fatalf("before expire: %v", err)
	}
	record := backing.records[HKey("users", "alice")]
	if record.Origin != "client_expire" {
		t.Fatalf("expected client_expire origin, got %q", record.Origin)
	}
	if record.Generation != 2 || record.Epoch != 9 || record.OwnerSeq != 1 {
		t.Fatalf("expected fence (2,9,1) on expire, got (%d,%d,%d)",
			record.Generation, record.Epoch, record.OwnerSeq)
	}
	if err := hook.AfterExpire(context.Background(), op, nil); err != nil {
		t.Fatalf("after expire: %v", err)
	}
	record = backing.records[HKey("users", "alice")]
	if record.WALState != store.WALStateCommitted || !record.FlushMySQL {
		t.Fatalf("expected committed flushable expire record, got %#v", record)
	}
}

func TestDurableHookFenceSequenceIsMonotonic(t *testing.T) {
	backing := newRecordingCommitStore()
	hook := newTestDurableHook(t, backing, fenceAt(1, 1))

	for i := 0; i < 3; i++ {
		entry := newTestEntry("k", "v", 0, int64(i))
		op, err := hook.BeforeSet(context.Background(), olricconfig.DurableOperation{
			DMap: "users", Key: "alice", HKey: HKey("users", "alice"), Entry: entry,
		})
		if err != nil {
			t.Fatalf("before set #%d: %v", i, err)
		}
		if err := hook.AfterSet(context.Background(), op, nil); err != nil {
			t.Fatalf("after set #%d: %v", i, err)
		}
	}
	record := backing.records[HKey("users", "alice")]
	if record.Generation != 1 || record.Epoch != 1 || record.OwnerSeq != 3 {
		t.Fatalf("expected fence (1,1,3) after 3 stamps, got (%d,%d,%d)",
			record.Generation, record.Epoch, record.OwnerSeq)
	}
}

func TestDurableHookLoadOnMissDecodesBackingEntry(t *testing.T) {
	backing := newRecordingCommitStore()
	hook := newTestDurableHook(t, backing, fenceAt(1, 1))
	recordEntry := newTestEntry("alice", "A", time.Now().Add(time.Minute).UnixMilli(), 44)
	backing.records[HKey("users", "alice")] = store.EntryRecord{
		DMap:         "users",
		Key:          "alice",
		HKey:         HKey("users", "alice"),
		EncodedEntry: recordEntry.Encode(),
		TTL:          recordEntry.TTL(),
		Timestamp:    recordEntry.Timestamp(),
	}

	template := newTestEntry("", "", 0, 0)
	loaded, err := hook.LoadOnMiss(context.Background(), olricconfig.DurableOperation{
		DMap: "users", Key: "alice", HKey: HKey("users", "alice"), Entry: template,
	})
	if err != nil {
		t.Fatalf("load on miss: %v", err)
	}
	if loaded.Key() != "alice" || string(loaded.Value()) != "A" {
		t.Fatalf("unexpected loaded entry: key=%q value=%q", loaded.Key(), loaded.Value())
	}
	// LoadOnMiss must not produce WAL writes — refill safety is enforced by
	// the fork's fragment-locked re-check, not by hook-side WAL stamping.
	if backing.committedCount != 0 || backing.abortedCount != 0 {
		t.Fatalf("LoadOnMiss must not commit/abort, got commits=%d aborts=%d",
			backing.committedCount, backing.abortedCount)
	}
}

func TestDurableHookLoadOnMissPropagatesNotFound(t *testing.T) {
	backing := newRecordingCommitStore()
	hook := newTestDurableHook(t, backing, fenceAt(1, 1))

	_, err := hook.LoadOnMiss(context.Background(), olricconfig.DurableOperation{
		DMap: "users", Key: "missing", HKey: HKey("users", "missing"),
		Entry: newTestEntry("", "", 0, 0),
	})
	if !errors.Is(err, olricstorage.ErrKeyNotFound) {
		t.Fatalf("expected backing miss, got %v", err)
	}
}

func newTestDurableHook(t *testing.T, backing store.CommitStore, lease *fakeFenceLease) *DurableHook {
	t.Helper()
	hook, err := NewDurableHook(backing, lease, newFakeFenceSequencer())
	if err != nil {
		t.Fatalf("new durable hook: %v", err)
	}
	return hook
}

func newTestEntry(key, value string, ttl, timestamp int64) *testEntry {
	return &testEntry{
		key:       key,
		value:     []byte(value),
		ttl:       ttl,
		timestamp: timestamp,
	}
}

// fenceAt creates a fakeFenceLease that returns a valid (g, e) snapshot.
// g=0 means the lease is invalid.
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

// VerifyFence mirrors LeaseTracker.VerifyFence for tests. It refuses when
// the lease is not currently servable, the snapshotted generation does not
// match (leadership change), or the current epoch has fallen behind the
// snapshotted one (impossible in production but trivial to detect here).
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

func recordHasFlushIntent(r store.EntryRecord) bool {
	// During prepare phase, FlushMySQL is intentionally false in the WAL
	// representation; the original "intent" is encoded by Origin so we just
	// confirm the record was created with a client-origin tag.
	return r.Origin == "client_set" || r.Origin == "client_expire" || r.Origin == "client_delete"
}

// TestDurableHookVerifyAfterLockPassesWhenFenceUnchanged covers the steady-
// state case: BeforeSet stamps a fence, the fork acquires the fragment lock,
// VerifyAfterLock confirms the fence is still current, and the prepared
// record is committed normally by AfterSet.
func TestDurableHookVerifyAfterLockPassesWhenFenceUnchanged(t *testing.T) {
	backing := newRecordingCommitStore()
	hook := newTestDurableHook(t, backing, fenceAt(7, 3))

	op, err := hook.BeforeSet(context.Background(), olricconfig.DurableOperation{
		DMap:  "users",
		Key:   "alice",
		HKey:  HKey("users", "alice"),
		Entry: newTestEntry("alice", "A", 0, 1),
	})
	if err != nil {
		t.Fatalf("before set: %v", err)
	}
	if op.FenceGeneration != 7 || op.FenceEpoch != 3 {
		t.Fatalf("expected fence (7,3) on op, got (%d,%d)", op.FenceGeneration, op.FenceEpoch)
	}

	if err := hook.VerifyAfterLock(context.Background(), op); err != nil {
		t.Fatalf("verify: %v", err)
	}
	if err := hook.AfterSet(context.Background(), op, nil); err != nil {
		t.Fatalf("after set: %v", err)
	}
}

// TestDurableHookVerifyAfterLockRejectsLeaseRevocation covers the failover
// race: BeforeSet stamps a fence, then the lease is revoked (or leadership
// moves) before the fork acquires the fragment lock. VerifyAfterLock must
// refuse so the fork can abort the prepared record without acknowledging
// the write.
func TestDurableHookVerifyAfterLockRejectsLeaseRevocation(t *testing.T) {
	backing := newRecordingCommitStore()
	lease := fenceAt(7, 3)
	hook := newTestDurableHook(t, backing, lease)

	op, err := hook.BeforeSet(context.Background(), olricconfig.DurableOperation{
		DMap:  "users",
		Key:   "alice",
		HKey:  HKey("users", "alice"),
		Entry: newTestEntry("alice", "A", 0, 1),
	})
	if err != nil {
		t.Fatalf("before set: %v", err)
	}

	// Simulate a demote between prepare and lock acquisition.
	lease.ok = false

	if err := hook.VerifyAfterLock(context.Background(), op); !errors.Is(err, ErrLeaseExpired) {
		t.Fatalf("expected ErrLeaseExpired from verify, got %v", err)
	}

	// The fork would now call AfterSet(op, vErr) which routes to AbortEntry.
	if err := hook.AfterSet(context.Background(), op, ErrLeaseExpired); err != nil {
		t.Fatalf("after set with verify error: %v", err)
	}
	for ref := range backing.records {
		t.Fatalf("aborted prepare must not leave a WAL record, found %v", ref)
	}
}

// TestDurableHookVerifyAfterLockRejectsGenerationChange catches the more
// subtle failover: lease still says "valid", but the new envelope advanced
// the generation (i.e. a new PRIMARY took over). The prepared record's fence
// is from the OLD primary and must not be acknowledged.
func TestDurableHookVerifyAfterLockRejectsGenerationChange(t *testing.T) {
	backing := newRecordingCommitStore()
	lease := fenceAt(7, 3)
	hook := newTestDurableHook(t, backing, lease)

	op, err := hook.BeforeSet(context.Background(), olricconfig.DurableOperation{
		DMap:  "users",
		Key:   "alice",
		HKey:  HKey("users", "alice"),
		Entry: newTestEntry("alice", "A", 0, 1),
	})
	if err != nil {
		t.Fatalf("before set: %v", err)
	}

	// New primary advanced the generation. lease.ok is still true.
	lease.g = 8
	lease.e = 0

	if err := hook.VerifyAfterLock(context.Background(), op); !errors.Is(err, ErrLeaseExpired) {
		t.Fatalf("expected ErrLeaseExpired on generation change, got %v", err)
	}
}
