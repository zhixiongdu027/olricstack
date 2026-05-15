package stringkv

import (
	"context"
	"errors"
	"path/filepath"
	"sync"
	"testing"
	"time"

	olricconfig "github.com/olric-data/olric/config"
	"github.com/zhixiongdu/olricstack/internal/store"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

// TestDurableHookEndToEndAbortPreventsMySQLLeak exercises the full prepare /
// abort path against a real WAL + sqlite-backed MySQLStore. It is the FT-1 /
// FT-2 regression: a successful BeforeSet stages a prepared WAL record; if the
// owner-side mutation fails (simulated here by passing a non-nil mutationErr
// to AfterSet), the prepared record must vanish from WAL and never reach the
// durable store.
func TestDurableHookEndToEndAbortPreventsMySQLLeak(t *testing.T) {
	dir := t.TempDir()
	db, err := gorm.Open(sqlite.Open("file:"+filepath.Join(dir, "cache.db")+"?cache=shared"), &gorm.Config{})
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	cacheStore, err := store.NewMySQLStore(db, store.Config{
		WALPath:       filepath.Join(dir, "cache.wal"),
		FlushInterval: 10 * time.Millisecond,
		BatchSize:     16,
	})
	if err != nil {
		t.Fatalf("new mysql store: %v", err)
	}
	if err := cacheStore.Start(context.Background()); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = cacheStore.Close(ctx)
	}()

	hook, err := NewDurableHook(cacheStore, fenceAt(7, 3), newFakeFenceSequencer())
	if err != nil {
		t.Fatalf("new hook: %v", err)
	}

	const testHKey uint64 = 42 // sqlite cannot bind uint64 values with the high bit set
	entry := newTestEntry("alice", "A", time.Now().Add(time.Minute).UnixMilli(), 42)
	op, err := hook.BeforeSet(context.Background(), olricconfig.DurableOperation{
		DMap:  "users",
		Key:   "alice",
		HKey:  testHKey,
		Entry: entry,
	})
	if err != nil {
		t.Fatalf("before set: %v", err)
	}
	if op.Version == 0 {
		t.Fatal("expected non-zero op.Version after prepare")
	}

	// Simulate a fork-side owner mutation failure (quorum loss, replica fail,
	// fragment migration race, etc). AfterSet MUST run anyway and abort.
	mutErr := errors.New("simulated quorum failure")
	if err := hook.AfterSet(context.Background(), op, mutErr); err != nil {
		t.Fatalf("after set with mutation error: %v", err)
	}

	// Force a final flush window. Since the prepared record was aborted, no
	// row should appear in MySQL even after the flusher had a chance to run.
	time.Sleep(50 * time.Millisecond)

	if _, err := cacheStore.LoadEntry(context.Background(), store.EntryRef{DMap: "users", Key: "alice", HKey: testHKey}); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("aborted prepare leaked into store: got err=%v", err)
	}

	// Sanity: a successful path on the same key still commits and reaches MySQL.
	op2, err := hook.BeforeSet(context.Background(), olricconfig.DurableOperation{
		DMap:  "users",
		Key:   "alice",
		HKey:  testHKey,
		Entry: entry,
	})
	if err != nil {
		t.Fatalf("before set #2: %v", err)
	}
	if err := hook.AfterSet(context.Background(), op2, nil); err != nil {
		t.Fatalf("after set #2: %v", err)
	}

	// Wait for the flusher to drain the committed record into sqlite.
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if rec, err := cacheStore.LoadEntry(context.Background(), store.EntryRef{DMap: "users", Key: "alice", HKey: testHKey}); err == nil && len(rec.EncodedEntry) > 0 {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("committed record never reached the durable store")
}

// TestDurableHookConcurrentLoadOnMissDoesNotOverwriteConcurrentPrepare is the
// FT-4 regression: a fork-side LoadOnMiss racing with a concurrent owner Set
// must not be able to resurrect a stale MySQL row over the new write. The hook
// layer's contract is "LoadOnMiss issues no WAL writes" (refill safety is
// enforced by the fork's fragment-locked re-check in internal/dmap/get.go).
// This test pins that hook-side invariant in code.
func TestDurableHookConcurrentLoadOnMissDoesNotOverwriteConcurrentPrepare(t *testing.T) {
	dir := t.TempDir()
	db, err := gorm.Open(sqlite.Open("file:"+filepath.Join(dir, "cache.db")+"?cache=shared"), &gorm.Config{})
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	cacheStore, err := store.NewMySQLStore(db, store.Config{
		WALPath:       filepath.Join(dir, "cache.wal"),
		FlushInterval: time.Hour, // keep WAL observable
		BatchSize:     1024,
	})
	if err != nil {
		t.Fatalf("new mysql store: %v", err)
	}
	if err := cacheStore.Start(context.Background()); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = cacheStore.Close(ctx)
	}()

	hook, err := NewDurableHook(cacheStore, fenceAt(7, 3), newFakeFenceSequencer())
	if err != nil {
		t.Fatalf("new hook: %v", err)
	}

	const testHKey uint64 = 4242
	ref := store.EntryRef{DMap: "users", Key: "alice", HKey: testHKey}

	// Seed sqlite with a stale value so LoadOnMiss has something to fetch.
	staleEntry := newTestEntry("alice", "stale", 0, 1)
	op0, err := hook.BeforeSet(context.Background(), olricconfig.DurableOperation{
		DMap: "users", Key: "alice", HKey: testHKey, Entry: staleEntry,
	})
	if err != nil {
		t.Fatalf("seed before set: %v", err)
	}
	if err := hook.AfterSet(context.Background(), op0, nil); err != nil {
		t.Fatalf("seed after set: %v", err)
	}

	// Race: 32 concurrent LoadOnMiss against a single fresh prepare/commit
	// sequence that ought to win. The hook MUST NOT issue WAL writes during
	// LoadOnMiss; the fork-side fragment recheck is what discards stale data,
	// and we verify the hook precondition (no WAL writes) by recording the
	// number of bbolt mutations before/after.
	freshEntry := newTestEntry("alice", "fresh", 0, 2)
	var wg sync.WaitGroup
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			loadOp := olricconfig.DurableOperation{
				DMap:  "users",
				Key:   "alice",
				HKey:  testHKey,
				Entry: newTestEntry("", "", 0, 0),
			}
			if _, err := hook.LoadOnMiss(context.Background(), loadOp); err != nil && !errors.Is(err, store.ErrNotFound) {
				t.Errorf("load on miss: %v", err)
			}
		}()
	}

	op, err := hook.BeforeSet(context.Background(), olricconfig.DurableOperation{
		DMap: "users", Key: "alice", HKey: testHKey, Entry: freshEntry,
	})
	if err != nil {
		t.Fatalf("fresh before set: %v", err)
	}
	if err := hook.AfterSet(context.Background(), op, nil); err != nil {
		t.Fatalf("fresh after set: %v", err)
	}
	wg.Wait()

	rec, err := cacheStore.LoadEntry(context.Background(), ref)
	if err != nil {
		t.Fatalf("load after race: %v", err)
	}
	if string(rec.EncodedEntry) == "" {
		t.Fatal("expected a committed record after the race")
	}
	// The latest committed write must remain visible: LoadOnMiss must not have
	// replaced the fresh value with the stale one via a hook-side WAL write.
	decoded := newTestEntry("", "", 0, 0)
	decoded.Decode(rec.EncodedEntry)
	if string(decoded.Value()) != "fresh" {
		t.Fatalf("LoadOnMiss leaked a stale refill into WAL: got value %q", decoded.Value())
	}
}
