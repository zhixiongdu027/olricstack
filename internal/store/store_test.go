package store

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

func TestMySQLStoreLoadMiss(t *testing.T) {
	cacheStore := newTestStore(t, Config{})

	_, err := cacheStore.LoadEntry(context.Background(), testRef("missing", 1))
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected ErrNotFound, got %v", err)
	}
	closeStore(t, cacheStore)
}

func TestMySQLStoreCoalescesAndFlushesLatestEntry(t *testing.T) {
	db := newTestDB(t)
	walPath := filepath.Join(t.TempDir(), "cache.wal")
	cacheStore := newTestStoreWithDB(t, db, Config{
		WALPath:       walPath,
		FlushInterval: time.Hour,
		BatchSize:     16,
	})

	ctx := context.Background()
	if err := cacheStore.StoreEntry(ctx, testFlushableRecord("user:1", 11, []byte("stale"))); err != nil {
		t.Fatalf("store stale: %v", err)
	}
	if err := cacheStore.StoreEntry(ctx, testFlushableRecord("user:1", 11, []byte("fresh"))); err != nil {
		t.Fatalf("store fresh: %v", err)
	}

	closeStore(t, cacheStore)

	reopened := newTestStoreWithDB(t, db, Config{WALPath: filepath.Join(t.TempDir(), "reopen.wal")})
	defer closeStore(t, reopened)
	record, err := reopened.LoadEntry(ctx, testRef("user:1", 11))
	if err != nil {
		t.Fatalf("load flushed value: %v", err)
	}
	if string(record.EncodedEntry) != "fresh" {
		t.Fatalf("expected latest value, got %q", record.EncodedEntry)
	}
}

func TestMySQLStoreFlushesWhenBatchSizeReached(t *testing.T) {
	cacheStore := newTestStore(t, Config{
		FlushInterval: time.Hour,
		BatchSize:     2,
	})

	ctx := context.Background()
	if err := cacheStore.StoreEntry(ctx, testFlushableRecord("a", 1, []byte("1"))); err != nil {
		t.Fatalf("store a: %v", err)
	}
	if err := cacheStore.StoreEntry(ctx, testFlushableRecord("b", 2, []byte("2"))); err != nil {
		t.Fatalf("store b: %v", err)
	}

	eventually(t, func() bool {
		record, err := cacheStore.LoadEntry(ctx, testRef("b", 2))
		return err == nil && string(record.EncodedEntry) == "2"
	})
	closeStore(t, cacheStore)
}

func TestMySQLStoreRejectsWhenDirtyQueueFull(t *testing.T) {
	cacheStore := newTestStore(t, Config{
		QueueSize:     1,
		FlushInterval: time.Hour,
		BatchSize:     128,
	})

	err := cacheStore.StoreEntry(context.Background(), testRecord("a", 1, []byte("1")))
	if err != nil {
		t.Fatalf("first store should fit queue: %v", err)
	}

	err = cacheStore.StoreEntry(context.Background(), testRecord("b", 2, []byte("2")))
	if !errors.Is(err, ErrQueueFull) {
		t.Fatalf("expected ErrQueueFull, got %v", err)
	}
	closeStore(t, cacheStore)
}

func TestMySQLStoreCloseReportsFinalFlushFailure(t *testing.T) {
	cacheStore := newTestStore(t, Config{
		FlushInterval: time.Hour,
		BatchSize:     128,
	})
	if err := cacheStore.StoreEntry(context.Background(), testFlushableRecord("a", 1, []byte("1"))); err != nil {
		t.Fatalf("store: %v", err)
	}

	sqlDB, err := cacheStore.db.DB()
	if err != nil {
		t.Fatalf("get sql db: %v", err)
	}
	if err := sqlDB.Close(); err != nil {
		t.Fatalf("close sql db: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := cacheStore.Close(ctx); err == nil {
		t.Fatal("expected close to report final flush failure")
	}
}

func TestMySQLStoreFlushBackoffAfterFailure(t *testing.T) {
	cacheStore := newTestStore(t, Config{
		FlushBackoff:  time.Hour,
		FlushInterval: time.Hour,
	})

	sqlDB, err := cacheStore.db.DB()
	if err != nil {
		t.Fatalf("get sql db: %v", err)
	}
	if err := sqlDB.Close(); err != nil {
		t.Fatalf("close sql db: %v", err)
	}

	entries := []dirtyEntry{{Record: testFencedRecord("a", 1, []byte("1"), 1, 1, 1)}}
	if cacheStore.flushEntries(entries) {
		t.Fatal("expected flush failure when underlying DB is closed")
	}
	cacheStore.deferFlush()
	if cacheStore.flushWAL(false) {
		t.Fatal("flush should be deferred after failure")
	}
}

// TestConfigDefaultsApplyFlushTimeout pins the new FlushTimeout default. A
// zero-valued FlushTimeout would silently restore the pre-fix behaviour
// (CreateInBatches with no context), so this guards the regression boundary.
func TestConfigDefaultsApplyFlushTimeout(t *testing.T) {
	cfg := Config{}.withDefaults()
	if cfg.FlushTimeout <= 0 {
		t.Fatalf("FlushTimeout default must be positive, got %s", cfg.FlushTimeout)
	}
	explicit := Config{FlushTimeout: 5 * time.Second}.withDefaults()
	if explicit.FlushTimeout != 5*time.Second {
		t.Fatalf("explicit FlushTimeout must survive defaults, got %s", explicit.FlushTimeout)
	}
}

// TestPebbleMetricsExposed pins the observability surface added for §3.1.
// The metrics handle must be non-nil while the store is open and nil after
// Close so a leaked Prometheus scrape cannot read freed memory.
func TestPebbleMetricsExposed(t *testing.T) {
	cacheStore := newTestStore(t, Config{FlushInterval: time.Hour})
	if m := cacheStore.PebbleMetrics(); m == nil {
		t.Fatal("PebbleMetrics returned nil while store is open")
	}
	closeStore(t, cacheStore)
	if m := cacheStore.PebbleMetrics(); m != nil {
		t.Fatalf("PebbleMetrics must return nil after close, got %#v", m)
	}
}

func TestMySQLStoreRejectsStoreAfterClose(t *testing.T) {
	cacheStore := newTestStore(t, Config{})
	closeStore(t, cacheStore)

	err := cacheStore.StoreEntry(context.Background(), testRecord("a", 1, []byte("1")))
	if err == nil {
		t.Fatal("expected store after close to fail")
	}
}

func TestMySQLStoreCloseFlushesAcceptedWrites(t *testing.T) {
	db := newTestDB(t)
	cacheStore := newTestStoreWithDB(t, db, Config{
		QueueSize:     128,
		FlushInterval: time.Hour,
		BatchSize:     1024,
	})

	ctx := context.Background()
	for i := 0; i < 64; i++ {
		key := fmt.Sprintf("key-%d", i)
		if err := cacheStore.StoreEntry(ctx, testFlushableRecord(key, uint64(i+1), []byte("value"))); err != nil {
			t.Fatalf("store %s: %v", key, err)
		}
	}
	closeStore(t, cacheStore)

	reopened := newTestStoreWithDB(t, db, Config{WALPath: filepath.Join(t.TempDir(), "reopen.wal")})
	defer closeStore(t, reopened)
	record, err := reopened.LoadEntry(ctx, testRef("key-0", 1))
	if err != nil {
		t.Fatalf("load flushed value: %v", err)
	}
	if string(record.EncodedEntry) != "value" {
		t.Fatalf("expected flushed value, got %q", record.EncodedEntry)
	}
}

func TestMySQLStoreWALSurvivesProcessRestart(t *testing.T) {
	db := newTestDB(t)
	walPath := filepath.Join(t.TempDir(), "cache.wal")

	first, err := NewMySQLStore(db, Config{
		WALPath:       walPath,
		FlushInterval: time.Hour,
		BatchSize:     128,
	})
	if err != nil {
		t.Fatalf("new first store: %v", err)
	}
	if err := first.Start(context.Background()); err != nil {
		t.Fatalf("start first store: %v", err)
	}
	if err := first.StoreEntry(context.Background(), testFlushableRecord("durable", 44, []byte("value"))); err != nil {
		t.Fatalf("store dirty value: %v", err)
	}
	first.mu.Lock()
	first.closing = true
	first.mu.Unlock()
	if err := first.wal.Close(); err != nil {
		t.Fatalf("simulate process death closing wal: %v", err)
	}

	second, err := NewMySQLStore(db, Config{
		WALPath:       walPath,
		FlushInterval: time.Hour,
		BatchSize:     128,
	})
	if err != nil {
		t.Fatalf("new second store: %v", err)
	}
	if err := second.Start(context.Background()); err != nil {
		t.Fatalf("start second store: %v", err)
	}
	closeStore(t, second)

	reader := newTestStoreWithDB(t, db, Config{WALPath: filepath.Join(t.TempDir(), "reader.wal")})
	defer closeStore(t, reader)
	record, err := reader.LoadEntry(context.Background(), testRef("durable", 44))
	if err != nil {
		t.Fatalf("load recovered value: %v", err)
	}
	if string(record.EncodedEntry) != "value" {
		t.Fatalf("expected recovered value, got %q", record.EncodedEntry)
	}
}

func TestMySQLStoreLoadPrefersLocalWALBeforeMySQL(t *testing.T) {
	cacheStore := newTestStore(t, Config{FlushInterval: time.Hour})
	if err := cacheStore.flushEntries([]dirtyEntry{{Record: testFencedRecord("split", 7, []byte("mysql"), 1, 1, 1)}}); !err {
		t.Fatalf("seed mysql: %v", cacheStore.workerError())
	}
	if err := cacheStore.StoreEntry(context.Background(), testRecord("split", 7, []byte("wal"))); err != nil {
		t.Fatalf("store wal value: %v", err)
	}

	record, err := cacheStore.LoadEntry(context.Background(), testRef("split", 7))
	if err != nil {
		t.Fatalf("load value: %v", err)
	}
	if string(record.EncodedEntry) != "wal" {
		t.Fatalf("expected WAL value, got %q", record.EncodedEntry)
	}
	closeStore(t, cacheStore)
}

func TestMySQLStoreLocalRefillDoesNotOverwriteFlushableDirtyRecord(t *testing.T) {
	cacheStore := newTestStore(t, Config{FlushInterval: time.Hour})
	if err := cacheStore.StoreEntry(context.Background(), testFlushableRecord("refill", 16, []byte("dirty"))); err != nil {
		t.Fatalf("store dirty value: %v", err)
	}
	if err := cacheStore.StoreEntry(context.Background(), EntryRecord{
		DMap:         "users",
		Key:          "refill",
		HKey:         16,
		EncodedEntry: []byte("mysql"),
		Origin:       "mysql_refill",
		FlushMySQL:   false,
		WALState:     WALStateLocal,
	}); err != nil {
		t.Fatalf("store local refill: %v", err)
	}

	record, err := cacheStore.LoadEntry(context.Background(), testRef("refill", 16))
	if err != nil {
		t.Fatalf("load dirty value: %v", err)
	}
	if string(record.EncodedEntry) != "dirty" {
		t.Fatalf("expected flushable dirty value to win, got %q", record.EncodedEntry)
	}
	closeStore(t, cacheStore)
}

func TestMySQLStoreDoesNotFlushLocalOnlyEntries(t *testing.T) {
	db := newTestDB(t)
	walPath := filepath.Join(t.TempDir(), "cache.wal")
	cacheStore := newTestStoreWithDB(t, db, Config{
		WALPath:       walPath,
		FlushInterval: time.Hour,
		BatchSize:     16,
	})

	if err := cacheStore.StoreEntry(context.Background(), testRecord("local-only", 31, []byte("value"))); err != nil {
		t.Fatalf("store local-only value: %v", err)
	}
	closeStore(t, cacheStore)

	reader := newTestStoreWithDB(t, db, Config{WALPath: filepath.Join(t.TempDir(), "reader.wal")})
	defer closeStore(t, reader)
	if _, err := reader.LoadEntry(context.Background(), testRef("local-only", 31)); !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected local-only record not to flush to mysql, got %v", err)
	}

	replayed, err := NewWALStore(Config{WALPath: walPath})
	if err != nil {
		t.Fatalf("reopen wal: %v", err)
	}
	defer closeStore(t, replayed)
	record, err := replayed.LoadEntry(context.Background(), testRef("local-only", 31))
	if err != nil {
		t.Fatalf("expected local-only WAL value to remain: %v", err)
	}
	if string(record.EncodedEntry) != "value" {
		t.Fatalf("unexpected local-only value: %q", record.EncodedEntry)
	}
}

// TestMySQLStoreSweepsPreparedOrphansAtStart guards §8.1 hardening #1: a
// prepared record that survives a crash (or a verify-fail abort that itself
// failed to write) must not linger across restart. We simulate the crash by
// preparing a record, then bypassing the normal Close path so the WAL keeps
// the Prepared entry on disk, then re-open and assert the sweep removed it.
func TestMySQLStoreSweepsPreparedOrphansAtStart(t *testing.T) {
	db := newTestDB(t)
	walPath := filepath.Join(t.TempDir(), "cache.wal")

	first, err := NewMySQLStore(db, Config{
		WALPath:       walPath,
		FlushInterval: time.Hour,
		BatchSize:     128,
	})
	if err != nil {
		t.Fatalf("new first store: %v", err)
	}
	if err := first.Start(context.Background()); err != nil {
		t.Fatalf("start first store: %v", err)
	}
	if _, err := first.PrepareEntry(context.Background(), testFlushableRecord("orphan", 51, []byte("value"))); err != nil {
		t.Fatalf("prepare orphan: %v", err)
	}
	count, err := first.PreparedCount()
	if err != nil {
		t.Fatalf("count prepared in first: %v", err)
	}
	if count != 1 {
		t.Fatalf("expected 1 prepared record before crash, got %d", count)
	}
	// Simulate crash: skip flushLoop drain and Close path; just close the WAL.
	first.mu.Lock()
	first.closing = true
	first.mu.Unlock()
	if err := first.wal.Close(); err != nil {
		t.Fatalf("close first wal: %v", err)
	}

	second, err := NewMySQLStore(db, Config{
		WALPath:       walPath,
		FlushInterval: time.Hour,
		BatchSize:     128,
	})
	if err != nil {
		t.Fatalf("new second store: %v", err)
	}
	if err := second.Start(context.Background()); err != nil {
		t.Fatalf("start second store: %v", err)
	}
	defer closeStore(t, second)

	count, err = second.PreparedCount()
	if err != nil {
		t.Fatalf("count prepared after sweep: %v", err)
	}
	if count != 0 {
		t.Fatalf("expected sweep to remove prepared orphan, %d remain", count)
	}
	if _, err := second.LoadEntry(context.Background(), testRef("orphan", 51)); !errors.Is(err, ErrNotFound) {
		t.Fatalf("swept orphan still loadable: %v", err)
	}
}

// TestMySQLStorePreparedCountReflectsLifecycle covers §8.1 hardening #2: the
// gauge must rise on Prepare and fall on Commit/Abort so operators can alert
// on stuck preparations.
func TestMySQLStorePreparedCountReflectsLifecycle(t *testing.T) {
	cacheStore := newTestStore(t, Config{FlushInterval: time.Hour})
	defer closeStore(t, cacheStore)

	count, err := cacheStore.PreparedCount()
	if err != nil {
		t.Fatalf("initial count: %v", err)
	}
	if count != 0 {
		t.Fatalf("expected 0 initially, got %d", count)
	}

	prepared, err := cacheStore.PrepareEntry(context.Background(), testFlushableRecord("alpha", 61, []byte("v1")))
	if err != nil {
		t.Fatalf("prepare alpha: %v", err)
	}
	aborted, err := cacheStore.PrepareEntry(context.Background(), testFlushableRecord("beta", 62, []byte("v2")))
	if err != nil {
		t.Fatalf("prepare beta: %v", err)
	}

	count, err = cacheStore.PreparedCount()
	if err != nil {
		t.Fatalf("count after prepare: %v", err)
	}
	if count != 2 {
		t.Fatalf("expected 2 prepared, got %d", count)
	}

	if err := cacheStore.CommitEntry(context.Background(), prepared.Ref(), prepared.WALSeq); err != nil {
		t.Fatalf("commit alpha: %v", err)
	}
	if err := cacheStore.AbortEntry(context.Background(), aborted.Ref(), aborted.WALSeq); err != nil {
		t.Fatalf("abort beta: %v", err)
	}

	count, err = cacheStore.PreparedCount()
	if err != nil {
		t.Fatalf("count after finalize: %v", err)
	}
	if count != 0 {
		t.Fatalf("expected 0 after commit+abort, got %d", count)
	}
}

func TestMySQLStoreDoesNotFlushPreparedEntriesBeforeCommit(t *testing.T) {
	db := newTestDB(t)
	walPath := filepath.Join(t.TempDir(), "cache.wal")
	cacheStore := newTestStoreWithDB(t, db, Config{
		WALPath:       walPath,
		FlushInterval: time.Hour,
		BatchSize:     16,
	})

	prepared, err := cacheStore.PrepareEntry(context.Background(), testFlushableRecord("prepared", 32, []byte("value")))
	if err != nil {
		t.Fatalf("prepare entry: %v", err)
	}
	if prepared.WALState != WALStatePrepared || prepared.FlushMySQL {
		t.Fatalf("expected unflushable prepared record, got %#v", prepared)
	}
	if _, err := cacheStore.LoadEntry(context.Background(), testRef("prepared", 32)); !errors.Is(err, ErrNotFound) {
		t.Fatalf("prepared record must not be visible before commit, got %v", err)
	}
	if cacheStore.flushWAL(true) {
		reader := newTestStoreWithDB(t, db, Config{WALPath: filepath.Join(t.TempDir(), "reader-before.wal")})
		if _, err := reader.LoadEntry(context.Background(), testRef("prepared", 32)); !errors.Is(err, ErrNotFound) {
			t.Fatalf("prepared record flushed before commit: %v", err)
		}
		closeStore(t, reader)
	}

	if err := cacheStore.CommitEntry(context.Background(), prepared.Ref(), prepared.WALSeq); err != nil {
		t.Fatalf("commit entry: %v", err)
	}
	closeStore(t, cacheStore)

	reader := newTestStoreWithDB(t, db, Config{WALPath: filepath.Join(t.TempDir(), "reader-after.wal")})
	defer closeStore(t, reader)
	record, err := reader.LoadEntry(context.Background(), testRef("prepared", 32))
	if err != nil {
		t.Fatalf("load committed value: %v", err)
	}
	if string(record.EncodedEntry) != "value" {
		t.Fatalf("unexpected committed value: %q", record.EncodedEntry)
	}
}

// TestMySQLStoreLateFlushFromPreviousOwnerLosesToCurrentOwner is the FT-3/FT-6
// regression. It mimics the production race that motivated owner-fenced
// versioning: an old owner with generation=N writes and commits a record
// locally but its flusher stalls (e.g. MySQL outage, WAL IO stuck behind a
// fsync). Meanwhile a new primary elects a new owner under generation=N+1
// which writes and successfully flushes its own value. When the old owner's
// flusher eventually drains, the fence triple comparison must reject the
// stale row, leaving MySQL holding the new owner's value.
func TestMySQLStoreLateFlushFromPreviousOwnerLosesToCurrentOwner(t *testing.T) {
	cacheStore := newTestStore(t, Config{})
	defer closeStore(t, cacheStore)

	// New owner under generation=2 writes and reaches MySQL first.
	newOwner := testFencedRecord("user:1", 7, []byte("from-new-owner"), 2, 1, 1)
	if !cacheStore.flushEntries([]dirtyEntry{{Record: newOwner}}) {
		t.Fatalf("flush new owner record: %v", cacheStore.workerError())
	}

	// Old owner under generation=1 had committed locally before failover and
	// only now drains its flusher. The fence triple lexicographic comparison
	// (G,E,S) must keep the new owner's value.
	oldOwner := testFencedRecord("user:1", 7, []byte("stale-from-old-owner"), 1, 999, 999)
	if !cacheStore.flushEntries([]dirtyEntry{{Record: oldOwner}}) {
		t.Fatalf("flush old owner record: %v", cacheStore.workerError())
	}

	rec, err := cacheStore.LoadEntry(context.Background(), testRef("user:1", 7))
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if string(rec.EncodedEntry) != "from-new-owner" {
		t.Fatalf("late flush from previous owner won the upsert: got %q", rec.EncodedEntry)
	}
	if rec.Generation != 2 || rec.Epoch != 1 || rec.OwnerSeq != 1 {
		t.Fatalf("expected fence (2,1,1) to remain, got (%d,%d,%d)", rec.Generation, rec.Epoch, rec.OwnerSeq)
	}
}

func TestMySQLStoreSameGenerationLowerEpochFlushIsRejected(t *testing.T) {
	cacheStore := newTestStore(t, Config{})

	// Seed with a fence (G=2, E=3, S=5) record.
	if !cacheStore.flushEntries([]dirtyEntry{{Record: testFencedRecord("conflict", 8, []byte("new"), 2, 3, 5)}}) {
		t.Fatalf("seed newer value: %v", cacheStore.workerError())
	}
	// Attempt to overwrite with an older fence — must be rejected by the
	// (G, E, S) lexicographic conflict resolution. The variations cover all
	// three positions of the fence triple.
	older := []EntryRecord{
		testFencedRecord("conflict", 8, []byte("older-G"), 1, 999, 999),
		testFencedRecord("conflict", 8, []byte("older-E"), 2, 2, 999),
		testFencedRecord("conflict", 8, []byte("older-S"), 2, 3, 4),
	}
	for i, rec := range older {
		if !cacheStore.flushEntries([]dirtyEntry{{Record: rec}}) {
			t.Fatalf("flush older[%d]: %v", i, cacheStore.workerError())
		}
	}

	record, err := cacheStore.LoadEntry(context.Background(), testRef("conflict", 8))
	if err != nil {
		t.Fatalf("load conflict value: %v", err)
	}
	if string(record.EncodedEntry) != "new" {
		t.Fatalf("older flush overwrote newer value: %q", record.EncodedEntry)
	}
	if record.Generation != 2 || record.Epoch != 3 || record.OwnerSeq != 5 {
		t.Fatalf("expected fence (2,3,5), got (%d,%d,%d)", record.Generation, record.Epoch, record.OwnerSeq)
	}
	closeStore(t, cacheStore)
}

// TestMySQLStoreEqualFenceUsesWriterIDTiebreaker covers §8.5: when two
// records carry the same (Generation, Epoch, OwnerSeq) — possible during a
// rare epoch ping-pong or a balancer race — the upsert must fall through to
// a deterministic writer_id lex comparison instead of last-MySQL-batch-wins.
// We seed a row with writer_id="alpha" then attempt to overwrite first with
// a smaller writer_id (must lose) and then with a larger one (must win).
func TestMySQLStoreEqualFenceUsesWriterIDTiebreaker(t *testing.T) {
	cacheStore := newTestStore(t, Config{})
	defer closeStore(t, cacheStore)

	seed := testFencedRecord("tied", 17, []byte("from-alpha"), 4, 2, 7)
	seed.WriterID = "alpha"
	if !cacheStore.flushEntries([]dirtyEntry{{Record: seed}}) {
		t.Fatalf("seed alpha: %v", cacheStore.workerError())
	}

	// Smaller writer_id must lose at equal (G, E, S).
	loser := testFencedRecord("tied", 17, []byte("from-aardvark"), 4, 2, 7)
	loser.WriterID = "aardvark"
	if !cacheStore.flushEntries([]dirtyEntry{{Record: loser}}) {
		t.Fatalf("flush smaller writer_id: %v", cacheStore.workerError())
	}
	got, err := cacheStore.LoadEntry(context.Background(), testRef("tied", 17))
	if err != nil {
		t.Fatalf("load after smaller writer: %v", err)
	}
	if string(got.EncodedEntry) != "from-alpha" || got.WriterID != "alpha" {
		t.Fatalf("smaller writer_id should not have won, got value=%q writer=%q", got.EncodedEntry, got.WriterID)
	}

	// Larger writer_id at equal fence must win deterministically.
	winner := testFencedRecord("tied", 17, []byte("from-beta"), 4, 2, 7)
	winner.WriterID = "beta"
	if !cacheStore.flushEntries([]dirtyEntry{{Record: winner}}) {
		t.Fatalf("flush larger writer_id: %v", cacheStore.workerError())
	}
	got, err = cacheStore.LoadEntry(context.Background(), testRef("tied", 17))
	if err != nil {
		t.Fatalf("load after larger writer: %v", err)
	}
	if string(got.EncodedEntry) != "from-beta" || got.WriterID != "beta" {
		t.Fatalf("larger writer_id should have won, got value=%q writer=%q", got.EncodedEntry, got.WriterID)
	}

	// Sanity: a fully-larger fence still beats writer_id without needing
	// it to match. Confirms tiebreaker only kicks in on equal (G, E, S).
	override := testFencedRecord("tied", 17, []byte("from-fence"), 4, 2, 8)
	override.WriterID = "alpha"
	if !cacheStore.flushEntries([]dirtyEntry{{Record: override}}) {
		t.Fatalf("flush higher owner_seq: %v", cacheStore.workerError())
	}
	got, err = cacheStore.LoadEntry(context.Background(), testRef("tied", 17))
	if err != nil {
		t.Fatalf("load after higher owner_seq: %v", err)
	}
	if string(got.EncodedEntry) != "from-fence" {
		t.Fatalf("higher owner_seq should have won regardless of writer_id, got %q", got.EncodedEntry)
	}
}

func TestMySQLStoreDeleteFlushedKeepsNewerWALValue(t *testing.T) {
	cacheStore := newTestStore(t, Config{FlushInterval: time.Hour})
	if err := cacheStore.StoreEntry(context.Background(), testRecord("race", 9, []byte("old"))); err != nil {
		t.Fatalf("store old value: %v", err)
	}
	entries, err := cacheStore.loadDirtyBatch(1)
	if err != nil {
		t.Fatalf("load dirty batch: %v", err)
	}
	if len(entries) != 1 {
		t.Fatalf("expected one dirty entry, got %d", len(entries))
	}
	if err := cacheStore.StoreEntry(context.Background(), testRecord("race", 9, []byte("new"))); err != nil {
		t.Fatalf("store new value: %v", err)
	}
	if err := cacheStore.deleteFlushed(entries); err != nil {
		t.Fatalf("delete flushed old entry: %v", err)
	}

	record, err := cacheStore.LoadEntry(context.Background(), testRef("race", 9))
	if err != nil {
		t.Fatalf("load dirty value: %v", err)
	}
	if string(record.EncodedEntry) != "new" {
		t.Fatalf("new WAL value was deleted by old flush: %q", record.EncodedEntry)
	}
	closeStore(t, cacheStore)
}

func TestMySQLStoreDeleteWritesTombstone(t *testing.T) {
	cacheStore := newTestStore(t, Config{FlushInterval: time.Hour})
	if err := cacheStore.StoreEntry(context.Background(), testRecord("dead", 10, []byte("value"))); err != nil {
		t.Fatalf("store value: %v", err)
	}
	if err := cacheStore.DeleteEntry(context.Background(), testRef("dead", 10)); err != nil {
		t.Fatalf("delete value: %v", err)
	}
	if _, err := cacheStore.LoadEntry(context.Background(), testRef("dead", 10)); !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected tombstone miss, got %v", err)
	}
	entries, err := cacheStore.loadDirtyBatch(1)
	if err != nil {
		t.Fatalf("load wal: %v", err)
	}
	if len(entries) != 1 || !entries[0].Record.Tombstone {
		t.Fatalf("expected tombstone entry, got %#v", entries)
	}
	if entries[0].Record.Origin != "client_delete" || !entries[0].Record.FlushMySQL {
		t.Fatalf("expected flushable client_delete tombstone, got %#v", entries[0].Record)
	}
	closeStore(t, cacheStore)
}

func TestMySQLStoreExpiredTTLReturnsMissAndTombstones(t *testing.T) {
	cacheStore := newTestStore(t, Config{FlushInterval: time.Hour})
	expired := testRecord("expired", 12, []byte("value"))
	expired.TTL = time.Now().Add(-time.Second).UnixMilli()
	if err := cacheStore.StoreEntry(context.Background(), expired); err != nil {
		t.Fatalf("store expired value: %v", err)
	}
	if _, err := cacheStore.LoadEntry(context.Background(), testRef("expired", 12)); !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected expired miss, got %v", err)
	}
	entries, err := cacheStore.loadDirtyBatch(1)
	if err != nil {
		t.Fatalf("load wal: %v", err)
	}
	if len(entries) != 1 || !entries[0].Record.Tombstone {
		t.Fatalf("expected expiry tombstone, got %#v", entries)
	}
	closeStore(t, cacheStore)
}

func TestWALStoreReplaysWithoutMySQL(t *testing.T) {
	walPath := filepath.Join(t.TempDir(), "cache.wal")
	first, err := NewWALStore(Config{WALPath: walPath})
	if err != nil {
		t.Fatalf("new wal store: %v", err)
	}
	if err := first.StoreEntry(context.Background(), testRecord("local", 13, []byte("value"))); err != nil {
		t.Fatalf("store local value: %v", err)
	}
	if err := first.wal.Close(); err != nil {
		t.Fatalf("close first wal: %v", err)
	}

	second, err := NewWALStore(Config{WALPath: walPath})
	if err != nil {
		t.Fatalf("reopen wal store: %v", err)
	}
	var replayed []EntryRecord
	if err := second.Replay(context.Background(), func(record EntryRecord) error {
		replayed = append(replayed, record)
		return nil
	}); err != nil {
		t.Fatalf("replay wal: %v", err)
	}
	if len(replayed) != 1 || string(replayed[0].EncodedEntry) != "value" {
		t.Fatalf("unexpected replayed records: %#v", replayed)
	}
	closeStore(t, second)
}

func TestWALStoreDoesNotReplayPreparedRecords(t *testing.T) {
	walPath := filepath.Join(t.TempDir(), "cache.wal")
	first, err := NewWALStore(Config{WALPath: walPath})
	if err != nil {
		t.Fatalf("new wal store: %v", err)
	}
	if _, err := first.PrepareEntry(context.Background(), testFlushableRecord("prepared", 14, []byte("ghost"))); err != nil {
		t.Fatalf("prepare entry: %v", err)
	}
	if err := first.wal.Close(); err != nil {
		t.Fatalf("close first wal: %v", err)
	}

	second, err := NewWALStore(Config{WALPath: walPath})
	if err != nil {
		t.Fatalf("reopen wal store: %v", err)
	}
	var replayed []EntryRecord
	if err := second.Replay(context.Background(), func(record EntryRecord) error {
		replayed = append(replayed, record)
		return nil
	}); err != nil {
		t.Fatalf("replay wal: %v", err)
	}
	if len(replayed) != 0 {
		t.Fatalf("prepared record must not be replayed into memory: %#v", replayed)
	}
	closeStore(t, second)
}

// TestReplayDropsExpiredEntriesWithoutTombstone pins D1 first-layer fix.
// Before: Replay returned expired refs; MySQLStore.Replay called DeleteEntry
// on each, producing a Generation=0 "tombstone" WAL record that isFlushable
// rejected. Result: the local WAL accumulated unflushable records forever
// and MySQL was never informed of the expiration (which is correct on its
// own — Replay has no lease and cannot stamp a fenced tombstone — but the
// local pollution was a real bug).
// After: Replay deletes expired entries from the WAL in place and never
// produces a tombstone record. MySQL state is left untouched; durable row
// cleanup is the business layer's responsibility.
func TestReplayDropsExpiredEntriesWithoutTombstone(t *testing.T) {
	db := newTestDB(t)
	walPath := filepath.Join(t.TempDir(), "cache.wal")

	first, err := NewMySQLStore(db, Config{
		WALPath:       walPath,
		FlushInterval: time.Hour,
		BatchSize:     128,
	})
	if err != nil {
		t.Fatalf("new first store: %v", err)
	}
	if err := first.Start(context.Background()); err != nil {
		t.Fatalf("start first store: %v", err)
	}
	rec := testFlushableRecord("expiring", 51, []byte("value"))
	rec.TTL = time.Now().Add(-time.Second).UnixMilli() // already expired
	if err := first.StoreEntry(context.Background(), rec); err != nil {
		t.Fatalf("store expired value: %v", err)
	}
	// Simulate crash so the expired record survives onto disk untouched.
	first.mu.Lock()
	first.closing = true
	first.mu.Unlock()
	if err := first.wal.Close(); err != nil {
		t.Fatalf("close first wal: %v", err)
	}

	second, err := NewMySQLStore(db, Config{
		WALPath:       walPath,
		FlushInterval: time.Hour,
		BatchSize:     128,
	})
	if err != nil {
		t.Fatalf("new second store: %v", err)
	}

	var replayed []EntryRecord
	if err := second.Replay(context.Background(), func(record EntryRecord) error {
		replayed = append(replayed, record)
		return nil
	}); err != nil {
		t.Fatalf("replay: %v", err)
	}
	if len(replayed) != 0 {
		t.Fatalf("expired record must not be replayed into memory: %#v", replayed)
	}

	// The pebble dirty bucket must no longer hold the expired record.
	if _, ok, err := second.loadDirty(testRef("expiring", 51)); err != nil {
		t.Fatalf("load dirty after replay: %v", err)
	} else if ok {
		t.Fatal("expired record must be dropped from WAL during replay")
	}

	// And there must be no Generation=0 ghost tombstone left behind. Scan
	// every remaining dirty entry — none should reference this ref.
	entries, err := second.loadDirtyBatch(128)
	if err != nil {
		t.Fatalf("scan dirty: %v", err)
	}
	for _, e := range entries {
		if e.Record.HKey == 51 && e.Record.DMap == rec.DMap {
			t.Fatalf("replay left a ghost record for expired key: %#v", e.Record)
		}
	}

	// MySQL state must be unaffected by replay — no row was ever flushed for
	// this key (it expired before any flusher tick), and no tombstone gets
	// stamped by replay either.
	if _, err := second.LoadEntry(context.Background(), testRef("expiring", 51)); !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected expired key to be NotFound, got %v", err)
	}

	closeStore(t, second)
}

func TestWALStoreReplaysCommittedPreparedRecords(t *testing.T) {
	walPath := filepath.Join(t.TempDir(), "cache.wal")
	first, err := NewWALStore(Config{WALPath: walPath})
	if err != nil {
		t.Fatalf("new wal store: %v", err)
	}
	prepared, err := first.PrepareEntry(context.Background(), testFlushableRecord("committed", 15, []byte("value")))
	if err != nil {
		t.Fatalf("prepare entry: %v", err)
	}
	if err := first.CommitEntry(context.Background(), prepared.Ref(), prepared.WALSeq); err != nil {
		t.Fatalf("commit entry: %v", err)
	}
	if err := first.wal.Close(); err != nil {
		t.Fatalf("close first wal: %v", err)
	}

	second, err := NewWALStore(Config{WALPath: walPath})
	if err != nil {
		t.Fatalf("reopen wal store: %v", err)
	}
	var replayed []EntryRecord
	if err := second.Replay(context.Background(), func(record EntryRecord) error {
		replayed = append(replayed, record)
		return nil
	}); err != nil {
		t.Fatalf("replay wal: %v", err)
	}
	if len(replayed) != 1 || string(replayed[0].EncodedEntry) != "value" || replayed[0].WALState != WALStateCommitted {
		t.Fatalf("unexpected replayed committed records: %#v", replayed)
	}
	closeStore(t, second)
}

func TestMySQLStoreAbortRemovesPreparedRecord(t *testing.T) {
	cacheStore := newTestStore(t, Config{FlushInterval: time.Hour})
	defer closeStore(t, cacheStore)

	prepared, err := cacheStore.PrepareEntry(context.Background(), testFlushableRecord("aborted", 90, []byte("ghost")))
	if err != nil {
		t.Fatalf("prepare entry: %v", err)
	}
	if err := cacheStore.AbortEntry(context.Background(), prepared.Ref(), prepared.WALSeq); err != nil {
		t.Fatalf("abort prepared: %v", err)
	}
	if _, err := cacheStore.LoadEntry(context.Background(), prepared.Ref()); !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected aborted record to be gone, got %v", err)
	}
	if cacheStore.flushWAL(true) {
		// flushWAL returns true on empty/no-op too, so explicitly check MySQL.
	}
	reader := newTestStoreWithDB(t, cacheStore.db, Config{WALPath: filepath.Join(t.TempDir(), "reader.wal")})
	defer closeStore(t, reader)
	if _, err := reader.LoadEntry(context.Background(), prepared.Ref()); !errors.Is(err, ErrNotFound) {
		t.Fatalf("aborted record leaked into MySQL: %v", err)
	}
}

func TestMySQLStoreAbortIdempotentAndCommitProtected(t *testing.T) {
	cacheStore := newTestStore(t, Config{FlushInterval: time.Hour})
	defer closeStore(t, cacheStore)

	// Abort with no prepared record is a no-op.
	if err := cacheStore.AbortEntry(context.Background(), testRef("missing", 1), 12345); err != nil {
		t.Fatalf("abort missing should be idempotent, got %v", err)
	}

	// Once committed, abort must refuse.
	prepared, err := cacheStore.PrepareEntry(context.Background(), testFlushableRecord("locked", 91, []byte("v")))
	if err != nil {
		t.Fatalf("prepare entry: %v", err)
	}
	if err := cacheStore.CommitEntry(context.Background(), prepared.Ref(), prepared.WALSeq); err != nil {
		t.Fatalf("commit entry: %v", err)
	}
	if err := cacheStore.AbortEntry(context.Background(), prepared.Ref(), prepared.WALSeq); err == nil {
		t.Fatal("expected abort of committed record to fail")
	}

	// Stale version is ignored (newer prepare wins).
	prepared2, err := cacheStore.PrepareEntry(context.Background(), testFlushableRecord("super", 92, []byte("v1")))
	if err != nil {
		t.Fatalf("prepare v1: %v", err)
	}
	prepared3, err := cacheStore.PrepareEntry(context.Background(), testFlushableRecord("super", 92, []byte("v2")))
	if err != nil {
		t.Fatalf("prepare v2: %v", err)
	}
	if prepared3.WALSeq <= prepared2.WALSeq {
		t.Fatalf("expected prepare v2 to advance version, got v1=%d v2=%d", prepared2.WALSeq, prepared3.WALSeq)
	}
	if err := cacheStore.AbortEntry(context.Background(), prepared2.Ref(), prepared2.WALSeq); err != nil {
		t.Fatalf("abort with stale version should be no-op, got %v", err)
	}
	if err := cacheStore.CommitEntry(context.Background(), prepared3.Ref(), prepared3.WALSeq); err != nil {
		t.Fatalf("commit current prepare must still succeed, got %v", err)
	}
}

func TestMySQLStorePurgeBelowGenerationDropsStaleRecords(t *testing.T) {
	// Use NewMySQLStore directly without Start() so the flush loop does not
	// drain WAL into MySQL before we observe the purge.
	walPath := filepath.Join(t.TempDir(), "cache.wal")
	cacheStore, err := NewMySQLStore(newTestDB(t), Config{
		WALPath:       walPath,
		FlushInterval: time.Hour,
		BatchSize:     1024,
	})
	if err != nil {
		t.Fatalf("new store: %v", err)
	}
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if err := cacheStore.Close(ctx); err != nil {
			t.Fatalf("close: %v", err)
		}
	}()

	if err := cacheStore.StoreEntry(context.Background(), testFencedRecord("a", 1, []byte("g1"), 1, 5, 1)); err != nil {
		t.Fatalf("store g1: %v", err)
	}
	if err := cacheStore.StoreEntry(context.Background(), testFencedRecord("b", 2, []byte("g2"), 2, 5, 1)); err != nil {
		t.Fatalf("store g2: %v", err)
	}
	if err := cacheStore.StoreEntry(context.Background(), testFencedRecord("c", 3, []byte("g3"), 3, 5, 1)); err != nil {
		t.Fatalf("store g3: %v", err)
	}

	purged, err := cacheStore.PurgeBelowGeneration(context.Background(), 2)
	if err != nil {
		t.Fatalf("purge: %v", err)
	}
	if purged != 1 {
		t.Fatalf("expected 1 purged record, got %d", purged)
	}
	if _, err := cacheStore.LoadEntry(context.Background(), testRef("a", 1)); !errors.Is(err, ErrNotFound) {
		t.Fatalf("stale g1 record should be gone, got %v", err)
	}
	if _, err := cacheStore.LoadEntry(context.Background(), testRef("b", 2)); err != nil {
		t.Fatalf("g2 record must remain: %v", err)
	}
	if _, err := cacheStore.LoadEntry(context.Background(), testRef("c", 3)); err != nil {
		t.Fatalf("g3 record must remain: %v", err)
	}
}

func TestMySQLStorePurgeBelowGenerationSparesPreparedRecords(t *testing.T) {
	walPath := filepath.Join(t.TempDir(), "cache.wal")
	cacheStore, err := NewMySQLStore(newTestDB(t), Config{
		WALPath:       walPath,
		FlushInterval: time.Hour,
		BatchSize:     1024,
	})
	if err != nil {
		t.Fatalf("new store: %v", err)
	}
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = cacheStore.Close(ctx)
	}()

	prepared, err := cacheStore.PrepareEntry(context.Background(), testFencedRecord("pending", 99, []byte("v"), 1, 1, 1))
	if err != nil {
		t.Fatalf("prepare: %v", err)
	}

	purged, err := cacheStore.PurgeBelowGeneration(context.Background(), 100)
	if err != nil {
		t.Fatalf("purge: %v", err)
	}
	if purged != 0 {
		t.Fatalf("prepared records must not be purged, purged=%d", purged)
	}
	if err := cacheStore.CommitEntry(context.Background(), prepared.Ref(), prepared.WALSeq); err != nil {
		t.Fatalf("commit after purge: %v", err)
	}
}

func TestMySQLStorePurgeBelowGenerationIsNoOpWhenMinIsZero(t *testing.T) {
	cacheStore := newTestStore(t, Config{FlushInterval: time.Hour})
	defer closeStore(t, cacheStore)

	if err := cacheStore.StoreEntry(context.Background(), testFencedRecord("a", 1, []byte("v"), 1, 1, 1)); err != nil {
		t.Fatalf("store: %v", err)
	}
	purged, err := cacheStore.PurgeBelowGeneration(context.Background(), 0)
	if err != nil {
		t.Fatalf("purge zero: %v", err)
	}
	if purged != 0 {
		t.Fatalf("purge with minGeneration=0 must be a no-op, got %d", purged)
	}
}

// TestMySQLStoreFlushHandoffDrainsCommittedRecords covers §8.6 / Phase 4
// fragment handoff drain. A committed record must reach MySQL and leave the
// WAL synchronously so the new owner can serve the key after migration.
func TestMySQLStoreFlushHandoffDrainsCommittedRecords(t *testing.T) {
	db := newTestDB(t)
	walPath := filepath.Join(t.TempDir(), "cache.wal")
	cacheStore := newTestStoreWithDB(t, db, Config{
		WALPath:       walPath,
		FlushInterval: time.Hour,
		BatchSize:     128,
	})
	defer closeStore(t, cacheStore)

	ctx := context.Background()
	rec := testFlushableRecord("hand:1", 71, []byte("v1"))
	rec.DMap = "shared"
	if err := cacheStore.StoreEntry(ctx, rec); err != nil {
		t.Fatalf("store: %v", err)
	}
	rec2 := testFlushableRecord("hand:2", 72, []byte("v2"))
	rec2.DMap = "shared"
	if err := cacheStore.StoreEntry(ctx, rec2); err != nil {
		t.Fatalf("store rec2: %v", err)
	}

	if err := cacheStore.FlushHandoff(ctx, []EntryRef{
		{DMap: "shared", HKey: 71},
		{DMap: "shared", HKey: 72},
	}); err != nil {
		t.Fatalf("flush handoff: %v", err)
	}

	// The dirty WAL must be empty for both refs.
	if got, _, _ := cacheStore.loadDirty(EntryRef{DMap: "shared", HKey: 71}); got.HKey != 0 {
		t.Fatalf("wal entry 71 still present after handoff: %+v", got)
	}
	if got, _, _ := cacheStore.loadDirty(EntryRef{DMap: "shared", HKey: 72}); got.HKey != 0 {
		t.Fatalf("wal entry 72 still present after handoff: %+v", got)
	}
	// MySQL must hold the latest value.
	loaded, err := cacheStore.LoadEntry(ctx, EntryRef{DMap: "shared", HKey: 71})
	if err != nil {
		t.Fatalf("load 71 after handoff: %v", err)
	}
	if string(loaded.EncodedEntry) != "v1" {
		t.Fatalf("expected MySQL row for 71 = v1, got %q", loaded.EncodedEntry)
	}
}

func TestMySQLStoreFlushHandoffPartitionDrainsNonResidentRecords(t *testing.T) {
	db := newTestDB(t)
	walPath := filepath.Join(t.TempDir(), "cache.wal")
	cacheStore := newTestStoreWithDB(t, db, Config{
		WALPath:       walPath,
		FlushInterval: time.Hour,
		BatchSize:     128,
	})
	defer closeStore(t, cacheStore)

	ctx := context.Background()
	live := testFlushableRecord("resident", 7, []byte("live"))
	live.DMap = "shared"
	if err := cacheStore.StoreEntry(ctx, live); err != nil {
		t.Fatalf("store live: %v", err)
	}
	tombstone := testFencedRecord("deleted", 17, nil, 1, 1, 1000)
	tombstone.DMap = "shared"
	tombstone.Tombstone = true
	preparedTombstone, err := cacheStore.PrepareEntry(ctx, tombstone)
	if err != nil {
		t.Fatalf("prepare tombstone: %v", err)
	}
	if err := cacheStore.CommitEntry(ctx, preparedTombstone.Ref(), preparedTombstone.WALSeq); err != nil {
		t.Fatalf("commit tombstone: %v", err)
	}
	otherPartition := testFlushableRecord("other", 8, []byte("other"))
	otherPartition.DMap = "shared"
	if err := cacheStore.StoreEntry(ctx, otherPartition); err != nil {
		t.Fatalf("store other partition: %v", err)
	}
	otherDMap := testFlushableRecord("other-dmap", 27, []byte("other-dmap"))
	otherDMap.DMap = "private"
	if err := cacheStore.StoreEntry(ctx, otherDMap); err != nil {
		t.Fatalf("store other dmap: %v", err)
	}

	if err := cacheStore.FlushHandoffPartition(ctx, "shared", 7, 10); err != nil {
		t.Fatalf("flush handoff partition: %v", err)
	}

	for _, ref := range []EntryRef{
		{DMap: "shared", HKey: 7},
		{DMap: "shared", HKey: 17},
	} {
		if got, ok, err := cacheStore.loadDirty(ref); err != nil || ok {
			t.Fatalf("expected drained wal for %+v, ok=%v rec=%+v err=%v", ref, ok, got, err)
		}
	}
	if _, ok, err := cacheStore.loadDirty(EntryRef{DMap: "shared", HKey: 8}); err != nil || !ok {
		t.Fatalf("other partition record should remain, ok=%v err=%v", ok, err)
	}
	if _, ok, err := cacheStore.loadDirty(EntryRef{DMap: "private", HKey: 27}); err != nil || !ok {
		t.Fatalf("other dmap record should remain, ok=%v err=%v", ok, err)
	}
	eventually(t, func() bool {
		_, err := cacheStore.LoadEntry(ctx, EntryRef{DMap: "shared", HKey: 17})
		return errors.Is(err, ErrNotFound)
	})
}

// TestMySQLStoreFlushHandoffSkipsPreparedAndAbsentRecords ensures the drain
// path never tries to flush in-flight (Prepared) writes — committing one
// from outside a hook would corrupt the WAL — and is idempotent when called
// against keys that were already flushed.
func TestMySQLStoreFlushHandoffSkipsPreparedAndAbsentRecords(t *testing.T) {
	db := newTestDB(t)
	walPath := filepath.Join(t.TempDir(), "cache.wal")
	cacheStore := newTestStoreWithDB(t, db, Config{
		WALPath:       walPath,
		FlushInterval: time.Hour,
		BatchSize:     128,
	})
	defer closeStore(t, cacheStore)

	ctx := context.Background()
	prep := testFlushableRecord("prep", 81, []byte("inflight"))
	prep.DMap = "shared"
	prepared, err := cacheStore.PrepareEntry(ctx, prep)
	if err != nil {
		t.Fatalf("prepare: %v", err)
	}

	if err := cacheStore.FlushHandoff(ctx, []EntryRef{
		{DMap: "shared", HKey: 81}, // Prepared — skip
		{DMap: "shared", HKey: 82}, // absent — skip
	}); err != nil {
		t.Fatalf("flush handoff with prepared+absent: %v", err)
	}

	// Prepared record must still be in the WAL, untouched.
	got, ok, err := cacheStore.loadDirty(EntryRef{DMap: "shared", HKey: 81})
	if err != nil {
		t.Fatalf("load prepared after handoff: %v", err)
	}
	if !ok || got.WALState != WALStatePrepared || got.WALSeq != prepared.WALSeq {
		t.Fatalf("prepared record disturbed by handoff: ok=%v rec=%+v", ok, got)
	}

	// Empty input must also be a no-op.
	if err := cacheStore.FlushHandoff(ctx, nil); err != nil {
		t.Fatalf("flush handoff with nil refs: %v", err)
	}
}

// TestMySQLStoreFlushHandoffSurfacesFlushFailure pins the handoff abort
// behaviour: when MySQL is unreachable the drain returns an error and the
// WAL keeps the record so the next regular flush (or a balancer retry) can
// re-attempt.
func TestMySQLStoreFlushHandoffSurfacesFlushFailure(t *testing.T) {
	db := newTestDB(t)
	walPath := filepath.Join(t.TempDir(), "cache.wal")
	cacheStore := newTestStoreWithDB(t, db, Config{
		WALPath:       walPath,
		FlushInterval: time.Hour,
		BatchSize:     128,
	})

	ctx := context.Background()
	rec := testFlushableRecord("hand:fail", 91, []byte("v"))
	rec.DMap = "shared"
	if err := cacheStore.StoreEntry(ctx, rec); err != nil {
		t.Fatalf("store: %v", err)
	}

	// Slam the SQLite db shut to simulate MySQL outage.
	sqlDB, err := db.DB()
	if err != nil {
		t.Fatalf("db handle: %v", err)
	}
	if err := sqlDB.Close(); err != nil {
		t.Fatalf("close sql db: %v", err)
	}

	if err := cacheStore.FlushHandoff(ctx, []EntryRef{{DMap: "shared", HKey: 91}}); err == nil {
		t.Fatalf("expected flush handoff to surface mysql failure, got nil")
	}

	// WAL must still hold the record so the next flush can retry.
	got, ok, _ := cacheStore.loadDirty(EntryRef{DMap: "shared", HKey: 91})
	if !ok || got.HKey != 91 {
		t.Fatalf("expected wal record preserved after failed handoff, got ok=%v rec=%+v", ok, got)
	}

	// Skip closeStore: it would block on flushLoop draining a closed DB.
	cacheStore.mu.Lock()
	cacheStore.closing = true
	cacheStore.mu.Unlock()
	_ = cacheStore.wal.Close()
}

func newTestStore(t *testing.T, cfg Config) *MySQLStore {
	t.Helper()

	db := newTestDB(t)
	return newTestStoreWithDB(t, db, cfg)
}

func newTestStoreWithDB(t *testing.T, db *gorm.DB, cfg Config) *MySQLStore {
	t.Helper()

	if cfg.WALPath == "" {
		cfg.WALPath = filepath.Join(t.TempDir(), "cache.wal")
	}
	cacheStore, err := NewMySQLStore(db, cfg)
	if err != nil {
		t.Fatalf("new store: %v", err)
	}
	if err := cacheStore.Start(context.Background()); err != nil {
		t.Fatalf("start store: %v", err)
	}
	return cacheStore
}

func newTestDB(t *testing.T) *gorm.DB {
	t.Helper()

	db, err := gorm.Open(sqlite.Open("file:"+filepath.Join(t.TempDir(), "cache.db")+"?cache=shared"), &gorm.Config{})
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	return db
}

func closeStore(t *testing.T, cacheStore *MySQLStore) {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := cacheStore.Close(ctx); err != nil {
		t.Fatalf("close store: %v", err)
	}
}

func eventually(t *testing.T, condition func() bool) {
	t.Helper()

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("condition was not met before timeout")
}

func testRef(key string, hkey uint64) EntryRef {
	return EntryRef{Key: key, HKey: hkey}
}

func testRecord(key string, hkey uint64, value []byte) EntryRecord {
	return EntryRecord{
		Key:          key,
		HKey:         hkey,
		EncodedEntry: append([]byte(nil), value...),
		Timestamp:    time.Now().UnixNano(),
	}
}

// testFenceCounter advances per testFlushableRecord call so default flushable
// records carry monotonically increasing OwnerSeq under fence (1, 1).
var testFenceCounter atomic.Int64

func testFlushableRecord(key string, hkey uint64, value []byte) EntryRecord {
	record := testRecord(key, hkey, value)
	record.Origin = "client_set"
	record.FlushMySQL = true
	record.Generation = 1
	record.Epoch = 1
	record.OwnerSeq = testFenceCounter.Add(1)
	return record
}

// testFencedRecord lets a test pin the exact (G, E, S) used in the upsert
// comparison. Required for tests that exercise the conflict-resolution rules.
func testFencedRecord(key string, hkey uint64, value []byte, generation, epoch, ownerSeq int64) EntryRecord {
	record := testFlushableRecord(key, hkey, value)
	record.Generation = generation
	record.Epoch = epoch
	record.OwnerSeq = ownerSeq
	record.UpdatedAt = time.Now().UTC()
	return record
}

// TestMySQLStoreBatchedPreparesAreConcurrentSafe stresses the WAL sequence
// allocation: many goroutines call PrepareEntry concurrently against distinct
// keys. The test verifies (1) every prepare returns a unique WALSeq and (2)
// every committed record is durably visible. If Batch idempotency were
// broken (e.g. local sequence allocation raced across concurrent prepares),
// either two records would share a WALSeq or one would be silently lost.
func TestMySQLStoreBatchedPreparesAreConcurrentSafe(t *testing.T) {
	cacheStore := newTestStore(t, Config{
		QueueSize:     1024,
		FlushInterval: time.Hour, // disable timed flush; we want WAL inspection
		BatchSize:     1024,
	})
	defer closeStore(t, cacheStore)

	const writers = 32
	const perWriter = 16
	var wg sync.WaitGroup
	type result struct {
		ref    EntryRef
		walSeq int64
	}
	results := make(chan result, writers*perWriter)
	errs := make(chan error, writers*perWriter)

	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func(writer int) {
			defer wg.Done()
			for i := 0; i < perWriter; i++ {
				key := fmt.Sprintf("w%d-k%d", writer, i)
				hkey := uint64(writer*1000 + i + 1)
				rec := testFlushableRecord(key, hkey, []byte(key))
				rec.WALState = WALStatePrepared
				rec.FlushMySQL = false
				prepared, err := cacheStore.PrepareEntry(context.Background(), rec)
				if err != nil {
					errs <- err
					return
				}
				if err := cacheStore.CommitEntry(context.Background(), prepared.Ref(), prepared.WALSeq); err != nil {
					errs <- err
					return
				}
				results <- result{ref: prepared.Ref(), walSeq: prepared.WALSeq}
			}
		}(w)
	}
	wg.Wait()
	close(results)
	close(errs)

	for err := range errs {
		t.Fatalf("concurrent prepare/commit: %v", err)
	}

	seen := make(map[int64]EntryRef, writers*perWriter)
	for r := range results {
		if prev, ok := seen[r.walSeq]; ok {
			t.Fatalf("duplicate WALSeq %d for refs %+v and %+v", r.walSeq, prev, r.ref)
		}
		seen[r.walSeq] = r.ref
	}
	if got, want := len(seen), writers*perWriter; got != want {
		t.Fatalf("expected %d distinct walseqs, got %d", want, got)
	}
}
