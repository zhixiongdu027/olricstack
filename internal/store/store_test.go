package store

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

func TestMySQLStoreLoadMiss(t *testing.T) {
	cacheStore := newTestStore(t, Config{})

	_, err := cacheStore.Load(context.Background(), "missing")
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected ErrNotFound, got %v", err)
	}
}

func TestMySQLStoreCoalescesAndFlushesLatestValue(t *testing.T) {
	db := newTestDB(t)
	walPath := filepath.Join(t.TempDir(), "cache.wal")
	cacheStore := newTestStoreWithDB(t, db, Config{
		WALPath:       walPath,
		FlushInterval: time.Hour,
		BatchSize:     16,
	})

	ctx := context.Background()
	if err := cacheStore.Store(ctx, "user:1", []byte("stale")); err != nil {
		t.Fatalf("store stale: %v", err)
	}
	if err := cacheStore.Store(ctx, "user:1", []byte("fresh")); err != nil {
		t.Fatalf("store fresh: %v", err)
	}

	closeStore(t, cacheStore)

	reopened := newTestStoreWithDB(t, db, Config{WALPath: filepath.Join(t.TempDir(), "reopen.wal")})
	defer closeStore(t, reopened)
	value, err := reopened.Load(ctx, "user:1")
	if err != nil {
		t.Fatalf("load flushed value: %v", err)
	}
	if string(value) != "fresh" {
		t.Fatalf("expected latest value, got %q", value)
	}
}

func TestMySQLStoreFlushesWhenBatchSizeReached(t *testing.T) {
	cacheStore := newTestStore(t, Config{
		FlushInterval: time.Hour,
		BatchSize:     2,
	})

	ctx := context.Background()
	if err := cacheStore.Store(ctx, "a", []byte("1")); err != nil {
		t.Fatalf("store a: %v", err)
	}
	if err := cacheStore.Store(ctx, "b", []byte("2")); err != nil {
		t.Fatalf("store b: %v", err)
	}

	eventually(t, func() bool {
		value, err := cacheStore.Load(ctx, "b")
		return err == nil && string(value) == "2"
	})
	closeStore(t, cacheStore)
}

func TestMySQLStoreRejectsWhenDirtyQueueFull(t *testing.T) {
	cacheStore := newTestStore(t, Config{
		QueueSize:     1,
		FlushInterval: time.Hour,
		BatchSize:     128,
	})

	err := cacheStore.Store(context.Background(), "a", []byte("1"))
	if err != nil {
		t.Fatalf("first store should fit queue: %v", err)
	}

	err = cacheStore.Store(context.Background(), "b", []byte("2"))
	if !errors.Is(err, ErrQueueFull) {
		t.Fatalf("expected ErrQueueFull, got %v", err)
	}
	closeStore(t, cacheStore)
}

func TestMySQLStoreFlushPendingRetainsBatchOnFailure(t *testing.T) {
	cacheStore := newTestStore(t, Config{})
	pending := map[string][]byte{"a": []byte("1")}

	sqlDB, err := cacheStore.db.DB()
	if err != nil {
		t.Fatalf("get sql db: %v", err)
	}
	if err := sqlDB.Close(); err != nil {
		t.Fatalf("close sql db: %v", err)
	}

	if cacheStore.flushPending(pending) {
		t.Fatal("expected flush failure")
	}
	if len(pending) != 1 {
		t.Fatalf("pending batch should remain available for retry, got len=%d", len(pending))
	}
}

func TestMySQLStoreFlushPendingReportsSuccess(t *testing.T) {
	cacheStore := newTestStore(t, Config{})
	pending := map[string][]byte{"a": []byte("1")}

	if !cacheStore.flushPending(pending) {
		t.Fatalf("expected flush success: %v", cacheStore.workerError())
	}
	value, err := cacheStore.Load(context.Background(), "a")
	if err != nil {
		t.Fatalf("load flushed value: %v", err)
	}
	if string(value) != "1" {
		t.Fatalf("expected value 1, got %q", value)
	}
	closeStore(t, cacheStore)
}

func TestMySQLStoreCloseReportsFinalFlushFailure(t *testing.T) {
	cacheStore := newTestStore(t, Config{
		FlushInterval: time.Hour,
		BatchSize:     128,
	})
	if err := cacheStore.Store(context.Background(), "a", []byte("1")); err != nil {
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
	pending := map[string][]byte{"a": []byte("1")}

	sqlDB, err := cacheStore.db.DB()
	if err != nil {
		t.Fatalf("get sql db: %v", err)
	}
	if err := sqlDB.Close(); err != nil {
		t.Fatalf("close sql db: %v", err)
	}

	if cacheStore.flushPending(pending) {
		t.Fatal("expected flush failure")
	}
	cacheStore.deferFlush()
	if cacheStore.flushWAL(false) {
		t.Fatal("flush should be deferred after failure")
	}
}

func TestMySQLStoreRejectsStoreAfterClose(t *testing.T) {
	cacheStore := newTestStore(t, Config{})
	closeStore(t, cacheStore)

	err := cacheStore.Store(context.Background(), "a", []byte("1"))
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
		if err := cacheStore.Store(ctx, key, []byte("value")); err != nil {
			t.Fatalf("store %s: %v", key, err)
		}
	}
	closeStore(t, cacheStore)

	reopened := newTestStoreWithDB(t, db, Config{WALPath: filepath.Join(t.TempDir(), "reopen.wal")})
	defer closeStore(t, reopened)
	value, err := reopened.Load(ctx, "key-0")
	if err != nil {
		t.Fatalf("load flushed value: %v", err)
	}
	if string(value) != "value" {
		t.Fatalf("expected flushed value, got %q", value)
	}
}

func TestMySQLStoreWALSurvivesProcessRestart(t *testing.T) {
	db := newTestDB(t)
	walPath := filepath.Join(t.TempDir(), "cache.wal")

	first, err := NewMySQLStore(db, Config{
		WALPath:       walPath,
		NodeID:        "node-a",
		FlushInterval: time.Hour,
		BatchSize:     128,
	})
	if err != nil {
		t.Fatalf("new first store: %v", err)
	}
	if err := first.Store(context.Background(), "durable", []byte("value")); err != nil {
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
		NodeID:        "node-a",
		FlushInterval: time.Hour,
		BatchSize:     128,
	})
	if err != nil {
		t.Fatalf("new second store: %v", err)
	}
	closeStore(t, second)

	reader := newTestStoreWithDB(t, db, Config{WALPath: filepath.Join(t.TempDir(), "reader.wal")})
	defer closeStore(t, reader)
	value, err := reader.Load(context.Background(), "durable")
	if err != nil {
		t.Fatalf("load recovered value: %v", err)
	}
	if string(value) != "value" {
		t.Fatalf("expected recovered value, got %q", value)
	}
}

func TestMySQLStoreLoadPrefersLocalWALBeforeMySQL(t *testing.T) {
	cacheStore := newTestStore(t, Config{FlushInterval: time.Hour})
	if err := cacheStore.flushEntries([]dirtyEntry{{
		Key:       "split",
		Value:     []byte("mysql"),
		Version:   time.Now().Add(-time.Second).UnixNano(),
		WriterID:  "node-a",
		UpdatedAt: time.Now().Add(-time.Second),
	}}); !err {
		t.Fatalf("seed mysql: %v", cacheStore.workerError())
	}
	if err := cacheStore.Store(context.Background(), "split", []byte("wal")); err != nil {
		t.Fatalf("store wal value: %v", err)
	}

	value, err := cacheStore.Load(context.Background(), "split")
	if err != nil {
		t.Fatalf("load value: %v", err)
	}
	if string(value) != "wal" {
		t.Fatalf("expected WAL value, got %q", value)
	}
	closeStore(t, cacheStore)
}

func TestMySQLStoreVersionedFlushDoesNotOverwriteNewerValue(t *testing.T) {
	cacheStore := newTestStore(t, Config{NodeID: "node-a"})
	now := time.Now()

	if !cacheStore.flushEntries([]dirtyEntry{{
		Key:       "conflict",
		Value:     []byte("new"),
		Version:   now.UnixNano(),
		WriterID:  "node-b",
		UpdatedAt: now,
	}}) {
		t.Fatalf("seed newer value: %v", cacheStore.workerError())
	}
	if !cacheStore.flushEntries([]dirtyEntry{{
		Key:       "conflict",
		Value:     []byte("old"),
		Version:   now.Add(-time.Second).UnixNano(),
		WriterID:  "node-a",
		UpdatedAt: now.Add(-time.Second),
	}}) {
		t.Fatalf("flush older value: %v", cacheStore.workerError())
	}

	value, err := cacheStore.Load(context.Background(), "conflict")
	if err != nil {
		t.Fatalf("load conflict value: %v", err)
	}
	if string(value) != "new" {
		t.Fatalf("older flush overwrote newer value: %q", value)
	}
	closeStore(t, cacheStore)
}

func TestMySQLStoreDeleteFlushedKeepsNewerWALValue(t *testing.T) {
	cacheStore := newTestStore(t, Config{FlushInterval: time.Hour})
	if err := cacheStore.Store(context.Background(), "race", []byte("old")); err != nil {
		t.Fatalf("store old value: %v", err)
	}
	entries, err := cacheStore.loadDirtyBatch(1)
	if err != nil {
		t.Fatalf("load dirty batch: %v", err)
	}
	if len(entries) != 1 {
		t.Fatalf("expected one dirty entry, got %d", len(entries))
	}
	if err := cacheStore.Store(context.Background(), "race", []byte("new")); err != nil {
		t.Fatalf("store new value: %v", err)
	}
	if err := cacheStore.deleteFlushed(entries); err != nil {
		t.Fatalf("delete flushed old entry: %v", err)
	}

	value, err := cacheStore.Load(context.Background(), "race")
	if err != nil {
		t.Fatalf("load dirty value: %v", err)
	}
	if string(value) != "new" {
		t.Fatalf("new WAL value was deleted by old flush: %q", value)
	}
	closeStore(t, cacheStore)
}

func TestMySQLStoreStoreVersionedUsesCallerVersion(t *testing.T) {
	cacheStore := newTestStore(t, Config{FlushInterval: time.Hour})
	if err := cacheStore.StoreVersioned(context.Background(), "versioned", []byte("v10"), 10); err != nil {
		t.Fatalf("store versioned: %v", err)
	}
	entries, err := cacheStore.loadDirtyBatch(1)
	if err != nil {
		t.Fatalf("load dirty batch: %v", err)
	}
	if len(entries) != 1 {
		t.Fatalf("expected one entry, got %d", len(entries))
	}
	if entries[0].Version != 10 {
		t.Fatalf("expected caller version 10, got %d", entries[0].Version)
	}
	closeStore(t, cacheStore)
}

func TestMySQLStoreStoreVersionedRejectsInvalidVersion(t *testing.T) {
	cacheStore := newTestStore(t, Config{})
	err := cacheStore.StoreVersioned(context.Background(), "versioned", []byte("bad"), 0)
	if err == nil {
		t.Fatal("expected invalid version error")
	}
	closeStore(t, cacheStore)
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
	if cfg.NodeID == "" {
		cfg.NodeID = "node-a"
	}
	cacheStore, err := NewMySQLStore(db, cfg)
	if err != nil {
		t.Fatalf("new store: %v", err)
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
