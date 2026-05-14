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

func TestMySQLStoreFlushPendingReportsSuccess(t *testing.T) {
	cacheStore := newTestStore(t, Config{})
	pending := map[string][]byte{"a": []byte("1")}

	if !cacheStore.flushPending(pending) {
		t.Fatalf("expected flush success: %v", cacheStore.workerError())
	}
	record, err := cacheStore.LoadEntry(context.Background(), EntryRef{Key: "a", HKey: 1})
	if err != nil {
		t.Fatalf("load flushed value: %v", err)
	}
	if string(record.EncodedEntry) != "1" {
		t.Fatalf("expected value 1, got %q", record.EncodedEntry)
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
		NodeID:        "node-a",
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
		NodeID:        "node-a",
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
	if err := cacheStore.flushEntries([]dirtyEntry{{Record: testVersionedRecord("split", 7, []byte("mysql"), time.Now().Add(-time.Second).UnixNano(), "node-a")}}); !err {
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

func TestMySQLStoreVersionedFlushDoesNotOverwriteNewerValue(t *testing.T) {
	cacheStore := newTestStore(t, Config{NodeID: "node-a"})
	now := time.Now()

	if !cacheStore.flushEntries([]dirtyEntry{{Record: testVersionedRecord("conflict", 8, []byte("new"), now.UnixNano(), "node-b")}}) {
		t.Fatalf("seed newer value: %v", cacheStore.workerError())
	}
	if !cacheStore.flushEntries([]dirtyEntry{{Record: testVersionedRecord("conflict", 8, []byte("old"), now.Add(-time.Second).UnixNano(), "node-a")}}) {
		t.Fatalf("flush older value: %v", cacheStore.workerError())
	}

	record, err := cacheStore.LoadEntry(context.Background(), testRef("conflict", 8))
	if err != nil {
		t.Fatalf("load conflict value: %v", err)
	}
	if string(record.EncodedEntry) != "new" {
		t.Fatalf("older flush overwrote newer value: %q", record.EncodedEntry)
	}
	closeStore(t, cacheStore)
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

func testFlushableRecord(key string, hkey uint64, value []byte) EntryRecord {
	record := testRecord(key, hkey, value)
	record.Origin = "client_set"
	record.FlushMySQL = true
	return record
}

func testVersionedRecord(key string, hkey uint64, value []byte, version int64, writer string) EntryRecord {
	record := testFlushableRecord(key, hkey, value)
	record.Version = version
	record.WriterID = writer
	record.UpdatedAt = time.Now().UTC()
	return record
}
