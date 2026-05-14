package store

import (
	"context"
	"errors"
	"fmt"
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
	cacheStore := newTestStore(t, Config{
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

	value, err := cacheStore.Load(ctx, "user:1")
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

func TestMySQLStoreRejectsStoreAfterClose(t *testing.T) {
	cacheStore := newTestStore(t, Config{})
	closeStore(t, cacheStore)

	err := cacheStore.Store(context.Background(), "a", []byte("1"))
	if err == nil {
		t.Fatal("expected store after close to fail")
	}
}

func TestMySQLStoreCloseFlushesAcceptedWrites(t *testing.T) {
	cacheStore := newTestStore(t, Config{
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

	value, err := cacheStore.Load(ctx, "key-0")
	if err != nil {
		t.Fatalf("load flushed value: %v", err)
	}
	if string(value) != "value" {
		t.Fatalf("expected flushed value, got %q", value)
	}
}

func newTestStore(t *testing.T, cfg Config) *MySQLStore {
	t.Helper()

	db, err := gorm.Open(sqlite.Open("file::memory:?cache=shared"), &gorm.Config{})
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}

	cacheStore, err := NewMySQLStore(db, cfg)
	if err != nil {
		t.Fatalf("new store: %v", err)
	}
	return cacheStore
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
