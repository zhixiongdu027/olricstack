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

func TestDurableHookBeforeSetWritesFlushableRecord(t *testing.T) {
	backing := newRecordingStore()
	hook := newTestDurableHook(t, backing)
	entry := newTestEntry("alice", "A", time.Now().Add(time.Minute).UnixMilli(), 42)

	_, err := hook.BeforeSet(context.Background(), olricconfig.DurableOperation{
		DMap:  "users",
		Key:   "alice",
		HKey:  HKey("users", "alice"),
		Entry: entry,
	})
	if err != nil {
		t.Fatalf("before set: %v", err)
	}
	record := backing.records[HKey("users", "alice")]
	if record.DMap != "users" || record.Key != "alice" || string(record.EncodedEntry) == "" {
		t.Fatalf("unexpected record: %#v", record)
	}
	if record.TTL != entry.TTL() || record.Timestamp != entry.Timestamp() || record.Origin != "client_set" || !record.FlushMySQL {
		t.Fatalf("unexpected durable metadata: %#v", record)
	}
}

func TestDurableHookBeforeDeleteWritesTombstone(t *testing.T) {
	backing := newRecordingStore()
	hook := newTestDurableHook(t, backing)

	_, err := hook.BeforeDelete(context.Background(), olricconfig.DurableOperation{
		DMap: "users",
		Key:  "alice",
		HKey: HKey("users", "alice"),
	})
	if err != nil {
		t.Fatalf("before delete: %v", err)
	}
	record := backing.records[HKey("users", "alice")]
	if !record.Tombstone || record.DMap != "users" || record.Key != "alice" {
		t.Fatalf("expected tombstone record, got %#v", record)
	}
}

func TestDurableHookBeforeExpireWritesUpdatedEntry(t *testing.T) {
	backing := newRecordingStore()
	hook := newTestDurableHook(t, backing)
	entry := newTestEntry("alice", "A", time.Now().Add(time.Hour).UnixMilli(), 43)

	_, err := hook.BeforeExpire(context.Background(), olricconfig.DurableOperation{
		DMap:  "users",
		Key:   "alice",
		HKey:  HKey("users", "alice"),
		Entry: entry,
	})
	if err != nil {
		t.Fatalf("before expire: %v", err)
	}
	record := backing.records[HKey("users", "alice")]
	if record.TTL != entry.TTL() || record.Timestamp != entry.Timestamp() || record.Origin != "client_expire" || !record.FlushMySQL {
		t.Fatalf("unexpected expire record: %#v", record)
	}
}

func TestDurableHookPrepareCommitWithCommitStore(t *testing.T) {
	backing := newRecordingCommitStore()
	hook := newTestDurableHook(t, backing)
	entry := newTestEntry("alice", "A", time.Now().Add(time.Minute).UnixMilli(), 45)

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
		t.Fatal("expected prepared version")
	}
	record := backing.records[HKey("users", "alice")]
	if record.WALState != store.WALStatePrepared || record.FlushMySQL {
		t.Fatalf("expected prepared unflushable record, got %#v", record)
	}
	if err := hook.AfterSet(context.Background(), op); err != nil {
		t.Fatalf("after set: %v", err)
	}
	record = backing.records[HKey("users", "alice")]
	if record.WALState != store.WALStateCommitted || !record.FlushMySQL {
		t.Fatalf("expected committed flushable record, got %#v", record)
	}
}

func TestDurableHookLoadOnMissRefillsLocalOnlyRecord(t *testing.T) {
	backing := newRecordingStore()
	hook := newTestDurableHook(t, backing)
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
		DMap:  "users",
		Key:   "alice",
		HKey:  HKey("users", "alice"),
		Entry: template,
	})
	if err != nil {
		t.Fatalf("load on miss: %v", err)
	}
	if loaded.Key() != "alice" || string(loaded.Value()) != "A" {
		t.Fatalf("unexpected loaded entry: key=%q value=%q", loaded.Key(), loaded.Value())
	}
	if len(backing.calls) != 2 || backing.calls[0] != "load" || backing.calls[1] != "store" {
		t.Fatalf("expected backing load then local refill store, got %v", backing.calls)
	}
	refill := backing.records[HKey("users", "alice")]
	if refill.Origin != "mysql_refill" || refill.FlushMySQL {
		t.Fatalf("expected local-only refill record, got %#v", refill)
	}
}

func TestDurableHookLoadOnMissPropagatesMiss(t *testing.T) {
	hook := newTestDurableHook(t, newRecordingStore())

	_, err := hook.LoadOnMiss(context.Background(), olricconfig.DurableOperation{
		DMap:  "users",
		Key:   "missing",
		HKey:  HKey("users", "missing"),
		Entry: newTestEntry("", "", 0, 0),
	})
	if !errors.Is(err, olricstorage.ErrKeyNotFound) {
		t.Fatalf("expected backing miss, got %v", err)
	}
}

func newTestDurableHook(t *testing.T, backing store.CacheStore) *DurableHook {
	t.Helper()
	hook, err := NewDurableHook(backing)
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
