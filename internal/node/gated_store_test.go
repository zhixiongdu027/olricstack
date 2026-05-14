package node

import (
	"context"
	"errors"
	"testing"
	"time"

	topologypb "github.com/zhixiongdu/olricstack/api/topology/v1"
	"github.com/zhixiongdu/olricstack/internal/store"
)

func TestLeaseGatedStoreRejectsWithoutLease(t *testing.T) {
	inner := &memoryStore{}
	gated, err := NewLeaseGatedStore(inner, NewLeaseTracker())
	if err != nil {
		t.Fatalf("new gated store: %v", err)
	}

	if err := gated.StoreEntry(context.Background(), testRecord("a", 1, []byte("1"))); !errors.Is(err, ErrTopologyLeaseRequired) {
		t.Fatalf("expected lease required on store, got %v", err)
	}
	if _, err := gated.LoadEntry(context.Background(), testRef("a", 1)); !errors.Is(err, ErrTopologyLeaseRequired) {
		t.Fatalf("expected lease required on load, got %v", err)
	}
	if err := gated.DeleteEntry(context.Background(), testRef("a", 1)); !errors.Is(err, ErrTopologyLeaseRequired) {
		t.Fatalf("expected lease required on delete, got %v", err)
	}
}

func TestLeaseGatedStoreAllowsWithValidLease(t *testing.T) {
	now := time.Now()
	lease := NewLeaseTracker()
	if err := lease.Apply(&topologypb.TopologyEnvelope{
		WatchdogRole:       topologypb.WatchdogRole_WATCHDOG_ROLE_PRIMARY,
		WatchdogGeneration: 1,
		ValidUntilUnixMs:   now.Add(time.Minute).UnixMilli(),
	}, now); err != nil {
		t.Fatalf("apply lease: %v", err)
	}
	inner := &memoryStore{values: make(map[uint64]store.EntryRecord)}
	gated, err := NewLeaseGatedStore(inner, lease)
	if err != nil {
		t.Fatalf("new gated store: %v", err)
	}
	gated.now = func() time.Time { return now }

	if err := gated.StoreEntry(context.Background(), testRecord("a", 1, []byte("1"))); err != nil {
		t.Fatalf("store with lease: %v", err)
	}
	record, err := gated.LoadEntry(context.Background(), testRef("a", 1))
	if err != nil {
		t.Fatalf("load with lease: %v", err)
	}
	if string(record.EncodedEntry) != "1" {
		t.Fatalf("expected value 1, got %q", record.EncodedEntry)
	}
	if err := gated.DeleteEntry(context.Background(), testRef("a", 1)); err != nil {
		t.Fatalf("delete with lease: %v", err)
	}
	if _, err := gated.LoadEntry(context.Background(), testRef("a", 1)); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("expected deleted record miss, got %v", err)
	}
}

func TestLeaseGatedStoreRejectsAfterLeaseExpires(t *testing.T) {
	now := time.Now()
	lease := NewLeaseTracker()
	if err := lease.Apply(&topologypb.TopologyEnvelope{
		WatchdogRole:       topologypb.WatchdogRole_WATCHDOG_ROLE_PRIMARY,
		WatchdogGeneration: 1,
		ValidUntilUnixMs:   now.Add(time.Second).UnixMilli(),
	}, now); err != nil {
		t.Fatalf("apply lease: %v", err)
	}
	gated, err := NewLeaseGatedStore(&memoryStore{values: make(map[uint64]store.EntryRecord)}, lease)
	if err != nil {
		t.Fatalf("new gated store: %v", err)
	}
	gated.now = func() time.Time { return now.Add(2 * time.Second) }

	if err := gated.StoreEntry(context.Background(), testRecord("a", 1, []byte("1"))); !errors.Is(err, ErrTopologyLeaseRequired) {
		t.Fatalf("expected lease required after expiry, got %v", err)
	}
}

type memoryStore struct {
	values map[uint64]store.EntryRecord
	closed bool
}

func (s *memoryStore) LoadEntry(ctx context.Context, ref store.EntryRef) (store.EntryRecord, error) {
	value, ok := s.values[ref.HKey]
	if !ok || value.Tombstone {
		return store.EntryRecord{}, store.ErrNotFound
	}
	return value.Clone(), nil
}

func (s *memoryStore) StoreEntry(ctx context.Context, record store.EntryRecord) error {
	if s.values == nil {
		s.values = make(map[uint64]store.EntryRecord)
	}
	s.values[record.HKey] = record.Clone()
	return nil
}

func (s *memoryStore) DeleteEntry(ctx context.Context, ref store.EntryRef) error {
	if s.values == nil {
		s.values = make(map[uint64]store.EntryRecord)
	}
	s.values[ref.HKey] = store.EntryRecord{Key: ref.Key, HKey: ref.HKey, Tombstone: true}
	return nil
}

func (s *memoryStore) Close(ctx context.Context) error {
	s.closed = true
	return nil
}

func testRef(key string, hkey uint64) store.EntryRef {
	return store.EntryRef{Key: key, HKey: hkey}
}

func testRecord(key string, hkey uint64, value []byte) store.EntryRecord {
	return store.EntryRecord{
		Key:          key,
		HKey:         hkey,
		EncodedEntry: append([]byte(nil), value...),
	}
}
