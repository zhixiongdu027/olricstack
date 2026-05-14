package stringkv

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/zhixiongdu/olricstack/internal/store"
)

func TestSetWritesWALBeforeOlricAndFlushableMySQLRecord(t *testing.T) {
	backing := newRecordingStore()
	dmap := newFakeDMap()
	service := newTestService(t, newFakeProvider(dmap), backing)

	if err := service.Set(context.Background(), "users", "alice", "A", time.Minute); err != nil {
		t.Fatalf("set: %v", err)
	}
	if len(backing.calls) != 1 || backing.calls[0] != "store" {
		t.Fatalf("expected backing store before olric set, got %v", backing.calls)
	}
	record := backing.records[HKey("users", "alice")]
	if record.DMap != "users" || record.Key != "alice" || string(record.EncodedEntry) != "A" {
		t.Fatalf("unexpected record: %#v", record)
	}
	if record.Origin != "client_set" || !record.FlushMySQL {
		t.Fatalf("expected client_set flushable record, got %#v", record)
	}
	if got := dmap.values["alice"]; got != "A" {
		t.Fatalf("expected olric value A, got %q", got)
	}
}

func TestSetDoesNotApplyOlricWhenWALFails(t *testing.T) {
	backing := newRecordingStore()
	backing.storeErr = errors.New("wal failed")
	dmap := newFakeDMap()
	service := newTestService(t, newFakeProvider(dmap), backing)

	if err := service.Set(context.Background(), "users", "alice", "A", 0); !errors.Is(err, backing.storeErr) {
		t.Fatalf("expected wal error, got %v", err)
	}
	if got := dmap.values["alice"]; got != "" {
		t.Fatalf("olric should not be updated after wal failure, got %q", got)
	}
}

func TestDeleteWritesTombstoneEvenWhenOlricMisses(t *testing.T) {
	backing := newRecordingStore()
	dmap := newFakeDMap()
	service := newTestService(t, newFakeProvider(dmap), backing)

	if err := service.Delete(context.Background(), "users", "missing"); err != nil {
		t.Fatalf("delete: %v", err)
	}
	record := backing.records[HKey("users", "missing")]
	if !record.Tombstone || record.DMap != "users" || record.Key != "missing" {
		t.Fatalf("expected tombstone record, got %#v", record)
	}
	if len(backing.calls) != 1 || backing.calls[0] != "delete" {
		t.Fatalf("expected backing delete, got %v", backing.calls)
	}
}

func TestGetRefillsFromBackingOnOlricMiss(t *testing.T) {
	backing := newRecordingStore()
	backing.records[HKey("users", "alice")] = store.EntryRecord{
		DMap:         "users",
		Key:          "alice",
		HKey:         HKey("users", "alice"),
		EncodedEntry: []byte("A"),
		TTL:          time.Now().Add(time.Minute).UnixMilli(),
	}
	dmap := newFakeDMap()
	service := newTestService(t, newFakeProvider(dmap), backing)

	value, err := service.Get(context.Background(), "users", "alice")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if value != "A" {
		t.Fatalf("expected A, got %q", value)
	}
	if got := dmap.values["alice"]; got != "A" {
		t.Fatalf("expected olric refill A, got %q", got)
	}
	if len(backing.calls) != 2 || backing.calls[0] != "load" || backing.calls[1] != "store" {
		t.Fatalf("expected backing load then local refill store, got %v", backing.calls)
	}
	record := backing.records[HKey("users", "alice")]
	if record.Origin != "mysql_refill" || record.FlushMySQL {
		t.Fatalf("expected local-only mysql_refill record, got %#v", record)
	}
}

func TestExpirePersistsTTLBeforeOlricExpire(t *testing.T) {
	backing := newRecordingStore()
	dmap := newFakeDMap()
	dmap.values["alice"] = "A"
	service := newTestService(t, newFakeProvider(dmap), backing)

	if err := service.Expire(context.Background(), "users", "alice", time.Minute); err != nil {
		t.Fatalf("expire: %v", err)
	}
	record := backing.records[HKey("users", "alice")]
	if string(record.EncodedEntry) != "A" || record.TTL == 0 || record.Origin != "client_expire" || !record.FlushMySQL {
		t.Fatalf("unexpected ttl record: %#v", record)
	}
	if dmap.ttls["alice"] != time.Minute {
		t.Fatalf("expected olric ttl update, got %s", dmap.ttls["alice"])
	}
}

func TestLeaseRequired(t *testing.T) {
	backing := newRecordingStore()
	dmap := newFakeDMap()
	service := newTestService(t, newFakeProvider(dmap), backing)
	service.lease.allowed = false

	if err := service.Set(context.Background(), "users", "alice", "A", 0); !errors.Is(err, ErrLeaseExpired) {
		t.Fatalf("expected lease error, got %v", err)
	}
	if len(backing.calls) != 0 {
		t.Fatalf("backing should not be touched without lease, got %v", backing.calls)
	}
}

type fakeProvider struct {
	dmaps map[string]*fakeDMap
}

func newFakeProvider(dmap *fakeDMap) *fakeProvider {
	return &fakeProvider{dmaps: map[string]*fakeDMap{"users": dmap}}
}

func (p *fakeProvider) DMap(name string) (DMap, error) {
	dmap, ok := p.dmaps[name]
	if !ok {
		return nil, ErrNotFound
	}
	return dmap, nil
}

type fakeDMap struct {
	values map[string]string
	ttls   map[string]time.Duration
}

func newFakeDMap() *fakeDMap {
	return &fakeDMap{
		values: make(map[string]string),
		ttls:   make(map[string]time.Duration),
	}
}

func (d *fakeDMap) Get(ctx context.Context, key string) (string, error) {
	value, ok := d.values[key]
	if !ok {
		return "", ErrNotFound
	}
	return value, nil
}

func (d *fakeDMap) Set(ctx context.Context, key, value string, ttl time.Duration) error {
	d.values[key] = value
	d.ttls[key] = ttl
	return nil
}

func (d *fakeDMap) Delete(ctx context.Context, key string) error {
	delete(d.values, key)
	delete(d.ttls, key)
	return nil
}

func (d *fakeDMap) Expire(ctx context.Context, key string, ttl time.Duration) error {
	if _, ok := d.values[key]; !ok {
		return ErrNotFound
	}
	d.ttls[key] = ttl
	return nil
}

type recordingStore struct {
	records  map[uint64]store.EntryRecord
	calls    []string
	storeErr error
}

func newRecordingStore() *recordingStore {
	return &recordingStore{records: make(map[uint64]store.EntryRecord)}
}

func (s *recordingStore) LoadEntry(ctx context.Context, ref store.EntryRef) (store.EntryRecord, error) {
	s.calls = append(s.calls, "load")
	record, ok := s.records[ref.HKey]
	if !ok || record.Tombstone {
		return store.EntryRecord{}, store.ErrNotFound
	}
	return record.Clone(), nil
}

func (s *recordingStore) StoreEntry(ctx context.Context, record store.EntryRecord) error {
	s.calls = append(s.calls, "store")
	if s.storeErr != nil {
		return s.storeErr
	}
	s.records[record.HKey] = record.Clone()
	return nil
}

func (s *recordingStore) DeleteEntry(ctx context.Context, ref store.EntryRef) error {
	s.calls = append(s.calls, "delete")
	s.records[ref.HKey] = store.EntryRecord{
		DMap:      ref.DMap,
		Key:       ref.Key,
		HKey:      ref.HKey,
		Tombstone: true,
	}
	return nil
}

func (s *recordingStore) Close(ctx context.Context) error {
	return nil
}

type fakeLease struct {
	allowed bool
}

func (l *fakeLease) ServingAllowed(time.Time) bool {
	return l.allowed
}

type testService struct {
	*Service
	lease *fakeLease
}

func newTestService(t *testing.T, provider DMapProvider, backing store.CacheStore) *testService {
	t.Helper()
	lease := &fakeLease{allowed: true}
	service, err := NewService(provider, backing, lease)
	if err != nil {
		t.Fatalf("new service: %v", err)
	}
	return &testService{Service: service, lease: lease}
}

var _ store.CacheStore = (*recordingStore)(nil)
