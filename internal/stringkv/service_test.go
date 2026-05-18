package stringkv

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/zhixiongdu/olricstack/internal/store"
)

func TestSetDelegatesToOlricWithoutIngressBackingWrite(t *testing.T) {
	backing := newRecordingStore()
	dmap := newFakeDMap()
	service := newTestService(t, newFakeProvider(dmap), backing)

	if err := service.Set(context.Background(), "users", "alice", "A", time.Minute); err != nil {
		t.Fatalf("set: %v", err)
	}
	if len(backing.calls) != 0 {
		t.Fatalf("ingress service must not write backing store, got %v", backing.calls)
	}
	if got := dmap.values["alice"]; got != "A" {
		t.Fatalf("expected olric value A, got %q", got)
	}
}

func TestSetIgnoresIngressBackingFailure(t *testing.T) {
	backing := newRecordingStore()
	backing.storeErr = errors.New("wal failed")
	dmap := newFakeDMap()
	service := newTestService(t, newFakeProvider(dmap), backing)

	if err := service.Set(context.Background(), "users", "alice", "A", 0); err != nil {
		t.Fatalf("set: %v", err)
	}
	if got := dmap.values["alice"]; got != "A" {
		t.Fatalf("expected olric write to proceed through owner path, got %q", got)
	}
	if len(backing.calls) != 0 {
		t.Fatalf("ingress service must not touch backing store, got %v", backing.calls)
	}
}

func TestDeleteDelegatesToOlricWithoutIngressTombstone(t *testing.T) {
	backing := newRecordingStore()
	dmap := newFakeDMap()
	service := newTestService(t, newFakeProvider(dmap), backing)

	if err := service.Delete(context.Background(), "users", "missing"); err != nil {
		t.Fatalf("delete: %v", err)
	}
	if len(backing.calls) != 0 {
		t.Fatalf("ingress service must not write tombstone, got %v", backing.calls)
	}
}

func TestGetDoesNotIngressRefillOnOlricMiss(t *testing.T) {
	backing := newRecordingStore()
	dmap := newFakeDMap()
	service := newTestService(t, newFakeProvider(dmap), backing)

	_, err := service.Get(context.Background(), "users", "alice")
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected olric miss without ingress refill, got %v", err)
	}
	if len(backing.calls) != 0 {
		t.Fatalf("ingress service must not load backing store, got %v", backing.calls)
	}
}

func TestExpireDelegatesToOlricWithoutIngressBackingWrite(t *testing.T) {
	backing := newRecordingStore()
	dmap := newFakeDMap()
	dmap.values["alice"] = "A"
	service := newTestService(t, newFakeProvider(dmap), backing)

	if err := service.Expire(context.Background(), "users", "alice", time.Minute); err != nil {
		t.Fatalf("expire: %v", err)
	}
	if dmap.ttls["alice"] != time.Minute {
		t.Fatalf("expected olric ttl update, got %s", dmap.ttls["alice"])
	}
	if len(backing.calls) != 0 {
		t.Fatalf("ingress service must not write ttl backing record, got %v", backing.calls)
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
	service, err := NewService(provider, lease)
	if err != nil {
		t.Fatalf("new service: %v", err)
	}
	return &testService{Service: service, lease: lease}
}

var _ store.CacheStore = (*recordingStore)(nil)

type recordingCommitStore struct {
	*recordingStore
	nextVersion    int64
	committedCount int
	abortedCount   int
	handoffCalls   [][]store.EntryRef
	handoffErr     error
}

func newRecordingCommitStore() *recordingCommitStore {
	return &recordingCommitStore{
		recordingStore: newRecordingStore(),
		nextVersion:    1,
	}
}

func (s *recordingCommitStore) PrepareEntry(ctx context.Context, record store.EntryRecord) (store.EntryRecord, error) {
	record.WALState = store.WALStatePrepared
	record.FlushMySQL = false
	return s.storePrepared(record)
}

func (s *recordingCommitStore) CommitEntry(ctx context.Context, ref store.EntryRef, version int64) error {
	record, ok := s.records[ref.HKey]
	if !ok || record.WALSeq != version {
		return store.ErrNotFound
	}
	record.WALState = store.WALStateCommitted
	record.FlushMySQL = true
	s.records[ref.HKey] = record.Clone()
	s.committedCount++
	return nil
}

func (s *recordingCommitStore) AbortEntry(ctx context.Context, ref store.EntryRef, version int64) error {
	record, ok := s.records[ref.HKey]
	if !ok {
		return nil
	}
	if record.WALSeq != version {
		return nil
	}
	if record.WALState == store.WALStateCommitted {
		return errors.New("cannot abort committed record")
	}
	delete(s.records, ref.HKey)
	s.abortedCount++
	return nil
}

func (s *recordingCommitStore) storePrepared(record store.EntryRecord) (store.EntryRecord, error) {
	if s.storeErr != nil {
		return store.EntryRecord{}, s.storeErr
	}
	record.WALSeq = s.nextVersion
	s.nextVersion++
	s.records[record.HKey] = record.Clone()
	return record.Clone(), nil
}

var _ store.CommitStore = (*recordingCommitStore)(nil)

func (s *recordingCommitStore) FlushHandoff(ctx context.Context, refs []store.EntryRef) error {
	cloned := append([]store.EntryRef(nil), refs...)
	s.handoffCalls = append(s.handoffCalls, cloned)
	return s.handoffErr
}
