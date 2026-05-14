package olricstore

import (
	"context"
	"testing"

	olricstorage "github.com/olric-data/olric/pkg/storage"
	"github.com/zhixiongdu/olricstack/internal/store"
)

func TestEnginePutWritesOnlyLocalMemory(t *testing.T) {
	backing := newMemoryStore()
	engine := New(backing)
	entry := engine.NewEntry()
	entry.SetKey("key-a")
	entry.SetValue([]byte("value-a"))
	entry.SetTTL(10)
	entry.SetTimestamp(20)

	if err := engine.Put(42, entry); err != nil {
		t.Fatalf("put: %v", err)
	}
	if len(backing.records) != 0 {
		t.Fatalf("generic engine put must not persist backing records: %#v", backing.records)
	}
	got, err := engine.Get(42)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if string(got.Value()) != "value-a" {
		t.Fatalf("expected value-a, got %q", got.Value())
	}
}

func TestEngineDoesNotLoadFromBackingOnGenericGet(t *testing.T) {
	backing := newMemoryStore()
	first := New(backing)
	entry := first.NewEntry()
	entry.SetKey("reload-key")
	entry.SetValue([]byte("reload-value"))
	entry.SetTTL(10)
	entry.SetTimestamp(20)
	entry.SetLastAccess(30)
	if err := first.Put(64, entry); err != nil {
		t.Fatalf("put first: %v", err)
	}

	second := New(backing)
	if _, err := second.Get(64); err != olricstorage.ErrKeyNotFound {
		t.Fatalf("expected generic get to avoid backing load, got %v", err)
	}
	if second.Check(64) {
		t.Fatal("generic get should not cache a backing value")
	}
	if len(backing.records) != 0 {
		t.Fatalf("generic engine put must not persist backing records: %#v", backing.records)
	}
}

func TestEnginePutRawRoundTrip(t *testing.T) {
	backing := newMemoryStore()
	engine := New(backing)
	entry := engine.NewEntry()
	entry.SetKey("raw-key")
	entry.SetValue([]byte("raw-value"))
	entry.SetTTL(10)
	entry.SetTimestamp(20)
	entry.SetLastAccess(30)

	if err := engine.PutRaw(7, entry.Encode()); err != nil {
		t.Fatalf("put raw: %v", err)
	}
	got, err := engine.Get(7)
	if err != nil {
		t.Fatalf("get raw: %v", err)
	}
	if got.Key() != "raw-key" || string(got.Value()) != "raw-value" || got.TTL() != 10 || got.Timestamp() != 20 || got.LastAccess() != 30 {
		t.Fatalf("unexpected entry: %#v", got)
	}
	if len(backing.records) != 0 {
		t.Fatalf("generic PutRaw must not persist backing records: %#v", backing.records)
	}
}

func TestEngineDeleteOnlyDeletesLocalMemory(t *testing.T) {
	backing := newMemoryStore()
	engine := New(backing)
	entry := engine.NewEntry()
	entry.SetKey("delete-key")
	entry.SetValue([]byte("delete-value"))
	if err := engine.Put(9, entry); err != nil {
		t.Fatalf("put: %v", err)
	}
	if err := engine.Delete(9); err != nil {
		t.Fatalf("delete: %v", err)
	}
	if _, err := engine.Get(9); err != olricstorage.ErrKeyNotFound {
		t.Fatalf("expected miss after delete, got %v", err)
	}
	if len(backing.tombstones) != 0 {
		t.Fatalf("generic delete must not persist tombstones: %#v", backing.tombstones)
	}
}

func TestEngineUpdateTTLOnlyUpdatesLocalMemory(t *testing.T) {
	backing := newMemoryStore()
	engine := New(backing)
	entry := engine.NewEntry()
	entry.SetKey("ttl-key")
	entry.SetValue([]byte("ttl-value"))
	if err := engine.Put(10, entry); err != nil {
		t.Fatalf("put: %v", err)
	}
	update := engine.NewEntry()
	update.SetTTL(100)
	update.SetTimestamp(200)
	update.SetLastAccess(300)
	if err := engine.UpdateTTL(10, update); err != nil {
		t.Fatalf("update ttl: %v", err)
	}
	got, err := engine.Get(10)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if got.TTL() != 100 || got.Timestamp() != 200 || got.LastAccess() != 300 {
		t.Fatalf("unexpected ttl update: %#v", got)
	}
	if len(backing.records) != 0 {
		t.Fatalf("generic ttl update must not persist backing records: %#v", backing.records)
	}
}

func TestEngineUpdateTTLDoesNotLoadFromBackingOnMemoryMiss(t *testing.T) {
	backing := newMemoryStore()
	backing.records[11] = store.EntryRecord{Key: "ttl-reload", HKey: 11, TTL: 10}

	engine := New(backing)
	update := engine.NewEntry()
	update.SetTTL(500)
	update.SetTimestamp(600)
	update.SetLastAccess(700)
	if err := engine.UpdateTTL(11, update); err != olricstorage.ErrKeyNotFound {
		t.Fatalf("expected ttl update to avoid backing load, got %v", err)
	}
	if backing.records[11].TTL != 10 {
		t.Fatalf("generic ttl update should not mutate backing, got %#v", backing.records[11])
	}
}

func TestEngineStartReplaysWALBeforeLoads(t *testing.T) {
	backing := newMemoryStore()
	entry := (&Engine{}).NewEntry()
	entry.SetKey("replay-key")
	entry.SetValue([]byte("replay-value"))
	backing.replay = []store.EntryRecord{{
		Key:          "replay-key",
		HKey:         15,
		EncodedEntry: entry.Encode(),
	}}
	engine := New(backing)
	if err := engine.Start(); err != nil {
		t.Fatalf("start: %v", err)
	}
	if !backing.started {
		t.Fatal("expected backing flusher start after replay")
	}
	got, err := engine.Get(15)
	if err != nil {
		t.Fatalf("get replayed entry: %v", err)
	}
	if got.Key() != "replay-key" || string(got.Value()) != "replay-value" {
		t.Fatalf("unexpected replayed entry: %#v", got)
	}
}

func TestEngineTransferIteratorExportImportDrop(t *testing.T) {
	source := New(nil)
	entry := source.NewEntry()
	entry.SetKey("move-key")
	entry.SetValue([]byte("move-value"))
	if err := source.Put(9, entry); err != nil {
		t.Fatalf("put source: %v", err)
	}

	iterator := source.TransferIterator()
	if !iterator.Next() {
		t.Fatal("expected transfer iterator item")
	}
	payload, index, err := iterator.Export()
	if err != nil {
		t.Fatalf("export: %v", err)
	}

	target := New(nil)
	if err := target.Import(payload, func(hkey uint64, entry olricstorage.Entry) error {
		return target.Put(hkey, entry)
	}); err != nil {
		t.Fatalf("import: %v", err)
	}
	got, err := target.Get(9)
	if err != nil {
		t.Fatalf("target get: %v", err)
	}
	if got.Key() != "move-key" || string(got.Value()) != "move-value" {
		t.Fatalf("unexpected moved entry: %#v", got)
	}

	if err := iterator.Drop(index); err != nil {
		t.Fatalf("drop: %v", err)
	}
	if _, err := source.Get(9); err != olricstorage.ErrKeyNotFound {
		t.Fatalf("expected source miss after drop, got %v", err)
	}
}

type memoryStore struct {
	records    map[uint64]store.EntryRecord
	tombstones map[uint64]bool
	replay     []store.EntryRecord
	started    bool
}

func newMemoryStore() *memoryStore {
	return &memoryStore{
		records:    make(map[uint64]store.EntryRecord),
		tombstones: make(map[uint64]bool),
	}
}

func (s *memoryStore) LoadEntry(ctx context.Context, ref store.EntryRef) (store.EntryRecord, error) {
	record, ok := s.records[ref.HKey]
	if !ok || s.tombstones[ref.HKey] {
		return store.EntryRecord{}, store.ErrNotFound
	}
	return record.Clone(), nil
}

func (s *memoryStore) StoreEntry(ctx context.Context, record store.EntryRecord) error {
	if s.records == nil {
		s.records = make(map[uint64]store.EntryRecord)
	}
	s.records[record.HKey] = record.Clone()
	delete(s.tombstones, record.HKey)
	return nil
}

func (s *memoryStore) DeleteEntry(ctx context.Context, ref store.EntryRef) error {
	if s.tombstones == nil {
		s.tombstones = make(map[uint64]bool)
	}
	s.tombstones[ref.HKey] = true
	return nil
}

func (s *memoryStore) Replay(ctx context.Context, f func(store.EntryRecord) error) error {
	for _, record := range s.replay {
		if err := f(record); err != nil {
			return err
		}
	}
	return nil
}

func (s *memoryStore) Start(ctx context.Context) error {
	s.started = true
	return nil
}

func (s *memoryStore) Close(ctx context.Context) error {
	return nil
}

func decodeEntry(t *testing.T, raw []byte) olricstorage.Entry {
	t.Helper()
	if len(raw) == 0 {
		t.Fatal("empty encoded entry")
	}
	entry := &Entry{}
	entry.Decode(raw)
	return entry
}

var _ store.CacheStore = (*memoryStore)(nil)
var _ store.ReplayStore = (*memoryStore)(nil)
var _ store.Starter = (*memoryStore)(nil)
