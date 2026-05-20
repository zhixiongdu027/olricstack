package sidecar

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/zhixiongdu/olricstack/internal/oplog"
	"github.com/zhixiongdu/olricstack/internal/ring"
	"github.com/zhixiongdu/olricstack/internal/store"
)

type stubBackend struct {
	mu            sync.Mutex
	upserts       []store.EntryRecord
	upsertErr     error
	purgeCount    int
	loadRec       store.EntryRecord
	loadFound     bool
	loadErr       error
	upsertCalled  chan struct{}
	blockUpsert   chan struct{}
	upsertEntered chan struct{}
}

func newStubBackend() *stubBackend {
	return &stubBackend{upsertCalled: make(chan struct{}, 16)}
}

func (s *stubBackend) UpsertEntries(_ context.Context, records []store.EntryRecord) error {
	if s.upsertEntered != nil {
		select {
		case s.upsertEntered <- struct{}{}:
		default:
		}
	}
	if s.blockUpsert != nil {
		<-s.blockUpsert
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.upsertErr != nil {
		return s.upsertErr
	}
	s.upserts = append(s.upserts, records...)
	select {
	case s.upsertCalled <- struct{}{}:
	default:
	}
	return nil
}

func (s *stubBackend) LoadFromMySQL(_ context.Context, _ store.EntryRef) (store.EntryRecord, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.loadErr != nil {
		return store.EntryRecord{}, s.loadErr
	}
	if !s.loadFound {
		return store.EntryRecord{}, store.ErrNotFound
	}
	return s.loadRec, nil
}

func (s *stubBackend) PurgeBelowGeneration(_ context.Context, _ int64) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.purgeCount, nil
}

func (s *stubBackend) Close(_ context.Context) error { return nil }

func (s *stubBackend) snapshotUpserts() []store.EntryRecord {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]store.EntryRecord, len(s.upserts))
	copy(out, s.upserts)
	return out
}

func newServerOnRing(t *testing.T, backend Backend) (*Server, *ring.Producer, func()) {
	t.Helper()
	path := t.TempDir() + "/ring.dat"
	if err := ring.Create(path, 64*1024); err != nil {
		t.Fatalf("ring create: %v", err)
	}
	prod, err := ring.OpenProducer(path)
	if err != nil {
		t.Fatalf("ring open producer: %v", err)
	}
	cons, err := ring.OpenConsumer(path)
	if err != nil {
		t.Fatalf("ring open consumer: %v", err)
	}
	srv, err := NewServer(cons, backend, Config{BatchSize: 8, IdlePoll: 10 * time.Millisecond, FlushTimeout: time.Second})
	if err != nil {
		t.Fatalf("new server: %v", err)
	}
	cleanup := func() {
		_ = prod.Close()
		_ = cons.Close()
	}
	return srv, prod, cleanup
}

func appendEntry(t *testing.T, prod *ring.Producer, e oplog.Entry) {
	t.Helper()
	payload, err := oplog.Encode(e)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	if err := prod.Append(payload); err != nil {
		t.Fatalf("append: %v", err)
	}
}

func runServer(t *testing.T, srv *Server) (context.CancelFunc, <-chan error) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- srv.Run(ctx) }()
	return cancel, done
}

func TestConsumerDrainsRingToBackend(t *testing.T) {
	backend := newStubBackend()
	srv, prod, cleanup := newServerOnRing(t, backend)
	defer cleanup()

	cancel, done := runServer(t, srv)
	defer func() { cancel(); <-done }()

	appendEntry(t, prod, oplog.Entry{
		Op: oplog.OpSet, DMap: "users", Key: "alice", HKey: 1,
		Generation: 7, Epoch: 3, OwnerSeq: 1, WriterID: "node-A",
		EncodedEntry: []byte("v"),
	})
	if _, err := srv.Notify(context.Background(), &NotifyRequest{Pending: 1}); err != nil {
		t.Fatalf("notify: %v", err)
	}

	select {
	case <-backend.upsertCalled:
	case <-time.After(2 * time.Second):
		t.Fatal("expected backend upsert within 2s")
	}
	got := backend.snapshotUpserts()
	if len(got) != 1 || got[0].Key != "alice" || got[0].Tombstone {
		t.Fatalf("unexpected upserts: %#v", got)
	}
}

func TestLoadFromPendingTakesPriorityOverBackend(t *testing.T) {
	backend := newStubBackend()
	backend.loadFound = true
	backend.loadRec = store.EntryRecord{Key: "stale", EncodedEntry: []byte("stale")}

	srv, prod, cleanup := newServerOnRing(t, backend)
	defer cleanup()

	// No Run() — we drive drainRing manually so the entry stays in pending.
	appendEntry(t, prod, oplog.Entry{
		Op: oplog.OpSet, DMap: "users", Key: "alice", HKey: 42,
		Generation: 1, Epoch: 1, OwnerSeq: 5, WriterID: "node-A",
		EncodedEntry: []byte("fresh"),
	})
	if _, err := srv.drainRing(); err != nil {
		t.Fatalf("drain ring: %v", err)
	}

	resp, err := srv.LoadFromMySQL(context.Background(), &LoadRequest{DMap: "users", Key: "alice", HKey: 42})
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if !resp.Found || string(resp.Record.EncodedEntry) != "fresh" {
		t.Fatalf("expected fresh pending entry to win over backend, got %#v", resp)
	}
}

func TestLoadFallsBackToBackendAfterFlush(t *testing.T) {
	backend := newStubBackend()
	backend.loadFound = true
	backend.loadRec = store.EntryRecord{Key: "from-mysql", EncodedEntry: []byte("from-mysql")}

	srv, prod, cleanup := newServerOnRing(t, backend)
	defer cleanup()

	cancel, done := runServer(t, srv)
	defer func() { cancel(); <-done }()

	appendEntry(t, prod, oplog.Entry{
		Op: oplog.OpSet, DMap: "users", Key: "alice", HKey: 42,
		Generation: 1, Epoch: 1, OwnerSeq: 5, WriterID: "node-A",
		EncodedEntry: []byte("fresh"),
	})
	if _, err := srv.Notify(context.Background(), &NotifyRequest{Pending: 1}); err != nil {
		t.Fatalf("notify: %v", err)
	}
	select {
	case <-backend.upsertCalled:
	case <-time.After(2 * time.Second):
		t.Fatal("expected upsert")
	}

	resp, err := srv.LoadFromMySQL(context.Background(), &LoadRequest{DMap: "users", Key: "alice", HKey: 42})
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if !resp.Found || string(resp.Record.EncodedEntry) != "from-mysql" {
		t.Fatalf("expected backend value after flush, got %#v", resp)
	}
}

func TestLoadTombstoneInPendingReportsNotFound(t *testing.T) {
	backend := newStubBackend()
	srv, prod, cleanup := newServerOnRing(t, backend)
	defer cleanup()
	cancel, done := runServer(t, srv)
	defer func() { cancel(); <-done }()

	appendEntry(t, prod, oplog.Entry{
		Op: oplog.OpDelete, DMap: "users", Key: "alice", HKey: 7,
		Generation: 1, Epoch: 1, OwnerSeq: 1, WriterID: "node-A",
	})
	_, _ = srv.Notify(context.Background(), &NotifyRequest{Pending: 1})
	select {
	case <-backend.upsertCalled:
	case <-time.After(2 * time.Second):
		t.Fatal("expected upsert")
	}

	resp, err := srv.LoadFromMySQL(context.Background(), &LoadRequest{DMap: "users", Key: "alice", HKey: 7})
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if resp.Found {
		t.Fatalf("tombstone should report not found")
	}
}

func TestLoadExpiredRecordReportsNotFound(t *testing.T) {
	backend := newStubBackend()
	backend.loadFound = true
	backend.loadRec = store.EntryRecord{
		DMap:         "users",
		Key:          "alice",
		HKey:         7,
		TTL:          time.Now().Add(-time.Second).UnixMilli(),
		EncodedEntry: []byte("expired"),
	}

	srv, _, cleanup := newServerOnRing(t, backend)
	defer cleanup()

	resp, err := srv.LoadFromMySQL(context.Background(), &LoadRequest{DMap: "users", Key: "alice", HKey: 7})
	if err != nil {
		t.Fatalf("load expired record: %v", err)
	}
	if resp.Found {
		t.Fatalf("expired record should report not found")
	}
}

func TestRecordExpiredAt(t *testing.T) {
	now := time.UnixMilli(1000)
	if recordExpiredAt(store.EntryRecord{TTL: 0}, now) {
		t.Fatal("zero ttl must not expire")
	}
	if recordExpiredAt(store.EntryRecord{TTL: 1001}, now) {
		t.Fatal("future ttl must not expire")
	}
	if !recordExpiredAt(store.EntryRecord{TTL: 999}, now) {
		t.Fatal("past ttl must expire")
	}
}

func TestDrainPartitionFlushesSubsetSynchronously(t *testing.T) {
	backend := newStubBackend()
	srv, prod, cleanup := newServerOnRing(t, backend)
	defer cleanup()

	appendEntry(t, prod, oplog.Entry{Op: oplog.OpSet, DMap: "users", Key: "k0", HKey: 0,
		Generation: 1, Epoch: 1, OwnerSeq: 1, WriterID: "node-A"})
	appendEntry(t, prod, oplog.Entry{Op: oplog.OpSet, DMap: "users", Key: "k1", HKey: 1,
		Generation: 1, Epoch: 1, OwnerSeq: 1, WriterID: "node-A"})

	if _, err := srv.DrainPartition(context.Background(), &DrainPartitionRequest{
		DMap: "users", PartitionID: 1, PartitionCount: 2,
	}); err != nil {
		t.Fatalf("drain: %v", err)
	}
	got := backend.snapshotUpserts()
	if len(got) != 1 || got[0].Key != "k1" {
		t.Fatalf("expected only odd partition flushed, got %#v", got)
	}
}

func TestPurgeBelowGenerationCleansPending(t *testing.T) {
	backend := newStubBackend()
	backend.purgeCount = 5
	srv, prod, cleanup := newServerOnRing(t, backend)
	defer cleanup()

	cancel, done := runServer(t, srv)
	defer func() { cancel(); <-done }()

	appendEntry(t, prod, oplog.Entry{Op: oplog.OpSet, DMap: "users", Key: "old", HKey: 1,
		Generation: 1, Epoch: 1, OwnerSeq: 1, WriterID: "node-A"})
	_, _ = srv.Notify(context.Background(), &NotifyRequest{Pending: 1})
	select {
	case <-backend.upsertCalled:
	case <-time.After(2 * time.Second):
		t.Fatal("expected upsert")
	}

	appendEntry(t, prod, oplog.Entry{Op: oplog.OpSet, DMap: "users", Key: "stale", HKey: 9,
		Generation: 1, Epoch: 1, OwnerSeq: 1, WriterID: "node-A"})
	deadline := time.Now().Add(1 * time.Second)
	for time.Now().Before(deadline) {
		if _, ok := srv.pending.get(store.EntryRef{DMap: "users", Key: "stale", HKey: 9}); ok {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	resp, err := srv.PurgeBelowGeneration(context.Background(), &PurgeRequest{MinGeneration: 5})
	if err != nil {
		t.Fatalf("purge: %v", err)
	}
	if resp.Purged < 5 {
		t.Fatalf("expected backend purge>=5, got %d", resp.Purged)
	}
}

func TestFlushDoesNotDropNewerPendingEntryForSameRef(t *testing.T) {
	backend := newStubBackend()
	backend.blockUpsert = make(chan struct{})
	backend.upsertEntered = make(chan struct{}, 1)
	srv, prod, cleanup := newServerOnRing(t, backend)
	defer cleanup()

	appendEntry(t, prod, oplog.Entry{
		Op: oplog.OpSet, DMap: "users", Key: "alice", HKey: 42,
		Generation: 1, Epoch: 1, OwnerSeq: 1, WriterID: "node-A",
		EncodedEntry: []byte("old"),
	})
	if _, err := srv.drainRing(); err != nil {
		t.Fatalf("drain old entry: %v", err)
	}

	done := make(chan error, 1)
	go func() { done <- srv.flushOnce(context.Background()) }()
	select {
	case <-backend.upsertEntered:
	case <-time.After(time.Second):
		t.Fatal("expected flush to enter backend")
	}

	appendEntry(t, prod, oplog.Entry{
		Op: oplog.OpSet, DMap: "users", Key: "alice", HKey: 42,
		Generation: 1, Epoch: 1, OwnerSeq: 2, WriterID: "node-A",
		EncodedEntry: []byte("new"),
	})
	if _, err := srv.drainRing(); err != nil {
		t.Fatalf("drain new entry while old flush is blocked: %v", err)
	}
	close(backend.blockUpsert)
	if err := <-done; err != nil {
		t.Fatalf("flush old snapshot: %v", err)
	}

	resp, err := srv.LoadFromMySQL(context.Background(), &LoadRequest{DMap: "users", Key: "alice", HKey: 42})
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if !resp.Found || string(resp.Record.EncodedEntry) != "new" || resp.Record.OwnerSeq != 2 {
		t.Fatalf("expected newer pending entry to survive old snapshot drop, got %#v", resp)
	}
}
