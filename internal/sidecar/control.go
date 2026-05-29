// Package sidecar implements the olric-sidecar control plane: a unix-socket
// gRPC server exposing operations the node cannot serve from its in-memory
// state, plus a consumer loop that drains the shm ring into MySQL.
//
// Wire layout — only two channels exist between olric-node and olric-sidecar:
//
//   - shm ring (data path): node Append, sidecar consumer Pop. ACK semantics
//     are "Append returned success" — durability is bounded by Pod lifetime.
//     The sidecar drains asynchronously into MySQL, batching for throughput.
//   - unix-socket gRPC (control path): Notify wakes the consumer on demand,
//     LoadFromMySQL serves miss-path reads, DrainPartition synchronously
//     flushes a partition for handoff, PurgeBelowGeneration drops stale rows,
//     Shutdown cooperatively drains.
package sidecar

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/zhixiongdu/olricstack/internal/oplog"
	"github.com/zhixiongdu/olricstack/internal/ring"
	"github.com/zhixiongdu/olricstack/internal/store"
	"google.golang.org/grpc"
	"google.golang.org/grpc/encoding"
)

const (
	ServiceName  = "sidecar.v1.OplogControl"
	defaultDrain = 200 * time.Millisecond
)

type jsonCodec struct{}

func (jsonCodec) Marshal(v any) ([]byte, error)      { return json.Marshal(v) }
func (jsonCodec) Unmarshal(data []byte, v any) error { return json.Unmarshal(data, v) }
func (jsonCodec) Name() string                       { return "json" }

func init() {
	encoding.RegisterCodec(jsonCodec{})
}

// NotifyRequest tells the sidecar that fresh entries are pending in the shm
// ring. The sidecar may already be draining; Notify is purely a wake-up.
type NotifyRequest struct {
	Pending uint64 `json:"pending"`
}

type Empty struct{}

// LoadRequest asks the sidecar to return the latest committed value for
// (DMap, Key, HKey). It looks first in pending in-memory entries (already in
// shm but not yet flushed) then falls back to MySQL.
type LoadRequest struct {
	DMap string `json:"dmap"`
	Key  string `json:"key"`
	HKey uint64 `json:"hkey"`
}

type LoadResponse struct {
	Found  bool              `json:"found"`
	Record store.EntryRecord `json:"record,omitempty"`
}

// DrainPartitionRequest synchronously flushes every pending entry whose hkey
// belongs to (PartitionID, PartitionCount) into MySQL before returning.
type DrainPartitionRequest struct {
	DMap           string `json:"dmap"`
	PartitionID    uint64 `json:"partition_id"`
	PartitionCount uint64 `json:"partition_count"`
}

// PurgeRequest removes pending entries with Generation < MinGeneration. It
// only affects the in-sidecar pending set; MySQL rows are governed by the
// fence comparator at upsert time.
type PurgeRequest struct {
	MinGeneration int64 `json:"min_generation"`
}

type PurgeResponse struct {
	Purged int `json:"purged"`
}

// Backend abstracts the durable persistence the sidecar manages. The
// production implementation is *MySQLBackend; tests substitute a stub.
type Backend interface {
	UpsertEntries(ctx context.Context, records []store.EntryRecord) error
	LoadFromMySQL(ctx context.Context, ref store.EntryRef) (store.EntryRecord, error)
	PurgeBelowGeneration(ctx context.Context, minGeneration int64) (int, error)
	Close(ctx context.Context) error
}

// pending holds the not-yet-flushed entries seen by the consumer. Lookups
// during LoadFromMySQL serve from here first to preserve read-your-write
// after Append returned but before MySQL upsert completed.
type pending struct {
	mu    sync.Mutex
	bytes uint64

	entries map[store.EntryRef]oplog.Entry
}

func newPending() *pending {
	return &pending{entries: make(map[store.EntryRef]oplog.Entry)}
}

func (p *pending) put(e oplog.Entry) {
	p.mu.Lock()
	defer p.mu.Unlock()
	ref := store.EntryRef{DMap: e.DMap, Key: e.Key, HKey: e.HKey}
	if existing, ok := p.entries[ref]; ok {
		p.bytes -= pendingEntryBytes(existing)
	}
	p.entries[ref] = e
	p.bytes += pendingEntryBytes(e)
}

func (p *pending) snapshot() []oplog.Entry {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := make([]oplog.Entry, 0, len(p.entries))
	for _, e := range p.entries {
		out = append(out, e)
	}
	return out
}

// drop removes ref iff its in-pending OwnerSeq is <= the seq we just flushed.
// A newer Append could have overwritten the entry between snapshot and drop;
// we must keep the newer one so it gets flushed in the next round.
func (p *pending) drop(ref store.EntryRef, ownerSeq int64) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if e, ok := p.entries[ref]; ok && e.OwnerSeq <= ownerSeq {
		delete(p.entries, ref)
		p.bytes -= pendingEntryBytes(e)
	}
}

func (p *pending) get(ref store.EntryRef) (oplog.Entry, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	e, ok := p.entries[ref]
	return e, ok
}

func (p *pending) purgeBelow(generation int64) int {
	p.mu.Lock()
	defer p.mu.Unlock()
	dropped := 0
	for ref, e := range p.entries {
		if e.Generation < generation {
			delete(p.entries, ref)
			p.bytes -= pendingEntryBytes(e)
			dropped++
		}
	}
	return dropped
}

func (p *pending) drainPartition(dmap string, partitionID, partitionCount uint64) []oplog.Entry {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := make([]oplog.Entry, 0)
	for _, e := range p.entries {
		if e.DMap != dmap {
			continue
		}
		if e.HKey%partitionCount != partitionID {
			continue
		}
		out = append(out, e)
	}
	return out
}

func (p *pending) size() (records int, bytes uint64) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.entries), p.bytes
}

func pendingEntryBytes(e oplog.Entry) uint64 {
	return uint64(len(e.DMap) + len(e.Key) + len(e.WriterID) + len(e.EncodedEntry) + 64)
}

// Server is the sidecar control RPC server plus the ring consumer loop.
type Server struct {
	consumer *ring.Consumer
	backend  Backend
	pending  *pending

	wakeCh chan struct{}
	doneCh chan struct{}

	batchSize         int
	idlePoll          time.Duration
	flushTimeout      time.Duration
	maxPendingRecords int
	maxPendingBytes   uint64
}

// Config tunes consumer behavior.
type Config struct {
	BatchSize    int
	IdlePoll     time.Duration
	FlushTimeout time.Duration
	// MaxPendingRecords caps sidecar memory pending entries. When reached, the
	// consumer stops advancing the ring head so backpressure reaches the node.
	// Zero keeps the historical unbounded behavior.
	MaxPendingRecords int
	// MaxPendingBytes caps approximate pending payload bytes. Zero means
	// unbounded by bytes.
	MaxPendingBytes uint64
}

func (c Config) withDefaults() Config {
	if c.BatchSize <= 0 {
		c.BatchSize = 256
	}
	if c.IdlePoll <= 0 {
		c.IdlePoll = defaultDrain
	}
	if c.FlushTimeout <= 0 {
		c.FlushTimeout = 30 * time.Second
	}
	return c
}

func NewServer(consumer *ring.Consumer, backend Backend, cfg Config) (*Server, error) {
	if consumer == nil {
		return nil, errors.New("ring consumer is nil")
	}
	if backend == nil {
		return nil, errors.New("backend is nil")
	}
	cfg = cfg.withDefaults()
	return &Server{
		consumer:          consumer,
		backend:           backend,
		pending:           newPending(),
		wakeCh:            make(chan struct{}, 1),
		doneCh:            make(chan struct{}),
		batchSize:         cfg.BatchSize,
		idlePoll:          cfg.IdlePoll,
		flushTimeout:      cfg.FlushTimeout,
		maxPendingRecords: cfg.MaxPendingRecords,
		maxPendingBytes:   cfg.MaxPendingBytes,
	}, nil
}

// Run blocks until ctx is cancelled. It alternates between draining the ring
// into pending and flushing pending into MySQL.
func (s *Server) Run(ctx context.Context) error {
	defer close(s.doneCh)
	timer := time.NewTimer(s.idlePoll)
	defer timer.Stop()
	for {
		drained, err := s.drainRing()
		if err != nil {
			return err
		}
		if drained > 0 || s.pendingLen() > 0 {
			if err := s.flushOnce(ctx); err != nil {
				return err
			}
		}
		if !timer.Stop() {
			select {
			case <-timer.C:
			default:
			}
		}
		timer.Reset(s.idlePoll)
		select {
		case <-ctx.Done():
			s.finalDrain(context.Background())
			return ctx.Err()
		case <-s.wakeCh:
		case <-timer.C:
		}
	}
}

func (s *Server) drainRing() (int, error) {
	count := 0
	for {
		if s.pendingFull() {
			return count, nil
		}
		payload, ok, err := s.consumer.Pop()
		if err != nil {
			return count, fmt.Errorf("ring pop: %w", err)
		}
		if !ok {
			return count, nil
		}
		entry, err := oplog.Decode(payload)
		if err != nil {
			return count, fmt.Errorf("oplog decode: %w", err)
		}
		s.pending.put(entry)
		count++
		if count >= s.batchSize {
			return count, nil
		}
	}
}

func (s *Server) pendingFull() bool {
	records, bytes := s.pending.size()
	if s.maxPendingRecords > 0 && records >= s.maxPendingRecords {
		return true
	}
	if s.maxPendingBytes > 0 && bytes >= s.maxPendingBytes {
		return true
	}
	return false
}

func (s *Server) pendingLen() int {
	records, _ := s.pending.size()
	return records
}

func (s *Server) flushOnce(ctx context.Context) error {
	entries := s.pending.snapshot()
	if len(entries) == 0 {
		return nil
	}
	records := make([]store.EntryRecord, 0, len(entries))
	for _, e := range entries {
		records = append(records, recordFromEntry(e))
	}
	flushCtx, cancel := context.WithTimeout(ctx, s.flushTimeout)
	defer cancel()
	if err := s.backend.UpsertEntries(flushCtx, records); err != nil {
		return fmt.Errorf("flush mysql: %w", err)
	}
	for _, e := range entries {
		s.pending.drop(store.EntryRef{DMap: e.DMap, Key: e.Key, HKey: e.HKey}, e.OwnerSeq)
	}
	return nil
}

func (s *Server) finalDrain(ctx context.Context) {
	for {
		drained, err := s.drainRing()
		if err != nil {
			return
		}
		if err := s.flushOnce(ctx); err != nil {
			return
		}
		if drained == 0 {
			return
		}
	}
}

func (s *Server) wake() {
	select {
	case s.wakeCh <- struct{}{}:
	default:
	}
}

// Notify wakes the consumer loop.
func (s *Server) Notify(ctx context.Context, req *NotifyRequest) (*Empty, error) {
	s.wake()
	return &Empty{}, nil
}

// LoadFromMySQL serves the node's miss-path read.
func (s *Server) LoadFromMySQL(ctx context.Context, req *LoadRequest) (*LoadResponse, error) {
	if req == nil {
		return nil, errors.New("load request is nil")
	}
	ref := store.EntryRef{DMap: req.DMap, Key: req.Key, HKey: req.HKey}
	if e, ok := s.pending.get(ref); ok {
		rec := recordFromEntry(e)
		if rec.Tombstone || recordExpired(rec) {
			return &LoadResponse{Found: false}, nil
		}
		return &LoadResponse{Found: true, Record: rec}, nil
	}
	rec, err := s.backend.LoadFromMySQL(ctx, ref)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return &LoadResponse{Found: false}, nil
		}
		return nil, err
	}
	if rec.Tombstone || recordExpired(rec) {
		return &LoadResponse{Found: false}, nil
	}
	return &LoadResponse{Found: true, Record: rec}, nil
}

// DrainPartition flushes every pending entry for (DMap, PartitionID) before
// returning.
func (s *Server) DrainPartition(ctx context.Context, req *DrainPartitionRequest) (*Empty, error) {
	if req == nil {
		return nil, errors.New("drain request is nil")
	}
	if req.PartitionCount == 0 {
		return nil, errors.New("partition count is zero")
	}
	if req.PartitionID >= req.PartitionCount {
		return nil, fmt.Errorf("partition id %d out of range %d", req.PartitionID, req.PartitionCount)
	}
	if _, err := s.drainRing(); err != nil {
		return nil, err
	}
	subset := s.pending.drainPartition(req.DMap, req.PartitionID, req.PartitionCount)
	if len(subset) == 0 {
		return &Empty{}, nil
	}
	records := make([]store.EntryRecord, 0, len(subset))
	for _, e := range subset {
		records = append(records, recordFromEntry(e))
	}
	flushCtx, cancel := context.WithTimeout(ctx, s.flushTimeout)
	defer cancel()
	if err := s.backend.UpsertEntries(flushCtx, records); err != nil {
		return nil, fmt.Errorf("drain partition flush: %w", err)
	}
	for _, e := range subset {
		s.pending.drop(store.EntryRef{DMap: e.DMap, Key: e.Key, HKey: e.HKey}, e.OwnerSeq)
	}
	return &Empty{}, nil
}

// PurgeBelowGeneration drops pending entries below the given generation and
// asks the backend to do the same for committed rows.
func (s *Server) PurgeBelowGeneration(ctx context.Context, req *PurgeRequest) (*PurgeResponse, error) {
	if req == nil {
		return nil, errors.New("purge request is nil")
	}
	if req.MinGeneration <= 0 {
		return &PurgeResponse{Purged: 0}, nil
	}
	dropped := s.pending.purgeBelow(req.MinGeneration)
	purged, err := s.backend.PurgeBelowGeneration(ctx, req.MinGeneration)
	if err != nil {
		return nil, err
	}
	return &PurgeResponse{Purged: dropped + purged}, nil
}

// Shutdown returns once the consumer has drained pending entries to MySQL.
func (s *Server) Shutdown(ctx context.Context, _ *Empty) (*Empty, error) {
	if err := s.flushOnce(ctx); err != nil {
		return nil, err
	}
	return &Empty{}, nil
}

func recordFromEntry(e oplog.Entry) store.EntryRecord {
	return store.EntryRecord{
		DMap:         e.DMap,
		Key:          e.Key,
		HKey:         e.HKey,
		EncodedEntry: e.EncodedEntry,
		TTL:          e.TTL,
		Timestamp:    e.Timestamp,
		Tombstone:    e.Op == oplog.OpDelete,
		Generation:   e.Generation,
		Epoch:        e.Epoch,
		OwnerSeq:     e.OwnerSeq,
		WriterID:     e.WriterID,
		UpdatedAt:    time.Unix(0, e.UpdatedAtUnixNano).UTC(),
	}
}

// --- gRPC plumbing -------------------------------------------------------

type OplogControlClient interface {
	Notify(ctx context.Context, in *NotifyRequest, opts ...grpc.CallOption) (*Empty, error)
	LoadFromMySQL(ctx context.Context, in *LoadRequest, opts ...grpc.CallOption) (*LoadResponse, error)
	DrainPartition(ctx context.Context, in *DrainPartitionRequest, opts ...grpc.CallOption) (*Empty, error)
	PurgeBelowGeneration(ctx context.Context, in *PurgeRequest, opts ...grpc.CallOption) (*PurgeResponse, error)
	Shutdown(ctx context.Context, in *Empty, opts ...grpc.CallOption) (*Empty, error)
}

type OplogControlServer interface {
	Notify(context.Context, *NotifyRequest) (*Empty, error)
	LoadFromMySQL(context.Context, *LoadRequest) (*LoadResponse, error)
	DrainPartition(context.Context, *DrainPartitionRequest) (*Empty, error)
	PurgeBelowGeneration(context.Context, *PurgeRequest) (*PurgeResponse, error)
	Shutdown(context.Context, *Empty) (*Empty, error)
}

func RegisterOplogControlServer(s grpc.ServiceRegistrar, srv OplogControlServer) {
	s.RegisterService(&grpc.ServiceDesc{
		ServiceName: ServiceName,
		HandlerType: (*OplogControlServer)(nil),
		Methods: []grpc.MethodDesc{
			{MethodName: "Notify", Handler: notifyHandler},
			{MethodName: "LoadFromMySQL", Handler: loadHandler},
			{MethodName: "DrainPartition", Handler: drainHandler},
			{MethodName: "PurgeBelowGeneration", Handler: purgeHandler},
			{MethodName: "Shutdown", Handler: shutdownHandler},
		},
	}, srv)
}

func notifyHandler(srv any, ctx context.Context, dec func(any) error, _ grpc.UnaryServerInterceptor) (any, error) {
	in := new(NotifyRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	return srv.(OplogControlServer).Notify(ctx, in)
}

func loadHandler(srv any, ctx context.Context, dec func(any) error, _ grpc.UnaryServerInterceptor) (any, error) {
	in := new(LoadRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	return srv.(OplogControlServer).LoadFromMySQL(ctx, in)
}

func drainHandler(srv any, ctx context.Context, dec func(any) error, _ grpc.UnaryServerInterceptor) (any, error) {
	in := new(DrainPartitionRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	return srv.(OplogControlServer).DrainPartition(ctx, in)
}

func purgeHandler(srv any, ctx context.Context, dec func(any) error, _ grpc.UnaryServerInterceptor) (any, error) {
	in := new(PurgeRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	return srv.(OplogControlServer).PurgeBelowGeneration(ctx, in)
}

func shutdownHandler(srv any, ctx context.Context, dec func(any) error, _ grpc.UnaryServerInterceptor) (any, error) {
	in := new(Empty)
	if err := dec(in); err != nil {
		return nil, err
	}
	return srv.(OplogControlServer).Shutdown(ctx, in)
}

type oplogControlClient struct {
	cc grpc.ClientConnInterface
}

func NewOplogControlClient(cc grpc.ClientConnInterface) OplogControlClient {
	return &oplogControlClient{cc: cc}
}

func (c *oplogControlClient) Notify(ctx context.Context, in *NotifyRequest, opts ...grpc.CallOption) (*Empty, error) {
	out := new(Empty)
	if err := c.cc.Invoke(ctx, "/"+ServiceName+"/Notify", in, out, opts...); err != nil {
		return nil, err
	}
	return out, nil
}

func (c *oplogControlClient) LoadFromMySQL(ctx context.Context, in *LoadRequest, opts ...grpc.CallOption) (*LoadResponse, error) {
	out := new(LoadResponse)
	if err := c.cc.Invoke(ctx, "/"+ServiceName+"/LoadFromMySQL", in, out, opts...); err != nil {
		return nil, err
	}
	return out, nil
}

func (c *oplogControlClient) DrainPartition(ctx context.Context, in *DrainPartitionRequest, opts ...grpc.CallOption) (*Empty, error) {
	out := new(Empty)
	if err := c.cc.Invoke(ctx, "/"+ServiceName+"/DrainPartition", in, out, opts...); err != nil {
		return nil, err
	}
	return out, nil
}

func (c *oplogControlClient) PurgeBelowGeneration(ctx context.Context, in *PurgeRequest, opts ...grpc.CallOption) (*PurgeResponse, error) {
	out := new(PurgeResponse)
	if err := c.cc.Invoke(ctx, "/"+ServiceName+"/PurgeBelowGeneration", in, out, opts...); err != nil {
		return nil, err
	}
	return out, nil
}

func (c *oplogControlClient) Shutdown(ctx context.Context, in *Empty, opts ...grpc.CallOption) (*Empty, error) {
	out := new(Empty)
	if err := c.cc.Invoke(ctx, "/"+ServiceName+"/Shutdown", in, out, opts...); err != nil {
		return nil, err
	}
	return out, nil
}

var _ OplogControlServer = (*Server)(nil)
