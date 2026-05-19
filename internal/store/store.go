package store

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/cockroachdb/pebble"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

var (
	ErrNotFound  = errors.New("cache value not found")
	ErrQueueFull = errors.New("dirty queue is full")
)

type CacheStore interface {
	LoadEntry(ctx context.Context, ref EntryRef) (EntryRecord, error)
	StoreEntry(ctx context.Context, record EntryRecord) error
	DeleteEntry(ctx context.Context, ref EntryRef) error
	Close(ctx context.Context) error
}

type CommitStore interface {
	PrepareEntry(ctx context.Context, record EntryRecord) (EntryRecord, error)
	CommitEntry(ctx context.Context, ref EntryRef, walSeq int64) error
	AbortEntry(ctx context.Context, ref EntryRef, walSeq int64) error
}

type Starter interface {
	Start(ctx context.Context) error
}

type ReplayStore interface {
	Replay(ctx context.Context, f func(EntryRecord) error) error
}

type EntryRef struct {
	DMap string
	Key  string
	HKey uint64
}

type EntryRecord struct {
	DMap         string `gorm:"primaryKey;size:256;column:dmap" json:"dmap"`
	Key          string `gorm:"column:key;size:512;not null;index:idx_olric_entry_lookup" json:"key"`
	HKey         uint64 `gorm:"primaryKey;column:hkey" json:"hkey"`
	EncodedEntry []byte `gorm:"column:encoded_entry;type:longblob" json:"encoded_entry"`
	TTL          int64  `gorm:"column:ttl;not null" json:"ttl"`
	Timestamp    int64  `gorm:"column:timestamp;not null" json:"timestamp"`
	Tombstone    bool   `gorm:"column:tombstone;not null" json:"tombstone"`
	Generation   int64  `gorm:"column:generation;not null;index:idx_cache_fence" json:"generation"`
	Epoch        int64  `gorm:"column:epoch;not null;index:idx_cache_fence" json:"epoch"`
	OwnerSeq     int64  `gorm:"column:owner_seq;not null;index:idx_cache_fence" json:"owner_seq"`
	// WriterID identifies the owner node that produced the record. It is the
	// final tiebreaker in versionedUpsertClause when (Generation, Epoch,
	// OwnerSeq) compare equal — a corner case that cannot arise under the
	// current cluster model (each (G, E) has at most one owner per
	// partition) but a future epoch ping-pong or a race in the balancer
	// could allow two nodes to stamp the same triple. Comparing writer_id
	// lexicographically gives a deterministic, cluster-stable winner
	// instead of last-MySQL-batch-wins. See §8.5.
	WriterID  string    `gorm:"column:writer_id;size:128;not null;default:''" json:"writer_id"`
	UpdatedAt time.Time `gorm:"column:updated_at" json:"updated_at"`
	// WALSeq is a node-local monotonic identifier assigned at PrepareEntry. It
	// is never persisted to MySQL — the durable contract is the fence triple
	// (Generation, Epoch, OwnerSeq). WALSeq exists only to bind a Commit/Abort
	// call back to the exact prepared record in the local WAL.
	WALSeq     int64  `gorm:"-" json:"wal_seq,omitempty"`
	Origin     string `gorm:"-" json:"origin,omitempty"`
	FlushMySQL bool   `gorm:"-" json:"flush_mysql,omitempty"`
	WALState   string `gorm:"-" json:"wal_state,omitempty"`
}

func (r EntryRecord) Fence() (generation, epoch, ownerSeq int64) {
	return r.Generation, r.Epoch, r.OwnerSeq
}

func fenceLess(a, b EntryRecord) bool {
	if a.Generation != b.Generation {
		return a.Generation < b.Generation
	}
	if a.Epoch != b.Epoch {
		return a.Epoch < b.Epoch
	}
	return a.OwnerSeq < b.OwnerSeq
}

const (
	WALStatePrepared  = "prepared"
	WALStateCommitted = "committed"
	WALStateLocal     = "local_refill"
)

func (EntryRecord) TableName() string {
	return "olric_cache_records"
}

func (r EntryRecord) Ref() EntryRef {
	return EntryRef{DMap: r.DMap, Key: r.Key, HKey: r.HKey}
}

func (r EntryRecord) Clone() EntryRecord {
	r.EncodedEntry = append([]byte(nil), r.EncodedEntry...)
	return r
}

type Config struct {
	QueueSize     int
	FlushInterval time.Duration
	BatchSize     int
	WALPath       string
	FlushBackoff  time.Duration
	// FlushTimeout bounds a single MySQL upsert batch. Without it, a hung
	// connection would freeze flushLoop indefinitely while new writes pile up
	// in the dirty queue until QueueSize triggers ErrQueueFull. Defaults to
	// 30s — long enough for normal slow paths, short enough to surface a
	// truly stuck flusher within one operator alert window.
	FlushTimeout time.Duration
	// PebbleEventListener receives Pebble lifecycle events (write stalls,
	// compactions, disk-slow). Optional; nil disables structured event
	// observability and lets pebble use its built-in logger.
	PebbleEventListener *pebble.EventListener
}

func (c Config) withDefaults() Config {
	if c.QueueSize <= 0 {
		c.QueueSize = 1024
	}
	if c.FlushInterval <= 0 {
		c.FlushInterval = time.Second
	}
	if c.BatchSize <= 0 {
		c.BatchSize = 256
	}
	if c.WALPath == "" {
		c.WALPath = filepath.Join(os.TempDir(), "olricstack-cache.wal")
	}
	if c.FlushBackoff <= 0 {
		c.FlushBackoff = c.FlushInterval
	}
	if c.FlushTimeout <= 0 {
		c.FlushTimeout = 30 * time.Second
	}
	return c
}

type dirtyEntry struct {
	Record EntryRecord `json:"record"`
}

type MySQLStore struct {
	db     *gorm.DB
	cfg    Config
	wal    *pebbleWAL
	notify chan struct{}
	done   chan struct{}
	closed chan struct{}

	mu          sync.Mutex
	closeOnce   sync.Once
	startOnce   sync.Once
	closing     bool
	started     bool
	nextFlushAt time.Time
	workerErrMu sync.Mutex
	workerErr   error
}

var _ CacheStore = (*MySQLStore)(nil)

func NewMySQLStore(db *gorm.DB, cfg Config) (*MySQLStore, error) {
	if db == nil {
		return nil, errors.New("gorm db is nil")
	}

	return newStore(db, cfg)
}

func NewWALStore(cfg Config) (*MySQLStore, error) {
	return newStore(nil, cfg)
}

func newStore(db *gorm.DB, cfg Config) (*MySQLStore, error) {
	cfg = cfg.withDefaults()
	if db != nil {
		if err := db.AutoMigrate(&EntryRecord{}); err != nil {
			return nil, fmt.Errorf("migrate cache table: %w", err)
		}
	}

	s := &MySQLStore{
		db:     db,
		cfg:    cfg,
		notify: make(chan struct{}, 1),
		done:   make(chan struct{}),
		closed: make(chan struct{}),
	}
	wal, err := openPebbleWAL(cfg.WALPath, cfg.QueueSize, cfg.PebbleEventListener)
	if err != nil {
		return nil, err
	}
	s.wal = wal
	return s, nil
}

func (s *MySQLStore) Start(context.Context) error {
	if s.db == nil {
		return nil
	}
	s.startOnce.Do(func() {
		swept, err := s.sweepPreparedOrphans()
		if err != nil {
			log.Printf("wal prepared orphan sweep failed: %v", err)
		} else if swept > 0 {
			log.Printf("wal prepared orphan sweep removed %d records", swept)
		}
		s.mu.Lock()
		s.started = true
		s.mu.Unlock()
		s.notifyFlush()
		go s.flushLoop()
	})
	return nil
}

// sweepPreparedOrphans deletes WAL records left in WALStatePrepared by a
// previous process. A Prepared record can survive across restart only if the
// owning process crashed between PrepareEntry and Commit/Abort, OR if a
// VerifyAfterLock-fail / pre-condition-fail path executed `_ = hook.AfterX`
// and the abort's WAL write itself failed. Either way, no in-flight goroutine
// of the *current* process can finalize them — they cannot reach MySQL
// (isFlushable filters Prepared) and they cannot be reused (the next write to
// the same key overwrites them via appendDirty), so dropping them at boot is
// safe and reclaims WAL space.
//
// Sweep runs before flushLoop starts and before any caller can issue a new
// PrepareEntry, so there is no race with live writers.
func (s *MySQLStore) sweepPreparedOrphans() (int, error) {
	swept, err := s.wal.SweepPrepared()
	if err != nil {
		return 0, fmt.Errorf("sweep prepared wal records: %w", err)
	}
	return swept, nil
}

// PreparedCount reports the number of WAL records currently in
// WALStatePrepared. Operators can scrape it as a backpressure / orphan
// indicator: a steady non-zero value past one flushInterval suggests stuck
// preparations (process crashed mid-mutation, or Olric mutation failed but
// AbortEntry's WAL write also failed). Healthy steady-state is 0; transient
// spikes during high-throughput writes are normal.
func (s *MySQLStore) PreparedCount() (int, error) {
	count, err := s.wal.PreparedCount()
	if err != nil {
		return 0, fmt.Errorf("count prepared wal records: %w", err)
	}
	return count, nil
}

// PebbleMetrics returns a point-in-time snapshot of Pebble's internal metrics
// (LSM levels, write stalls, compaction stats, disk space, etc.). Intended
// for Prometheus exporters or ad-hoc operator inspection. Returns nil if the
// underlying WAL has been closed.
func (s *MySQLStore) PebbleMetrics() *pebble.Metrics {
	return s.wal.Metrics()
}

// PurgeBelowGeneration removes WAL records whose fence Generation is strictly
// less than minGeneration. It exists for the case where a node restarts under
// a higher Watchdog generation than the one its committed records were stamped
// with: those records can no longer win the MySQL upsert (the fence triple
// would lose) and would otherwise just produce noise as flushable IO. This is
// the engineering counterpart to Formal Target #6 — old owners cannot
// overwrite newer state — by deleting the IO before it ever reaches MySQL.
//
// Records currently in WALStatePrepared are left untouched: the active
// Olric mutation may still call AfterSet/Commit. Tombstones (Generation>0)
// follow the same purge rule as live records.
func (s *MySQLStore) PurgeBelowGeneration(ctx context.Context, minGeneration int64) (int, error) {
	if minGeneration <= 0 {
		return 0, nil
	}
	s.mu.Lock()
	if s.closing {
		s.mu.Unlock()
		return 0, errors.New("cache store is closed")
	}
	s.mu.Unlock()

	if err := ctx.Err(); err != nil {
		return 0, err
	}

	purged, err := s.wal.PurgeBelowGeneration(minGeneration)
	if err != nil {
		return 0, fmt.Errorf("purge below generation %d: %w", minGeneration, err)
	}
	return purged, nil
}

func (s *MySQLStore) Replay(ctx context.Context, f func(EntryRecord) error) error {
	if err := s.wal.Replay(ctx, f); err != nil {
		return fmt.Errorf("replay wal: %w", err)
	}
	return nil
}

// FlushHandoff synchronously flushes every committed WAL record matching
// (dmap, hkey) in refs to the terminal store (MySQL) and deletes the
// corresponding WAL entries. It is the engineering counterpart to Formal
// Safety Target #5 — ownership transfer must not complete until the new
// owner has an equivalent durable source — by emptying the old owner's
// dirty WAL for the migrating fragment before the fork hands off.
//
// Behavior:
//
//   - Records in WALStatePrepared are left alone (the matching mutation is
//     still in-flight or already abort-pending). They cannot reach MySQL
//     and the new owner will never observe them; they are either GC'd by
//     the next write to the same key or by the boot sweep on restart.
//   - Records not currently in the dirty bucket are ignored (already
//     flushed, never written, or aborted).
//   - If MySQL is unreachable, FlushHandoff returns the underlying error so
//     the caller (DurableHook.DrainForHandoff) can abort the migration.
//   - When the configured store has no MySQL backend (db==nil, i.e. the
//     WAL-only test profile) the WAL entries are removed without an upsert
//     so callers can still exercise the handoff path deterministically.
//
// Safety: callers MUST hold the per-fragment write lock for every ref in
// the set so no concurrent BeforeX/AfterX runs against these keys. The
// fork's fragment.Move satisfies this by calling DrainForHandoff inside
// f.Lock().
func (s *MySQLStore) FlushHandoff(ctx context.Context, refs []EntryRef) error {
	if len(refs) == 0 {
		return nil
	}
	if err := s.ensureOpen(ctx); err != nil {
		return err
	}
	entries, err := s.wal.ScanFlushableRefs(refs)
	if err != nil {
		return fmt.Errorf("collect handoff records: %w", err)
	}
	return s.flushHandoffEntries(entries)
}

// FlushHandoffPartition synchronously flushes every committed WAL record for
// dmap whose hkey belongs to partitionID under partitionCount. This is the
// production handoff path: scanning the WAL by partition covers records that
// are no longer resident in the in-memory fragment, such as committed
// tombstones or evicted-but-dirty writes.
func (s *MySQLStore) FlushHandoffPartition(ctx context.Context, dmap string, partitionID, partitionCount uint64) error {
	if dmap == "" {
		return errors.New("handoff dmap is empty")
	}
	if partitionCount == 0 {
		return errors.New("handoff partition count is zero")
	}
	if partitionID >= partitionCount {
		return fmt.Errorf("handoff partition id %d out of range %d", partitionID, partitionCount)
	}
	if err := s.ensureOpen(ctx); err != nil {
		return err
	}
	entries, err := s.wal.ScanFlushablePartition(dmap, partitionID, partitionCount)
	if err != nil {
		return fmt.Errorf("collect handoff partition records: %w", err)
	}
	return s.flushHandoffEntries(entries)
}

func (s *MySQLStore) ensureOpen(ctx context.Context) error {
	s.mu.Lock()
	if s.closing {
		s.mu.Unlock()
		return errors.New("cache store is closed")
	}
	s.mu.Unlock()

	if err := ctx.Err(); err != nil {
		return err
	}
	return nil
}

func (s *MySQLStore) flushHandoffEntries(entries []dirtyEntry) error {
	if len(entries) == 0 {
		return nil
	}

	if !s.flushEntries(entries) {
		return fmt.Errorf("flush handoff records: %w", s.workerError())
	}
	if err := s.deleteFlushed(entries); err != nil {
		return fmt.Errorf("delete flushed handoff records: %w", err)
	}
	return nil
}

func (s *MySQLStore) LoadEntry(ctx context.Context, ref EntryRef) (EntryRecord, error) {
	if record, ok, err := s.loadDirty(ref); err != nil {
		return EntryRecord{}, err
	} else if ok {
		if record.WALState == WALStatePrepared {
			return EntryRecord{}, ErrNotFound
		}
		if record.Tombstone {
			return EntryRecord{}, ErrNotFound
		}
		if isExpired(record.TTL, time.Now()) {
			_ = s.DeleteEntry(context.Background(), record.Ref())
			return EntryRecord{}, ErrNotFound
		}
		return record, nil
	}

	if s.db == nil {
		return EntryRecord{}, ErrNotFound
	}

	var rec EntryRecord
	if err := s.db.WithContext(ctx).First(&rec, "dmap = ? AND hkey = ?", ref.DMap, ref.HKey).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return EntryRecord{}, ErrNotFound
		}
		return EntryRecord{}, fmt.Errorf("load entry %s: %w", walKey(ref), err)
	}
	if rec.Tombstone {
		return EntryRecord{}, ErrNotFound
	}
	if isExpired(rec.TTL, time.Now()) {
		_ = s.DeleteEntry(context.Background(), rec.Ref())
		return EntryRecord{}, ErrNotFound
	}

	return rec.Clone(), nil
}

func (s *MySQLStore) StoreEntry(ctx context.Context, record EntryRecord) error {
	record.Tombstone = false
	if record.WALState == "" {
		if record.FlushMySQL {
			record.WALState = WALStateCommitted
		} else {
			record.WALState = WALStateLocal
		}
	}
	return s.store(ctx, record)
}

func (s *MySQLStore) DeleteEntry(ctx context.Context, ref EntryRef) error {
	return s.store(ctx, EntryRecord{
		DMap:       ref.DMap,
		Key:        ref.Key,
		HKey:       ref.HKey,
		Tombstone:  true,
		UpdatedAt:  time.Now().UTC(),
		Origin:     "client_delete",
		FlushMySQL: true,
		WALState:   WALStateCommitted,
	})
}

// PrepareEntry stages a record in the WAL with WALStatePrepared. The caller
// controls record.Tombstone (true for delete intent). The record only becomes
// flushable to MySQL after a successful CommitEntry; an AbortEntry removes it.
func (s *MySQLStore) PrepareEntry(ctx context.Context, record EntryRecord) (EntryRecord, error) {
	record.FlushMySQL = false
	record.WALState = WALStatePrepared
	return s.storeAndReturn(ctx, record)
}

func (s *MySQLStore) CommitEntry(ctx context.Context, ref EntryRef, walSeq int64) error {
	s.mu.Lock()
	if s.closing {
		s.mu.Unlock()
		return errors.New("cache store is closed")
	}
	if err := ctx.Err(); err != nil {
		s.mu.Unlock()
		return err
	}
	depth, err := s.commitDirty(ref, walSeq)
	if err != nil {
		s.mu.Unlock()
		return err
	}
	s.mu.Unlock()

	if depth >= s.cfg.BatchSize {
		s.notifyFlush()
	}
	return nil
}

// AbortEntry deletes a prepared WAL record after the corresponding Olric
// mutation failed. It is intentionally narrow:
//
//   - If no record exists for ref, it is a no-op (idempotent).
//   - If the record's WALSeq does not match, it was superseded by a newer
//     prepare/commit and must not be touched.
//   - If the record was already committed, abort is rejected — committed state
//     is the durable contract and only flush+deleteFlushed may remove it.
//
// This makes AbortEntry safe to call from a DurableHook error path even if the
// hook does not know whether the prepare itself reached the WAL.
func (s *MySQLStore) AbortEntry(ctx context.Context, ref EntryRef, walSeq int64) error {
	s.mu.Lock()
	if s.closing {
		s.mu.Unlock()
		return errors.New("cache store is closed")
	}
	if err := ctx.Err(); err != nil {
		s.mu.Unlock()
		return err
	}
	defer s.mu.Unlock()
	return s.abortDirty(ref, walSeq)
}

func (s *MySQLStore) store(ctx context.Context, record EntryRecord) error {
	_, err := s.storeAndReturn(ctx, record)
	return err
}

func (s *MySQLStore) storeAndReturn(ctx context.Context, record EntryRecord) (EntryRecord, error) {
	s.mu.Lock()
	if s.closing {
		s.mu.Unlock()
		return EntryRecord{}, errors.New("cache store is closed")
	}
	if err := ctx.Err(); err != nil {
		s.mu.Unlock()
		return EntryRecord{}, err
	}
	entry, depth, err := s.appendDirty(record)
	if err != nil {
		s.mu.Unlock()
		return EntryRecord{}, err
	}
	s.mu.Unlock()

	if depth >= s.cfg.BatchSize {
		s.notifyFlush()
	}
	return entry.Record.Clone(), nil
}

func (s *MySQLStore) Close(ctx context.Context) error {
	var started bool
	s.closeOnce.Do(func() {
		s.mu.Lock()
		s.closing = true
		started = s.started
		close(s.done)
		s.mu.Unlock()
	})

	if !started {
		return s.wal.Close()
	}

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-s.closed:
		err := s.workerError()
		if closeErr := s.wal.Close(); closeErr != nil && err == nil {
			err = closeErr
		}
		return err
	}
}

func (s *MySQLStore) flushLoop() {
	defer close(s.closed)

	ticker := time.NewTicker(s.cfg.FlushInterval)
	defer ticker.Stop()

	for {
		select {
		case _, ok := <-s.notify:
			if !ok {
				s.flushWAL(true)
				return
			}
			s.flushWAL(false)
		case <-ticker.C:
			s.flushWAL(false)
		case <-s.done:
			s.flushWAL(true)
			return
		}
	}
}

func (s *MySQLStore) flushWAL(force bool) bool {
	if !force && !s.flushReady(time.Now()) {
		return false
	}
	entries, err := s.loadFlushableBatch(s.cfg.BatchSize)
	if err != nil {
		s.setWorkerError(fmt.Errorf("load dirty wal: %w", err))
		return false
	}
	if len(entries) == 0 {
		s.setWorkerError(nil)
		return true
	}
	if !s.flushEntries(entries) {
		s.deferFlush()
		return false
	}
	if err := s.deleteFlushed(entries); err != nil {
		s.setWorkerError(fmt.Errorf("delete flushed wal entries: %w", err))
		return false
	}
	if len(entries) == s.cfg.BatchSize {
		s.notifyFlush()
	}
	return true
}

func (s *MySQLStore) flushReady(now time.Time) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.nextFlushAt.IsZero() || !now.Before(s.nextFlushAt)
}

func (s *MySQLStore) deferFlush() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.nextFlushAt = time.Now().Add(s.cfg.FlushBackoff)
}

func (s *MySQLStore) flushEntries(entries []dirtyEntry) bool {
	if s.db == nil {
		s.setWorkerError(nil)
		return true
	}
	records := make([]EntryRecord, 0, len(entries))
	now := time.Now().UTC()
	for _, entry := range entries {
		if !isFlushable(entry.Record) {
			continue
		}
		record := entry.Record.Clone()
		record.UpdatedAt = now
		records = append(records, record)
	}
	if len(records) == 0 {
		s.setWorkerError(nil)
		return true
	}

	ctx, cancel := context.WithTimeout(context.Background(), s.cfg.FlushTimeout)
	defer cancel()
	err := s.db.WithContext(ctx).Clauses(s.versionedUpsertClause()).CreateInBatches(records, s.cfg.BatchSize).Error
	if err != nil {
		s.setWorkerError(fmt.Errorf("flush dirty records: %w", err))
		return false
	}
	s.setWorkerError(nil)
	s.mu.Lock()
	s.nextFlushAt = time.Time{}
	s.mu.Unlock()
	return true
}

func (s *MySQLStore) appendDirty(record EntryRecord) (dirtyEntry, int, error) {
	return s.wal.Prepare(record)
}

func (s *MySQLStore) commitDirty(ref EntryRef, walSeq int64) (int, error) {
	return s.wal.Commit(ref, walSeq)
}

func (s *MySQLStore) abortDirty(ref EntryRef, walSeq int64) error {
	return s.wal.Abort(ref, walSeq)
}

func (s *MySQLStore) loadDirty(ref EntryRef) (EntryRecord, bool, error) {
	record, ok, err := s.wal.Load(ref)
	if err != nil {
		return EntryRecord{}, false, fmt.Errorf("load dirty wal entry %s: %w", walKey(ref), err)
	}
	return record, ok, nil
}

func (s *MySQLStore) loadDirtyBatch(limit int) ([]dirtyEntry, error) {
	return s.wal.Scan(limit)
}

func (s *MySQLStore) loadFlushableBatch(limit int) ([]dirtyEntry, error) {
	return s.wal.ScanFlushable(limit)
}

func (s *MySQLStore) deleteFlushed(entries []dirtyEntry) error {
	return s.wal.DeleteIfSeq(entries)
}

// versionedUpsertClause encodes the durable conflict-resolution rule used by
// the asynchronous flusher. Conflict resolution is a strict lexicographic
// comparison of the fence tuple (Generation, Epoch, OwnerSeq, WriterID):
//
//	v1 < v2 ⟺  G1<G2
//	         ∨ (G1=G2 ∧ E1<E2)
//	         ∨ (G1=G2 ∧ E1=E2 ∧ S1<S2)
//	         ∨ (G1=G2 ∧ E1=E2 ∧ S1=S2 ∧ W1<W2)   ← writer_id tiebreaker
//
// This is what makes a delayed flush from an old owner provably lose to a
// newer-owner write — local wall-clock time cannot defeat a higher fence.
// isFlushable() rejects records without G>0 ∧ S>0, so by construction every
// row reaching this clause carries a fence.
//
// The writer_id tiebreaker closes §8.5: under the current cluster model
// each (G, E) has at most one owner per partition, so the lex comparison
// on (G, E, S) is already total. But a future balancer race or epoch
// ping-pong could let two nodes stamp the same triple. Without a
// tiebreaker the upsert would fall through to "keep existing", giving
// last-MySQL-batch-wins for equal fences — a non-deterministic outcome.
// The lex extension over writer_id gives a cluster-stable deterministic
// winner instead.
func (s *MySQLStore) versionedUpsertClause() clause.OnConflict {
	if s.db.Dialector.Name() == "mysql" {
		newer := "VALUES(generation) > generation OR " +
			"(VALUES(generation) = generation AND VALUES(epoch) > epoch) OR " +
			"(VALUES(generation) = generation AND VALUES(epoch) = epoch AND VALUES(owner_seq) > owner_seq) OR " +
			"(VALUES(generation) = generation AND VALUES(epoch) = epoch AND VALUES(owner_seq) = owner_seq AND VALUES(writer_id) > writer_id)"
		return clause.OnConflict{
			Columns: []clause.Column{{Name: "dmap"}, {Name: "hkey"}},
			DoUpdates: clause.Assignments(map[string]interface{}{
				"key":           gorm.Expr("CASE WHEN " + newer + " THEN VALUES(`key`) ELSE `key` END"),
				"encoded_entry": gorm.Expr("CASE WHEN " + newer + " THEN VALUES(encoded_entry) ELSE encoded_entry END"),
				"ttl":           gorm.Expr("CASE WHEN " + newer + " THEN VALUES(ttl) ELSE ttl END"),
				"timestamp":     gorm.Expr("CASE WHEN " + newer + " THEN VALUES(timestamp) ELSE timestamp END"),
				"tombstone":     gorm.Expr("CASE WHEN " + newer + " THEN VALUES(tombstone) ELSE tombstone END"),
				"generation":    gorm.Expr("CASE WHEN " + newer + " THEN VALUES(generation) ELSE generation END"),
				"epoch":         gorm.Expr("CASE WHEN " + newer + " THEN VALUES(epoch) ELSE epoch END"),
				"owner_seq":     gorm.Expr("CASE WHEN " + newer + " THEN VALUES(owner_seq) ELSE owner_seq END"),
				"writer_id":     gorm.Expr("CASE WHEN " + newer + " THEN VALUES(writer_id) ELSE writer_id END"),
				"updated_at":    gorm.Expr("CASE WHEN " + newer + " THEN VALUES(updated_at) ELSE updated_at END"),
			}),
		}
	}

	newer := "excluded.generation > generation OR " +
		"(excluded.generation = generation AND excluded.epoch > epoch) OR " +
		"(excluded.generation = generation AND excluded.epoch = epoch AND excluded.owner_seq > owner_seq) OR " +
		"(excluded.generation = generation AND excluded.epoch = epoch AND excluded.owner_seq = owner_seq AND excluded.writer_id > writer_id)"
	return clause.OnConflict{
		Columns: []clause.Column{{Name: "dmap"}, {Name: "hkey"}},
		DoUpdates: clause.Assignments(map[string]interface{}{
			"key":           gorm.Expr("CASE WHEN " + newer + " THEN excluded.key ELSE key END"),
			"encoded_entry": gorm.Expr("CASE WHEN " + newer + " THEN excluded.encoded_entry ELSE encoded_entry END"),
			"ttl":           gorm.Expr("CASE WHEN " + newer + " THEN excluded.ttl ELSE ttl END"),
			"timestamp":     gorm.Expr("CASE WHEN " + newer + " THEN excluded.timestamp ELSE timestamp END"),
			"tombstone":     gorm.Expr("CASE WHEN " + newer + " THEN excluded.tombstone ELSE tombstone END"),
			"generation":    gorm.Expr("CASE WHEN " + newer + " THEN excluded.generation ELSE generation END"),
			"epoch":         gorm.Expr("CASE WHEN " + newer + " THEN excluded.epoch ELSE epoch END"),
			"owner_seq":     gorm.Expr("CASE WHEN " + newer + " THEN excluded.owner_seq ELSE owner_seq END"),
			"writer_id":     gorm.Expr("CASE WHEN " + newer + " THEN excluded.writer_id ELSE writer_id END"),
			"updated_at":    gorm.Expr("CASE WHEN " + newer + " THEN excluded.updated_at ELSE updated_at END"),
		}),
	}
}

func isExpired(ttl int64, now time.Time) bool {
	return ttl > 0 && now.UnixMilli() >= ttl
}

func isFlushable(record EntryRecord) bool {
	if !record.FlushMySQL {
		return false
	}
	if record.WALState != "" && record.WALState != WALStateCommitted {
		return false
	}
	// Fence (Generation, Epoch, OwnerSeq) is the durable contract for any
	// MySQL upsert. Records without a fence cannot win the upsert race
	// against fenced records, so we drop them at the flusher boundary
	// instead of letting them produce non-comparable rows.
	return record.Generation > 0 && record.OwnerSeq > 0
}

func shouldPreserveDirtyRecord(current, incoming EntryRecord) bool {
	return isLocalRefill(incoming) && (current.FlushMySQL || current.WALState == WALStatePrepared)
}

func isLocalRefill(record EntryRecord) bool {
	return record.WALState == WALStateLocal || (!record.FlushMySQL && record.Origin == "mysql_refill")
}

func (s *MySQLStore) notifyFlush() {
	select {
	case <-s.done:
	case s.notify <- struct{}{}:
	default:
	}
}

func (s *MySQLStore) setWorkerError(err error) {
	s.workerErrMu.Lock()
	defer s.workerErrMu.Unlock()
	s.workerErr = err
}

func (s *MySQLStore) workerError() error {
	s.workerErrMu.Lock()
	defer s.workerErrMu.Unlock()
	return s.workerErr
}
