package store

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"time"

	bolt "go.etcd.io/bbolt"
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
	DMap         string    `gorm:"primaryKey;size:256;column:dmap" json:"dmap"`
	Key          string    `gorm:"column:key;size:512;not null;index:idx_olric_entry_lookup" json:"key"`
	HKey         uint64    `gorm:"primaryKey;column:hkey" json:"hkey"`
	EncodedEntry []byte    `gorm:"column:encoded_entry;type:longblob" json:"encoded_entry"`
	TTL          int64     `gorm:"column:ttl;not null" json:"ttl"`
	Timestamp    int64     `gorm:"column:timestamp;not null" json:"timestamp"`
	Tombstone    bool      `gorm:"column:tombstone;not null" json:"tombstone"`
	Generation   int64     `gorm:"column:generation;not null;index:idx_cache_fence" json:"generation"`
	Epoch        int64     `gorm:"column:epoch;not null;index:idx_cache_fence" json:"epoch"`
	OwnerSeq     int64     `gorm:"column:owner_seq;not null;index:idx_cache_fence" json:"owner_seq"`
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
	// call back to the exact prepared record in bbolt.
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
	return c
}

type dirtyEntry struct {
	Record EntryRecord `json:"record"`
}

var (
	walDirtyBucket = []byte("dirty")
	walMetaBucket  = []byte("meta")
	walSeqKey      = []byte("wal_seq")
)

type MySQLStore struct {
	db     *gorm.DB
	cfg    Config
	wal    *bolt.DB
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
	wal, err := openWAL(cfg.WALPath)
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
// and the abort's bbolt write itself failed. Either way, no in-flight goroutine
// of the *current* process can finalize them — they cannot reach MySQL
// (isFlushable filters Prepared) and they cannot be reused (the next write to
// the same key overwrites them via appendDirty), so dropping them at boot is
// safe and reclaims bbolt space.
//
// Sweep runs before flushLoop starts and before any caller can issue a new
// PrepareEntry, so there is no race with live writers.
func (s *MySQLStore) sweepPreparedOrphans() (int, error) {
	var swept int
	err := s.wal.Update(func(tx *bolt.Tx) error {
		dirty := tx.Bucket(walDirtyBucket)
		if dirty == nil {
			return nil
		}
		var stale [][]byte
		cursor := dirty.Cursor()
		for key, encoded := cursor.First(); key != nil; key, encoded = cursor.Next() {
			var entry dirtyEntry
			if err := json.Unmarshal(encoded, &entry); err != nil {
				return err
			}
			if entry.Record.WALState == WALStatePrepared {
				stale = append(stale, append([]byte(nil), key...))
			}
		}
		for _, key := range stale {
			if err := dirty.Delete(key); err != nil {
				return err
			}
		}
		swept = len(stale)
		return nil
	})
	if err != nil {
		return 0, fmt.Errorf("sweep prepared wal records: %w", err)
	}
	return swept, nil
}

// PreparedCount reports the number of WAL records currently in
// WALStatePrepared. Operators can scrape it as a backpressure / orphan
// indicator: a steady non-zero value past one flushInterval suggests stuck
// preparations (process crashed mid-mutation, or Olric mutation failed but
// AbortEntry's bbolt write also failed). Healthy steady-state is 0; transient
// spikes during high-throughput writes are normal.
func (s *MySQLStore) PreparedCount() (int, error) {
	var count int
	err := s.wal.View(func(tx *bolt.Tx) error {
		dirty := tx.Bucket(walDirtyBucket)
		if dirty == nil {
			return nil
		}
		cursor := dirty.Cursor()
		for key, encoded := cursor.First(); key != nil; key, encoded = cursor.Next() {
			var entry dirtyEntry
			if err := json.Unmarshal(encoded, &entry); err != nil {
				return err
			}
			if entry.Record.WALState == WALStatePrepared {
				count++
			}
		}
		return nil
	})
	if err != nil {
		return 0, fmt.Errorf("count prepared wal records: %w", err)
	}
	return count, nil
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

	var purged int
	err := s.wal.Update(func(tx *bolt.Tx) error {
		dirty := tx.Bucket(walDirtyBucket)
		cursor := dirty.Cursor()
		var stale [][]byte
		for key, encoded := cursor.First(); key != nil; key, encoded = cursor.Next() {
			var entry dirtyEntry
			if err := json.Unmarshal(encoded, &entry); err != nil {
				return err
			}
			if entry.Record.WALState == WALStatePrepared {
				continue
			}
			if entry.Record.Generation > 0 && entry.Record.Generation < minGeneration {
				stale = append(stale, append([]byte(nil), key...))
			}
		}
		for _, key := range stale {
			if err := dirty.Delete(key); err != nil {
				return err
			}
		}
		purged = len(stale)
		return nil
	})
	if err != nil {
		return 0, fmt.Errorf("purge below generation %d: %w", minGeneration, err)
	}
	return purged, nil
}

func (s *MySQLStore) Replay(ctx context.Context, f func(EntryRecord) error) error {
	expired := make([]EntryRef, 0)
	err := s.wal.View(func(tx *bolt.Tx) error {
		cursor := tx.Bucket(walDirtyBucket).Cursor()
		for _, encoded := cursor.First(); encoded != nil; _, encoded = cursor.Next() {
			if err := ctx.Err(); err != nil {
				return err
			}
			var entry dirtyEntry
			if err := json.Unmarshal(encoded, &entry); err != nil {
				return err
			}
			record := entry.Record.Clone()
			if record.WALState == WALStatePrepared {
				continue
			}
			if record.Tombstone {
				continue
			}
			if isExpired(record.TTL, time.Now()) {
				expired = append(expired, record.Ref())
				continue
			}
			if err := f(record); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("replay wal: %w", err)
	}
	for _, ref := range expired {
		if err := s.DeleteEntry(ctx, ref); err != nil {
			return err
		}
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
	s.mu.Lock()
	if s.closing {
		s.mu.Unlock()
		return errors.New("cache store is closed")
	}
	s.mu.Unlock()

	if err := ctx.Err(); err != nil {
		return err
	}

	entries := make([]dirtyEntry, 0, len(refs))
	err := s.wal.View(func(tx *bolt.Tx) error {
		dirty := tx.Bucket(walDirtyBucket)
		if dirty == nil {
			return nil
		}
		for _, ref := range refs {
			encoded := dirty.Get([]byte(walKey(ref)))
			if encoded == nil {
				continue
			}
			var entry dirtyEntry
			if err := json.Unmarshal(encoded, &entry); err != nil {
				return err
			}
			if !isFlushable(entry.Record) {
				continue
			}
			entries = append(entries, entry)
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("collect handoff records: %w", err)
	}
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

	err := s.db.Clauses(s.versionedUpsertClause()).CreateInBatches(records, s.cfg.BatchSize).Error
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

func openWAL(path string) (*bolt.DB, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, fmt.Errorf("create wal dir: %w", err)
	}
	db, err := bolt.Open(path, 0o600, &bolt.Options{Timeout: time.Second})
	if err != nil {
		return nil, fmt.Errorf("open wal: %w", err)
	}
	// MaxBatchDelay caps how long the first caller of a Batch waits before
	// bbolt commits the merged tx. The default of 10ms shows up directly in
	// client write latency because PrepareEntry/CommitEntry are on the hot
	// path. 1ms still lets a few concurrent fsyncs collapse without making
	// single-writer latency worse than db.Update.
	db.MaxBatchDelay = time.Millisecond
	if err := db.Update(func(tx *bolt.Tx) error {
		if _, err := tx.CreateBucketIfNotExists(walDirtyBucket); err != nil {
			return err
		}
		_, err := tx.CreateBucketIfNotExists(walMetaBucket)
		return err
	}); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("init wal: %w", err)
	}
	return db, nil
}

// appendDirty stages a record into the WAL. It uses bbolt.Batch so concurrent
// callers across goroutines can collapse their fsyncs into a single disk flush.
//
// Batch idempotency: the closure may run more than once if a sibling call in
// the same batch returns an error. All side effects either come from tx state
// (nextWALSeq, dirty.Get, dirty.Stats), are pure functions of the input
// (record fields), or are written to outer variables that bbolt overwrites
// atomically on the winning attempt (entry.Record.WALSeq, entry.Record, depth).
// The bbolt contract guarantees only the final retry's mutations are visible
// to the caller.
func (s *MySQLStore) appendDirty(record EntryRecord) (dirtyEntry, int, error) {
	now := time.Now().UTC()
	record = record.Clone()
	if record.UpdatedAt.IsZero() {
		record.UpdatedAt = now
	}
	entry := dirtyEntry{Record: record}
	var depth int
	err := s.wal.Batch(func(tx *bolt.Tx) error {
		// Reset per-attempt mutable state so a retry sees a clean slate.
		entry = dirtyEntry{Record: record.Clone()}
		dirty := tx.Bucket(walDirtyBucket)
		key := []byte(walKey(record.Ref()))
		if existing := dirty.Get(key); existing != nil {
			var current dirtyEntry
			if err := json.Unmarshal(existing, &current); err != nil {
				return err
			}
			if shouldPreserveDirtyRecord(current.Record, entry.Record) {
				entry.Record = current.Record.Clone()
				depth = dirty.Stats().KeyN
				return nil
			}
		} else if dirty.Stats().KeyN >= s.cfg.QueueSize {
			return ErrQueueFull
		}
		meta := tx.Bucket(walMetaBucket)
		entry.Record.WALSeq = nextWALSeq(meta)
		encoded, err := json.Marshal(entry)
		if err != nil {
			return err
		}
		if err := dirty.Put(key, encoded); err != nil {
			return err
		}
		depth = dirty.Stats().KeyN
		return nil
	})
	return entry, depth, err
}

func (s *MySQLStore) commitDirty(ref EntryRef, walSeq int64) (int, error) {
	var depth int
	err := s.wal.Batch(func(tx *bolt.Tx) error {
		dirty := tx.Bucket(walDirtyBucket)
		key := []byte(walKey(ref))
		encoded := dirty.Get(key)
		if encoded == nil {
			return ErrNotFound
		}
		var entry dirtyEntry
		if err := json.Unmarshal(encoded, &entry); err != nil {
			return err
		}
		if entry.Record.WALSeq != walSeq {
			return ErrNotFound
		}
		entry.Record.WALState = WALStateCommitted
		entry.Record.FlushMySQL = true
		encoded, err := json.Marshal(entry)
		if err != nil {
			return err
		}
		if err := dirty.Put(key, encoded); err != nil {
			return err
		}
		depth = dirty.Stats().KeyN
		return nil
	})
	return depth, err
}

func (s *MySQLStore) abortDirty(ref EntryRef, walSeq int64) error {
	return s.wal.Batch(func(tx *bolt.Tx) error {
		dirty := tx.Bucket(walDirtyBucket)
		key := []byte(walKey(ref))
		encoded := dirty.Get(key)
		if encoded == nil {
			// Already gone — abort is idempotent.
			return nil
		}
		var entry dirtyEntry
		if err := json.Unmarshal(encoded, &entry); err != nil {
			return err
		}
		if entry.Record.WALSeq != walSeq {
			// Superseded by a newer prepare; do not touch the live record.
			return nil
		}
		if entry.Record.WALState == WALStateCommitted {
			return errors.New("cannot abort committed wal record")
		}
		return dirty.Delete(key)
	})
}

func (s *MySQLStore) loadDirty(ref EntryRef) (EntryRecord, bool, error) {
	var record EntryRecord
	var ok bool
	err := s.wal.View(func(tx *bolt.Tx) error {
		encoded := tx.Bucket(walDirtyBucket).Get([]byte(walKey(ref)))
		if encoded == nil {
			return nil
		}
		var entry dirtyEntry
		if err := json.Unmarshal(encoded, &entry); err != nil {
			return err
		}
		record = entry.Record.Clone()
		ok = true
		return nil
	})
	if err != nil {
		return EntryRecord{}, false, fmt.Errorf("load dirty wal entry %s: %w", walKey(ref), err)
	}
	return record, ok, nil
}

func (s *MySQLStore) loadDirtyBatch(limit int) ([]dirtyEntry, error) {
	entries := make([]dirtyEntry, 0, limit)
	err := s.wal.View(func(tx *bolt.Tx) error {
		cursor := tx.Bucket(walDirtyBucket).Cursor()
		for key, encoded := cursor.First(); key != nil && len(entries) < limit; key, encoded = cursor.Next() {
			var entry dirtyEntry
			if err := json.Unmarshal(encoded, &entry); err != nil {
				return err
			}
			entries = append(entries, entry)
		}
		return nil
	})
	return entries, err
}

func (s *MySQLStore) loadFlushableBatch(limit int) ([]dirtyEntry, error) {
	entries := make([]dirtyEntry, 0, limit)
	err := s.wal.View(func(tx *bolt.Tx) error {
		cursor := tx.Bucket(walDirtyBucket).Cursor()
		for key, encoded := cursor.First(); key != nil && len(entries) < limit; key, encoded = cursor.Next() {
			var entry dirtyEntry
			if err := json.Unmarshal(encoded, &entry); err != nil {
				return err
			}
			if !isFlushable(entry.Record) {
				continue
			}
			entries = append(entries, entry)
		}
		return nil
	})
	return entries, err
}

func (s *MySQLStore) deleteFlushed(entries []dirtyEntry) error {
	return s.wal.Update(func(tx *bolt.Tx) error {
		dirty := tx.Bucket(walDirtyBucket)
		for _, flushed := range entries {
			key := []byte(walKey(flushed.Record.Ref()))
			encoded := dirty.Get(key)
			if encoded == nil {
				continue
			}
			var current dirtyEntry
			if err := json.Unmarshal(encoded, &current); err != nil {
				return err
			}
			if current.Record.WALSeq == flushed.Record.WALSeq {
				if err := dirty.Delete(key); err != nil {
					return err
				}
			}
		}
		return nil
	})
}

// nextWALSeq returns a strictly-monotonic per-WAL identifier used to bind a
// prepared dirty record to its later Commit/Abort call. It is *not* persisted
// to MySQL — fence (Generation, Epoch, OwnerSeq) is the only durable ordering.
func nextWALSeq(bucket *bolt.Bucket) int64 {
	var current uint64
	if encoded := bucket.Get(walSeqKey); len(encoded) == 8 {
		current = binary.BigEndian.Uint64(encoded)
	}
	next := current + 1
	var encoded [8]byte
	binary.BigEndian.PutUint64(encoded[:], next)
	_ = bucket.Put(walSeqKey, encoded[:])
	return int64(next)
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

func walKey(ref EntryRef) string {
	return ref.DMap + "\x00" + strconv.FormatUint(ref.HKey, 10)
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
