package store

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
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
	Version      int64     `gorm:"column:version;not null;index:idx_cache_version_writer" json:"version"`
	WriterID     string    `gorm:"column:writer_id;size:128;not null;index:idx_cache_version_writer" json:"writer_id"`
	UpdatedAt    time.Time `gorm:"column:updated_at" json:"updated_at"`
	Origin       string    `gorm:"-" json:"origin,omitempty"`
	FlushMySQL   bool      `gorm:"-" json:"flush_mysql,omitempty"`
}

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
	NodeID        string
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
	if c.NodeID == "" {
		c.NodeID = "unknown"
	}
	return c
}

type dirtyEntry struct {
	Record EntryRecord `json:"record"`
}

var (
	walDirtyBucket = []byte("dirty")
	walMetaBucket  = []byte("meta")
	walVersionKey  = []byte("version")
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
		s.mu.Lock()
		s.started = true
		s.mu.Unlock()
		s.notifyFlush()
		go s.flushLoop()
	})
	return nil
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

func (s *MySQLStore) LoadEntry(ctx context.Context, ref EntryRef) (EntryRecord, error) {
	if record, ok, err := s.loadDirty(ref); err != nil {
		return EntryRecord{}, err
	} else if ok {
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
	return s.store(ctx, record, 0)
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
	}, 0)
}

func (s *MySQLStore) store(ctx context.Context, record EntryRecord, version int64) error {
	s.mu.Lock()
	if s.closing {
		s.mu.Unlock()
		return errors.New("cache store is closed")
	}
	if err := ctx.Err(); err != nil {
		s.mu.Unlock()
		return err
	}
	depth, err := s.appendDirty(record, version)
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

func (s *MySQLStore) flushPending(pending map[string][]byte) bool {
	entries := make([]dirtyEntry, 0, len(pending))
	now := time.Now().UTC()
	for key, value := range pending {
		entries = append(entries, dirtyEntry{Record: EntryRecord{
			Key:          key,
			HKey:         uint64(len(key)),
			EncodedEntry: value,
			Version:      now.UnixNano(),
			WriterID:     s.cfg.NodeID,
			UpdatedAt:    now,
			Origin:       "legacy_pending",
			FlushMySQL:   true,
		}})
	}
	return s.flushEntries(entries)
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
		if !entry.Record.FlushMySQL {
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

func (s *MySQLStore) appendDirty(record EntryRecord, version int64) (int, error) {
	now := time.Now().UTC()
	record = record.Clone()
	if record.UpdatedAt.IsZero() {
		record.UpdatedAt = now
	}
	if record.WriterID == "" {
		record.WriterID = s.cfg.NodeID
	}
	entry := dirtyEntry{Record: record}
	var depth int
	err := s.wal.Update(func(tx *bolt.Tx) error {
		dirty := tx.Bucket(walDirtyBucket)
		key := []byte(walKey(record.Ref()))
		if dirty.Get(key) == nil && dirty.Stats().KeyN >= s.cfg.QueueSize {
			return ErrQueueFull
		}
		meta := tx.Bucket(walMetaBucket)
		entry.Record.Version = nextVersion(meta, entry.Record.UpdatedAt, version)
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
			if !entry.Record.FlushMySQL {
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
			if current.Record.Version == flushed.Record.Version {
				if err := dirty.Delete(key); err != nil {
					return err
				}
			}
		}
		return nil
	})
}

func nextVersion(bucket *bolt.Bucket, now time.Time, requested int64) int64 {
	var current uint64
	if encoded := bucket.Get(walVersionKey); len(encoded) == 8 {
		current = binary.BigEndian.Uint64(encoded)
	}
	next := uint64(now.UnixNano())
	if requested > 0 {
		next = uint64(requested)
	}
	if next <= current {
		next = current + 1
	}
	var encoded [8]byte
	binary.BigEndian.PutUint64(encoded[:], next)
	_ = bucket.Put(walVersionKey, encoded[:])
	return int64(next)
}

func (s *MySQLStore) versionedUpsertClause() clause.OnConflict {
	if s.db.Dialector.Name() == "mysql" {
		newer := "VALUES(version) > version OR (VALUES(version) = version AND VALUES(writer_id) > writer_id)"
		return clause.OnConflict{
			Columns: []clause.Column{{Name: "dmap"}, {Name: "hkey"}},
			DoUpdates: clause.Assignments(map[string]interface{}{
				"key":           gorm.Expr("CASE WHEN " + newer + " THEN VALUES(`key`) ELSE `key` END"),
				"encoded_entry": gorm.Expr("CASE WHEN " + newer + " THEN VALUES(encoded_entry) ELSE encoded_entry END"),
				"ttl":           gorm.Expr("CASE WHEN " + newer + " THEN VALUES(ttl) ELSE ttl END"),
				"timestamp":     gorm.Expr("CASE WHEN " + newer + " THEN VALUES(timestamp) ELSE timestamp END"),
				"tombstone":     gorm.Expr("CASE WHEN " + newer + " THEN VALUES(tombstone) ELSE tombstone END"),
				"version":       gorm.Expr("GREATEST(version, VALUES(version))"),
				"writer_id":     gorm.Expr("CASE WHEN " + newer + " THEN VALUES(writer_id) ELSE writer_id END"),
				"updated_at":    gorm.Expr("CASE WHEN " + newer + " THEN VALUES(updated_at) ELSE updated_at END"),
			}),
		}
	}

	newer := "excluded.version > version OR (excluded.version = version AND excluded.writer_id > writer_id)"
	return clause.OnConflict{
		Columns: []clause.Column{{Name: "dmap"}, {Name: "hkey"}},
		DoUpdates: clause.Assignments(map[string]interface{}{
			"key":           gorm.Expr("CASE WHEN " + newer + " THEN excluded.key ELSE key END"),
			"encoded_entry": gorm.Expr("CASE WHEN " + newer + " THEN excluded.encoded_entry ELSE encoded_entry END"),
			"ttl":           gorm.Expr("CASE WHEN " + newer + " THEN excluded.ttl ELSE ttl END"),
			"timestamp":     gorm.Expr("CASE WHEN " + newer + " THEN excluded.timestamp ELSE timestamp END"),
			"tombstone":     gorm.Expr("CASE WHEN " + newer + " THEN excluded.tombstone ELSE tombstone END"),
			"version":       gorm.Expr("MAX(version, excluded.version)"),
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
