package store

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
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
	Load(ctx context.Context, key string) ([]byte, error)
	Store(ctx context.Context, key string, value []byte) error
	Close(ctx context.Context) error
}

type Record struct {
	Key       string `gorm:"primaryKey;size:512;column:key"`
	Value     []byte `gorm:"column:value;type:longblob;not null"`
	Version   int64  `gorm:"column:version;not null;index:idx_cache_version_writer"`
	WriterID  string `gorm:"column:writer_id;size:128;not null;index:idx_cache_version_writer"`
	UpdatedAt time.Time
}

func (Record) TableName() string {
	return "olric_cache_records"
}

type Config struct {
	QueueSize     int
	FlushInterval time.Duration
	BatchSize     int
	WALPath       string
	NodeID        string
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
	if c.NodeID == "" {
		c.NodeID = "unknown"
	}
	return c
}

type dirtyEntry struct {
	Key       string    `json:"key"`
	Value     []byte    `json:"value"`
	Version   int64     `json:"version"`
	WriterID  string    `json:"writer_id"`
	UpdatedAt time.Time `json:"updated_at"`
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
	closing     bool
	workerErrMu sync.Mutex
	workerErr   error
}

var _ CacheStore = (*MySQLStore)(nil)

func NewMySQLStore(db *gorm.DB, cfg Config) (*MySQLStore, error) {
	if db == nil {
		return nil, errors.New("gorm db is nil")
	}

	cfg = cfg.withDefaults()
	if err := db.AutoMigrate(&Record{}); err != nil {
		return nil, fmt.Errorf("migrate cache table: %w", err)
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
	s.notifyFlush()
	go s.flushLoop()
	return s, nil
}

func (s *MySQLStore) Load(ctx context.Context, key string) ([]byte, error) {
	if value, ok, err := s.loadDirty(key); err != nil {
		return nil, err
	} else if ok {
		return value, nil
	}

	var rec Record
	if err := s.db.WithContext(ctx).First(&rec, "key = ?", key).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, ErrNotFound
		}
		return nil, fmt.Errorf("load key %q: %w", key, err)
	}

	value := make([]byte, len(rec.Value))
	copy(value, rec.Value)
	return value, nil
}

func (s *MySQLStore) Store(ctx context.Context, key string, value []byte) error {
	s.mu.Lock()
	if s.closing {
		s.mu.Unlock()
		return errors.New("cache store is closed")
	}
	if err := ctx.Err(); err != nil {
		s.mu.Unlock()
		return err
	}
	depth, err := s.appendDirty(key, value)
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
	s.closeOnce.Do(func() {
		s.mu.Lock()
		s.closing = true
		close(s.done)
		s.mu.Unlock()
	})

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
				s.flushWAL()
				return
			}
			s.flushWAL()
		case <-ticker.C:
			s.flushWAL()
		case <-s.done:
			s.flushWAL()
			return
		}
	}
}

func (s *MySQLStore) flushPending(pending map[string][]byte) bool {
	entries := make([]dirtyEntry, 0, len(pending))
	now := time.Now().UTC()
	for key, value := range pending {
		entries = append(entries, dirtyEntry{Key: key, Value: value, Version: now.UnixNano(), WriterID: s.cfg.NodeID, UpdatedAt: now})
	}
	return s.flushEntries(entries)
}

func (s *MySQLStore) flushWAL() bool {
	entries, err := s.loadDirtyBatch(s.cfg.BatchSize)
	if err != nil {
		s.setWorkerError(fmt.Errorf("load dirty wal: %w", err))
		return false
	}
	if len(entries) == 0 {
		s.setWorkerError(nil)
		return true
	}
	if !s.flushEntries(entries) {
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

func (s *MySQLStore) flushEntries(entries []dirtyEntry) bool {
	records := make([]Record, 0, len(entries))
	now := time.Now().UTC()
	for _, entry := range entries {
		records = append(records, Record{
			Key:       entry.Key,
			Value:     entry.Value,
			Version:   entry.Version,
			WriterID:  entry.WriterID,
			UpdatedAt: now,
		})
	}

	err := s.db.Clauses(s.versionedUpsertClause()).CreateInBatches(records, s.cfg.BatchSize).Error
	if err != nil {
		s.setWorkerError(fmt.Errorf("flush dirty records: %w", err))
		return false
	}
	s.setWorkerError(nil)
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

func (s *MySQLStore) appendDirty(key string, value []byte) (int, error) {
	entry := dirtyEntry{
		Key:       key,
		Value:     append([]byte(nil), value...),
		WriterID:  s.cfg.NodeID,
		UpdatedAt: time.Now().UTC(),
	}
	var depth int
	err := s.wal.Update(func(tx *bolt.Tx) error {
		dirty := tx.Bucket(walDirtyBucket)
		if dirty.Get([]byte(key)) == nil && dirty.Stats().KeyN >= s.cfg.QueueSize {
			return ErrQueueFull
		}
		meta := tx.Bucket(walMetaBucket)
		entry.Version = nextVersion(meta, entry.UpdatedAt)
		encoded, err := json.Marshal(entry)
		if err != nil {
			return err
		}
		if err := dirty.Put([]byte(key), encoded); err != nil {
			return err
		}
		depth = dirty.Stats().KeyN
		return nil
	})
	return depth, err
}

func (s *MySQLStore) loadDirty(key string) ([]byte, bool, error) {
	var value []byte
	var ok bool
	err := s.wal.View(func(tx *bolt.Tx) error {
		encoded := tx.Bucket(walDirtyBucket).Get([]byte(key))
		if encoded == nil {
			return nil
		}
		var entry dirtyEntry
		if err := json.Unmarshal(encoded, &entry); err != nil {
			return err
		}
		value = append([]byte(nil), entry.Value...)
		ok = true
		return nil
	})
	if err != nil {
		return nil, false, fmt.Errorf("load dirty wal key %q: %w", key, err)
	}
	return value, ok, nil
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

func (s *MySQLStore) deleteFlushed(entries []dirtyEntry) error {
	return s.wal.Update(func(tx *bolt.Tx) error {
		dirty := tx.Bucket(walDirtyBucket)
		for _, flushed := range entries {
			encoded := dirty.Get([]byte(flushed.Key))
			if encoded == nil {
				continue
			}
			var current dirtyEntry
			if err := json.Unmarshal(encoded, &current); err != nil {
				return err
			}
			if current.Version == flushed.Version {
				if err := dirty.Delete([]byte(flushed.Key)); err != nil {
					return err
				}
			}
		}
		return nil
	})
}

func nextVersion(bucket *bolt.Bucket, now time.Time) int64 {
	var current uint64
	if encoded := bucket.Get(walVersionKey); len(encoded) == 8 {
		current = binary.BigEndian.Uint64(encoded)
	}
	next := uint64(now.UnixNano())
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
			Columns: []clause.Column{{Name: "key"}},
			DoUpdates: clause.Assignments(map[string]interface{}{
				"value":      gorm.Expr("CASE WHEN " + newer + " THEN VALUES(value) ELSE value END"),
				"version":    gorm.Expr("GREATEST(version, VALUES(version))"),
				"writer_id":  gorm.Expr("CASE WHEN " + newer + " THEN VALUES(writer_id) ELSE writer_id END"),
				"updated_at": gorm.Expr("CASE WHEN " + newer + " THEN VALUES(updated_at) ELSE updated_at END"),
			}),
		}
	}

	newer := "excluded.version > version OR (excluded.version = version AND excluded.writer_id > writer_id)"
	return clause.OnConflict{
		Columns: []clause.Column{{Name: "key"}},
		DoUpdates: clause.Assignments(map[string]interface{}{
			"value":      gorm.Expr("CASE WHEN " + newer + " THEN excluded.value ELSE value END"),
			"version":    gorm.Expr("MAX(version, excluded.version)"),
			"writer_id":  gorm.Expr("CASE WHEN " + newer + " THEN excluded.writer_id ELSE writer_id END"),
			"updated_at": gorm.Expr("CASE WHEN " + newer + " THEN excluded.updated_at ELSE updated_at END"),
		}),
	}
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
