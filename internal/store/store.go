package store

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

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
	UpdatedAt time.Time
}

func (Record) TableName() string {
	return "olric_cache_records"
}

type Config struct {
	QueueSize     int
	FlushInterval time.Duration
	BatchSize     int
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
	return c
}

type dirtyEntry struct {
	key   string
	value []byte
}

type MySQLStore struct {
	db     *gorm.DB
	cfg    Config
	dirty  chan dirtyEntry
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
		dirty:  make(chan dirtyEntry, cfg.QueueSize),
		closed: make(chan struct{}),
	}
	go s.flushLoop()
	return s, nil
}

func (s *MySQLStore) Load(ctx context.Context, key string) ([]byte, error) {
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
	entry := dirtyEntry{key: key, value: append([]byte(nil), value...)}

	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closing {
		return errors.New("cache store is closed")
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case s.dirty <- entry:
		return nil
	default:
		return ErrQueueFull
	}
}

func (s *MySQLStore) Close(ctx context.Context) error {
	s.closeOnce.Do(func() {
		s.mu.Lock()
		s.closing = true
		close(s.dirty)
		s.mu.Unlock()
	})

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-s.closed:
		return s.workerError()
	}
}

func (s *MySQLStore) flushLoop() {
	defer close(s.closed)

	ticker := time.NewTicker(s.cfg.FlushInterval)
	defer ticker.Stop()

	pending := make(map[string][]byte)
	for {
		select {
		case entry, ok := <-s.dirty:
			if !ok {
				if len(pending) > 0 {
					s.flushPending(pending)
				}
				return
			}
			pending[entry.key] = entry.value
			if len(pending) >= s.cfg.BatchSize {
				if s.flushPending(pending) {
					pending = make(map[string][]byte)
				}
			}
		case <-ticker.C:
			if len(pending) > 0 {
				if s.flushPending(pending) {
					pending = make(map[string][]byte)
				}
			}
		}
	}
}

func (s *MySQLStore) flushPending(pending map[string][]byte) bool {
	records := make([]Record, 0, len(pending))
	now := time.Now().UTC()
	for key, value := range pending {
		records = append(records, Record{
			Key:       key,
			Value:     value,
			UpdatedAt: now,
		})
	}

	err := s.db.Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "key"}},
		DoUpdates: clause.AssignmentColumns([]string{"value", "updated_at"}),
	}).CreateInBatches(records, s.cfg.BatchSize).Error
	if err != nil {
		s.setWorkerError(fmt.Errorf("flush dirty records: %w", err))
		return false
	}
	s.setWorkerError(nil)
	return true
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
