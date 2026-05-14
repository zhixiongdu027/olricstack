package stringkv

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/cespare/xxhash/v2"
	"github.com/zhixiongdu/olricstack/internal/store"
)

var (
	ErrNotFound     = errors.New("string kv value not found")
	ErrLeaseExpired = errors.New("string kv serving lease is expired")
)

type DMap interface {
	Get(ctx context.Context, key string) (string, error)
	Set(ctx context.Context, key, value string, ttl time.Duration) error
	Delete(ctx context.Context, key string) error
	Expire(ctx context.Context, key string, ttl time.Duration) error
}

type Lease interface {
	ServingAllowed(now time.Time) bool
}

type Service struct {
	dmaps DMapProvider
	store store.CacheStore
	lease Lease
	now   func() time.Time
}

type DMapProvider interface {
	DMap(name string) (DMap, error)
}

func NewService(dmaps DMapProvider, backing store.CacheStore, lease Lease) (*Service, error) {
	if dmaps == nil {
		return nil, errors.New("dmap provider is nil")
	}
	if backing == nil {
		return nil, errors.New("backing store is nil")
	}
	if lease == nil {
		return nil, errors.New("lease is nil")
	}
	return &Service{
		dmaps: dmaps,
		store: backing,
		lease: lease,
		now:   time.Now,
	}, nil
}

func (s *Service) Get(ctx context.Context, dmap, key string) (string, error) {
	if err := s.requireLease(); err != nil {
		return "", err
	}
	dm, err := s.dmaps.DMap(dmap)
	if err != nil {
		return "", err
	}
	value, err := dm.Get(ctx, key)
	if err == nil {
		return value, nil
	}
	if !errors.Is(err, ErrNotFound) {
		return "", err
	}
	if err := s.requireLease(); err != nil {
		return "", err
	}

	record, err := s.store.LoadEntry(ctx, ref(dmap, key))
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return "", ErrNotFound
		}
		return "", err
	}
	record.Origin = "mysql_refill"
	record.FlushMySQL = false
	if err := s.store.StoreEntry(ctx, record); err != nil {
		return "", fmt.Errorf("record mysql refill: %w", err)
	}
	if err := dm.Set(ctx, key, string(record.EncodedEntry), ttlFromRecord(record, s.now())); err != nil {
		return "", fmt.Errorf("refill olric: %w", err)
	}
	return string(record.EncodedEntry), nil
}

func (s *Service) Set(ctx context.Context, dmap, key, value string, ttl time.Duration) error {
	if err := s.requireLease(); err != nil {
		return err
	}
	dm, err := s.dmaps.DMap(dmap)
	if err != nil {
		return err
	}
	if err := s.store.StoreEntry(ctx, s.record(dmap, key, value, ttl, "client_set", true)); err != nil {
		return err
	}
	if err := dm.Set(ctx, key, value, ttl); err != nil {
		return err
	}
	return nil
}

func (s *Service) Delete(ctx context.Context, dmap, key string) error {
	if err := s.requireLease(); err != nil {
		return err
	}
	dm, err := s.dmaps.DMap(dmap)
	if err != nil {
		return err
	}
	if err := s.store.DeleteEntry(ctx, ref(dmap, key)); err != nil {
		return err
	}
	if err := dm.Delete(ctx, key); err != nil && !errors.Is(err, ErrNotFound) {
		return err
	}
	return nil
}

func (s *Service) Expire(ctx context.Context, dmap, key string, ttl time.Duration) error {
	if ttl <= 0 {
		return errors.New("ttl must be positive")
	}
	if err := s.requireLease(); err != nil {
		return err
	}
	dm, err := s.dmaps.DMap(dmap)
	if err != nil {
		return err
	}
	value, err := dm.Get(ctx, key)
	if err != nil {
		return err
	}
	if err := s.store.StoreEntry(ctx, s.record(dmap, key, value, ttl, "client_expire", true)); err != nil {
		return err
	}
	return dm.Expire(ctx, key, ttl)
}

func (s *Service) requireLease() error {
	if s.lease.ServingAllowed(s.now()) {
		return nil
	}
	return ErrLeaseExpired
}

func (s *Service) record(dmap, key, value string, ttl time.Duration, origin string, flush bool) store.EntryRecord {
	now := s.now()
	return store.EntryRecord{
		DMap:         dmap,
		Key:          key,
		HKey:         HKey(dmap, key),
		EncodedEntry: []byte(value),
		TTL:          expiresAtUnixMilli(now, ttl),
		Timestamp:    now.UnixNano(),
		Origin:       origin,
		FlushMySQL:   flush,
		UpdatedAt:    now.UTC(),
	}
}

func ref(dmap, key string) store.EntryRef {
	return store.EntryRef{DMap: dmap, Key: key, HKey: HKey(dmap, key)}
}

func HKey(dmap, key string) uint64 {
	return xxhash.Sum64String(dmap + key)
}

func expiresAtUnixMilli(now time.Time, ttl time.Duration) int64 {
	if ttl <= 0 {
		return 0
	}
	return now.Add(ttl).UnixMilli()
}

func ttlFromRecord(record store.EntryRecord, now time.Time) time.Duration {
	if record.TTL <= 0 {
		return 0
	}
	ttl := time.Until(time.UnixMilli(record.TTL))
	if now.IsZero() {
		return ttl
	}
	return time.UnixMilli(record.TTL).Sub(now)
}
