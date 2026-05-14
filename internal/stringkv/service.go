package stringkv

import (
	"context"
	"errors"
	"time"

	"github.com/cespare/xxhash/v2"
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
	lease Lease
	now   func() time.Time
}

type DMapProvider interface {
	DMap(name string) (DMap, error)
}

func NewService(dmaps DMapProvider, lease Lease) (*Service, error) {
	if dmaps == nil {
		return nil, errors.New("dmap provider is nil")
	}
	if lease == nil {
		return nil, errors.New("lease is nil")
	}
	return &Service{
		dmaps: dmaps,
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
	return "", ErrNotFound
}

func (s *Service) Set(ctx context.Context, dmap, key, value string, ttl time.Duration) error {
	if err := s.requireLease(); err != nil {
		return err
	}
	dm, err := s.dmaps.DMap(dmap)
	if err != nil {
		return err
	}
	return dm.Set(ctx, key, value, ttl)
}

func (s *Service) Delete(ctx context.Context, dmap, key string) error {
	if err := s.requireLease(); err != nil {
		return err
	}
	dm, err := s.dmaps.DMap(dmap)
	if err != nil {
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
	return dm.Expire(ctx, key, ttl)
}

func (s *Service) requireLease() error {
	if s.lease.ServingAllowed(s.now()) {
		return nil
	}
	return ErrLeaseExpired
}

func HKey(dmap, key string) uint64 {
	return xxhash.Sum64String(dmap + key)
}
