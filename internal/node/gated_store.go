package node

import (
	"context"
	"errors"
	"time"

	"github.com/zhixiongdu/olricstack/internal/store"
)

var ErrTopologyLeaseRequired = errors.New("topology lease is required")

type LeaseGatedStore struct {
	inner store.CacheStore
	lease *LeaseTracker
	now   func() time.Time
}

func NewLeaseGatedStore(inner store.CacheStore, lease *LeaseTracker) (*LeaseGatedStore, error) {
	if inner == nil {
		return nil, errors.New("cache store is nil")
	}
	if lease == nil {
		return nil, errors.New("lease tracker is nil")
	}
	return &LeaseGatedStore{
		inner: inner,
		lease: lease,
		now:   time.Now,
	}, nil
}

func (s *LeaseGatedStore) LoadEntry(ctx context.Context, ref store.EntryRef) (store.EntryRecord, error) {
	if err := s.requireLease(); err != nil {
		return store.EntryRecord{}, err
	}
	return s.inner.LoadEntry(ctx, ref)
}

func (s *LeaseGatedStore) StoreEntry(ctx context.Context, record store.EntryRecord) error {
	if err := s.requireLease(); err != nil {
		return err
	}
	return s.inner.StoreEntry(ctx, record)
}

func (s *LeaseGatedStore) DeleteEntry(ctx context.Context, ref store.EntryRef) error {
	if err := s.requireLease(); err != nil {
		return err
	}
	return s.inner.DeleteEntry(ctx, ref)
}

func (s *LeaseGatedStore) Close(ctx context.Context) error {
	return s.inner.Close(ctx)
}

func (s *LeaseGatedStore) Start(ctx context.Context) error {
	if starter, ok := s.inner.(store.Starter); ok {
		return starter.Start(ctx)
	}
	return nil
}

func (s *LeaseGatedStore) Replay(ctx context.Context, f func(store.EntryRecord) error) error {
	if replay, ok := s.inner.(store.ReplayStore); ok {
		return replay.Replay(ctx, f)
	}
	return nil
}

func (s *LeaseGatedStore) requireLease() error {
	if s.lease.ServingAllowed(s.now()) {
		return nil
	}
	return ErrTopologyLeaseRequired
}
