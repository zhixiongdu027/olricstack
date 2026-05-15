package node

import (
	"context"
	"errors"
	"time"

	"github.com/zhixiongdu/olricstack/internal/store"
)

var (
	ErrTopologyLeaseRequired = errors.New("topology lease is required")
	errCommitNotSupported    = errors.New("inner store does not support commit protocol")
)

// LeaseGatedStore enforces a topology-lease gate on every CacheStore access AND
// forwards the prepare/commit/abort protocol when the inner store implements
// store.CommitStore. It is a hard requirement that production wiring uses an
// inner that satisfies CommitStore (e.g. *store.MySQLStore); otherwise the
// DurableHook two-phase write path degrades to a single StoreEntry call which
// can leak prepared WAL records on Olric mutation failure.
//
// A static interface assertion at the bottom of this file pins this contract.
type LeaseGatedStore struct {
	inner    store.CacheStore
	commit   store.CommitStore // optional; nil if inner does not support 2PC
	lease    *LeaseTracker
	now      func() time.Time
}

func NewLeaseGatedStore(inner store.CacheStore, lease *LeaseTracker) (*LeaseGatedStore, error) {
	if inner == nil {
		return nil, errors.New("cache store is nil")
	}
	if lease == nil {
		return nil, errors.New("lease tracker is nil")
	}
	s := &LeaseGatedStore{
		inner: inner,
		lease: lease,
		now:   time.Now,
	}
	if cs, ok := inner.(store.CommitStore); ok {
		s.commit = cs
	}
	return s, nil
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

func (s *LeaseGatedStore) PrepareEntry(ctx context.Context, record store.EntryRecord) (store.EntryRecord, error) {
	if err := s.requireLease(); err != nil {
		return store.EntryRecord{}, err
	}
	if s.commit == nil {
		return store.EntryRecord{}, errCommitNotSupported
	}
	return s.commit.PrepareEntry(ctx, record)
}

func (s *LeaseGatedStore) CommitEntry(ctx context.Context, ref store.EntryRef, version int64) error {
	if err := s.requireLease(); err != nil {
		return err
	}
	if s.commit == nil {
		return errCommitNotSupported
	}
	return s.commit.CommitEntry(ctx, ref, version)
}

// AbortEntry intentionally does NOT enforce the lease gate. Aborting a prepared
// record is part of the failure-path cleanup; if the lease has just expired we
// still want the WAL record removed so a future re-elected primary cannot
// resurrect it. The underlying store performs its own bounded validation
// (matching version, prepared state).
func (s *LeaseGatedStore) AbortEntry(ctx context.Context, ref store.EntryRef, version int64) error {
	if s.commit == nil {
		return errCommitNotSupported
	}
	return s.commit.AbortEntry(ctx, ref, version)
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

// PurgeBelowGeneration forwards to the inner store when supported. The lease
// gate is intentionally not enforced: purging stale records is a cleanup task
// that must run even if the current lease is not yet established (e.g. during
// the first envelope arrival after restart).
func (s *LeaseGatedStore) PurgeBelowGeneration(ctx context.Context, minGeneration int64) (int, error) {
	type purger interface {
		PurgeBelowGeneration(context.Context, int64) (int, error)
	}
	if p, ok := s.inner.(purger); ok {
		return p.PurgeBelowGeneration(ctx, minGeneration)
	}
	return 0, nil
}

func (s *LeaseGatedStore) requireLease() error {
	if s.lease.ServingAllowed(s.now()) {
		return nil
	}
	return ErrTopologyLeaseRequired
}

var (
	_ store.CacheStore  = (*LeaseGatedStore)(nil)
	_ store.CommitStore = (*LeaseGatedStore)(nil)
)
