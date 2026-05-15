package main

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	topologypb "github.com/zhixiongdu/olricstack/api/topology/v1"
	"github.com/zhixiongdu/olricstack/internal/topology"
)

func TestWatchEpochHealthTerminatesAfterConsecutiveFailures(t *testing.T) {
	service := topology.NewServiceWithConfig(topology.Config{
		EpochStore: persistentlyFailingEpochStore{},
	})
	// GetTopology will surface the epoch-store error to the caller; we don't
	// care about the call's return value, only that the failure is recorded
	// inside the service so EpochError() becomes non-nil.
	_, _ = service.GetTopology(context.Background(), &topologypb.TopologyQuery{StackId: "stack-a"})
	if service.EpochError() == nil {
		t.Fatal("expected EpochError to be set after seeding GetTopology")
	}

	var terminated atomic.Int32
	terminate := func(int) { terminated.Add(1) }

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go watchEpochHealth(ctx, service, terminate, 5*time.Millisecond, 3)

	deadline := time.Now().Add(500 * time.Millisecond)
	for time.Now().Before(deadline) {
		if terminated.Load() > 0 {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if got := terminated.Load(); got == 0 {
		t.Fatal("expected terminate to be called after consecutive epoch failures")
	}
}

func TestWatchEpochHealthIgnoresTransientFailure(t *testing.T) {
	store := newFlakyEpochStore()
	service := topology.NewServiceWithConfig(topology.Config{EpochStore: store})

	_, _ = service.GetTopology(context.Background(), &topologypb.TopologyQuery{StackId: "stack-a"})
	if service.EpochError() == nil {
		t.Fatal("expected initial EpochError")
	}

	store.recover()
	_, _ = service.GetTopology(context.Background(), &topologypb.TopologyQuery{StackId: "stack-a"})
	if service.EpochError() != nil {
		t.Fatalf("expected EpochError to clear after recovery, got %v", service.EpochError())
	}

	var terminated atomic.Int32
	terminate := func(int) { terminated.Add(1) }

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go watchEpochHealth(ctx, service, terminate, 5*time.Millisecond, 5)

	time.Sleep(60 * time.Millisecond)
	if got := terminated.Load(); got != 0 {
		t.Fatalf("terminate must not be called after transient recovery, got %d", got)
	}
}

type persistentlyFailingEpochStore struct{}

func (persistentlyFailingEpochStore) LoadEpoch(context.Context, string) (int64, error) {
	return 0, errors.New("load boom")
}

func (persistentlyFailingEpochStore) SaveEpoch(context.Context, string, int64) error {
	return errors.New("save boom")
}

type flakyMainEpochStore struct {
	failing atomic.Bool
}

func newFlakyEpochStore() *flakyMainEpochStore {
	s := &flakyMainEpochStore{}
	s.failing.Store(true)
	return s
}

func (s *flakyMainEpochStore) recover() { s.failing.Store(false) }

func (s *flakyMainEpochStore) LoadEpoch(context.Context, string) (int64, error) {
	if s.failing.Load() {
		return 0, errors.New("transient load boom")
	}
	return 0, nil
}

func (s *flakyMainEpochStore) SaveEpoch(context.Context, string, int64) error {
	if s.failing.Load() {
		return errors.New("transient save boom")
	}
	return nil
}
