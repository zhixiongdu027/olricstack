package main

import (
	"context"
	"errors"
	"net"
	"sync/atomic"
	"testing"
	"time"

	topologypb "github.com/zhixiongdu/olricstack/api/topology/v1"
	"github.com/zhixiongdu/olricstack/internal/node"
	"github.com/zhixiongdu/olricstack/internal/topology"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func TestWatchEpochHealthTerminatesAfterConsecutiveFailures(t *testing.T) {
	t.Parallel()
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
	t.Parallel()
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

	// Poll deterministically: the test passes iff terminate is never invoked
	// within the watch window. Bail out immediately on the first termination
	// rather than rely on a fixed sleep that could miss a fast failure.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if got := terminated.Load(); got != 0 {
			t.Fatalf("terminate must not be called after transient recovery, got %d", got)
		}
		time.Sleep(10 * time.Millisecond)
	}
	if got := terminated.Load(); got != 0 {
		t.Fatalf("terminate must not be called after transient recovery, got %d", got)
	}
}

func TestValidateWatchdogTimingRejectsEpochWindowBeyondLease(t *testing.T) {
	t.Parallel()
	err := validateWatchdogTiming(topology.Config{
		LeaseTTL:    30 * time.Second,
		ExpireAfter: 20 * time.Second,
	}, 5*time.Second, 4)
	if err == nil {
		t.Fatal("expected invalid timing when degraded window reaches expire window")
	}
}

func TestValidateWatchdogTimingAcceptsWindowBelowLease(t *testing.T) {
	t.Parallel()
	err := validateWatchdogTiming(topology.Config{
		LeaseTTL:    30 * time.Second,
		ExpireAfter: 30 * time.Second,
	}, 5*time.Second, 3)
	if err != nil {
		t.Fatalf("expected timing to be accepted: %v", err)
	}
}

func TestAppConfigDemoteTopologySendsStandbyEnvelope(t *testing.T) {
	t.Parallel()
	service := topology.NewServiceWithConfig(topology.Config{
		Role:             topologypb.WatchdogRole_WATCHDOG_ROLE_PRIMARY,
		Generation:       7,
		LeaseTTL:         time.Minute,
		BookwormInterval: time.Hour,
		ReapInterval:     time.Hour,
	})
	addr, cleanup := serveTestTopology(t, service)
	defer cleanup()

	lease := node.NewLeaseTracker()
	subscriber, err := node.NewSubscriber(node.SubscriberConfig{
		StackID:           "stack-a",
		NodeID:            "node-a",
		PodName:           "pod-a",
		PodIP:             "10.0.0.2",
		Incarnation:       1,
		HeartbeatInterval: time.Hour,
		Lease:             lease,
	})
	if err != nil {
		t.Fatalf("new subscriber: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	conn, err := grpc.DialContext(ctx, addr, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithBlock())
	if err != nil {
		t.Fatalf("dial topology: %v", err)
	}
	defer conn.Close()
	done := make(chan error, 1)
	go func() {
		done <- subscriber.Run(ctx, topologypb.NewTopologyControlClient(conn))
	}()
	waitForLeaseState(t, lease, true, time.Second)

	cfg := &appConfig{}
	cfg.setTopologyService(service, 7)
	cfg.demoteTopology()

	waitForLeaseState(t, lease, false, time.Second)
	select {
	case err := <-done:
		if !errors.Is(err, node.ErrTopologyNotPrimary) {
			t.Fatalf("expected subscriber to stop on standby envelope, got %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("subscriber did not stop after demote")
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

func serveTestTopology(t *testing.T, service *topology.Service) (string, func()) {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	server := grpc.NewServer()
	topologypb.RegisterTopologyControlServer(server, service)
	go func() {
		_ = server.Serve(listener)
	}()
	return listener.Addr().String(), func() {
		server.Stop()
		_ = listener.Close()
	}
}

func waitForLeaseState(t *testing.T, lease *node.LeaseTracker, want bool, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if lease.ServingAllowed(time.Now()) == want {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("lease state did not become %t within %s", want, timeout)
}
