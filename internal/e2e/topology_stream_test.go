//go:build e2e

package e2e

import (
	"context"
	"errors"
	"net"
	"reflect"
	"sort"
	"sync"
	"testing"
	"time"

	topologypb "github.com/zhixiongdu/olricstack/api/topology/v1"
	"github.com/zhixiongdu/olricstack/internal/node"
	"github.com/zhixiongdu/olricstack/internal/topology"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

func TestTopologySubscriptionE2E(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	addr, _, cleanup := startWatchdog(t, ctx)
	defer cleanup()

	client, closeClient := newTopologyClient(t, addr)
	defer closeClient()

	joinerA := &historyJoiner{}
	subA := newSubscriber(t, "node-a", "demo-a", "10.0.0.2", joinerA)
	ctxA, cancelA := context.WithCancel(ctx)
	defer cancelA()
	runSubscriber(t, ctxA, subA, client)
	joinerA.waitFor(t, []string{"node-a"}, 0)

	joinerB := &historyJoiner{}
	subB := newSubscriber(t, "node-b", "demo-b", "10.0.0.3", joinerB)
	ctxB, cancelB := context.WithCancel(ctx)
	defer cancelB()
	runSubscriber(t, ctxB, subB, client)

	expectedBoth := []string{"node-a", "node-b"}
	joinerA.waitFor(t, expectedBoth, 0)
	joinerB.waitFor(t, expectedBoth, 0)
	joinerA.waitForRepeatedEpoch(t, joinerA.lastEpoch(), joinerA.len())

	startIndex := joinerA.len()
	cancelB()
	joinerA.waitFor(t, []string{"node-a"}, startIndex)
}

func TestTopologySubscriptionRevokesLeaseOnDemotion(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	addr, service, cleanup := startWatchdog(t, ctx)
	defer cleanup()

	client, closeClient := newTopologyClient(t, addr)
	defer closeClient()

	joiner := &historyJoiner{}
	lease := node.NewLeaseTracker()
	sub, err := node.NewSubscriber(node.SubscriberConfig{
		StackID:           "demo",
		NodeID:            "node-a",
		PodName:           "demo-a",
		PodIP:             "10.0.0.2",
		Incarnation:       1,
		ProtocolVersion:   "v2",
		HeartbeatInterval: 10 * time.Millisecond,
		Lease:             lease,
		Joiner:            joiner,
	})
	if err != nil {
		t.Fatalf("new subscriber: %v", err)
	}

	errCh := make(chan error, 1)
	go func() {
		errCh <- sub.Run(ctx, client)
	}()

	joiner.waitFor(t, []string{"node-a"}, 0)
	if !lease.ServingAllowed(time.Now()) {
		t.Fatal("expected lease to become valid before demotion")
	}

	service.SetLeadership(topologypb.WatchdogRole_WATCHDOG_ROLE_STANDBY, 1)

	select {
	case err := <-errCh:
		if !errors.Is(err, node.ErrTopologyNotPrimary) {
			t.Fatalf("expected standby demotion error, got %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("subscriber did not stop after demotion")
	}

	if lease.ServingAllowed(time.Now()) {
		t.Fatal("expected lease to be revoked immediately on demotion")
	}
}

func TestTopologySubscriptionReconnectsAfterPromotion(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	addr, service, cleanup := startWatchdog(t, ctx)
	defer cleanup()

	client, closeClient := newTopologyClient(t, addr)
	defer closeClient()

	joiner := &historyJoiner{}
	lease := node.NewLeaseTracker()
	sub, err := node.NewSubscriber(node.SubscriberConfig{
		StackID:           "demo",
		NodeID:            "node-a",
		PodName:           "demo-a",
		PodIP:             "10.0.0.2",
		Incarnation:       1,
		ProtocolVersion:   "v2",
		HeartbeatInterval: 10 * time.Millisecond,
		Lease:             lease,
		Joiner:            joiner,
	})
	if err != nil {
		t.Fatalf("new subscriber: %v", err)
	}

	firstErrCh := make(chan error, 1)
	go func() {
		firstErrCh <- sub.Run(ctx, client)
	}()

	joiner.waitFor(t, []string{"node-a"}, 0)
	if !lease.ServingAllowed(time.Now()) {
		t.Fatal("expected lease to become valid before demotion")
	}

	service.SetLeadership(topologypb.WatchdogRole_WATCHDOG_ROLE_STANDBY, 1)

	select {
	case err := <-firstErrCh:
		if !errors.Is(err, node.ErrTopologyNotPrimary) {
			t.Fatalf("expected standby demotion error, got %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("subscriber did not stop after demotion")
	}
	if lease.ServingAllowed(time.Now()) {
		t.Fatal("expected lease to be revoked on demotion")
	}

	service.SetLeadership(topologypb.WatchdogRole_WATCHDOG_ROLE_PRIMARY, 2)
	reconnectClient, closeReconnectClient := newTopologyClient(t, addr)
	defer closeReconnectClient()

	startIndex := joiner.len()
	secondErrCh := make(chan error, 1)
	go func() {
		secondErrCh <- sub.Run(ctx, reconnectClient)
	}()

	joiner.waitFor(t, []string{"node-a"}, startIndex)
	if !lease.ServingAllowed(time.Now()) {
		t.Fatal("expected lease to become valid again after promotion")
	}

	cancel()
	select {
	case err := <-secondErrCh:
		if !errors.Is(err, context.Canceled) && status.Code(err) != codes.Canceled {
			t.Fatalf("expected canceled reconnect shutdown, got %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("reconnected subscriber did not stop on context cancel")
	}
}

func startWatchdog(t *testing.T, ctx context.Context) (string, *topology.Service, func()) {
	t.Helper()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}

	service := topology.NewServiceWithConfig(topology.Config{
		SuspectAfter:     30 * time.Millisecond,
		ExpireAfter:      60 * time.Millisecond,
		ReapInterval:     10 * time.Millisecond,
		BookwormInterval: 10 * time.Millisecond,
		LeaseTTL:         60 * time.Millisecond,
		WatchdogID:       "wd-primary",
		Generation:       1,
		Role:             topologypb.WatchdogRole_WATCHDOG_ROLE_PRIMARY,
	})
	go service.RunReaper(ctx)
	go service.RunBookworm(ctx)

	server := grpc.NewServer()
	topologypb.RegisterTopologyControlServer(server, service)
	go func() {
		_ = server.Serve(listener)
	}()

	return listener.Addr().String(), service, func() {
		server.GracefulStop()
		_ = listener.Close()
	}
}

func newTopologyClient(t *testing.T, addr string) (topologypb.TopologyControlClient, func()) {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	conn, err := grpc.DialContext(ctx, addr, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithBlock())
	if err != nil {
		t.Fatalf("dial watchdog: %v", err)
	}
	return topologypb.NewTopologyControlClient(conn), func() {
		_ = conn.Close()
	}
}

func newSubscriber(t *testing.T, nodeID, podName, podIP string, joiner node.Joiner) *node.Subscriber {
	t.Helper()

	sub, err := node.NewSubscriber(node.SubscriberConfig{
		StackID:           "demo",
		NodeID:            nodeID,
		PodName:           podName,
		PodIP:             podIP,
		Incarnation:       1,
		ProtocolVersion:   "v2",
		HeartbeatInterval: 10 * time.Millisecond,
		Joiner:            joiner,
	})
	if err != nil {
		t.Fatalf("new subscriber: %v", err)
	}
	return sub
}

func runSubscriber(t *testing.T, ctx context.Context, sub *node.Subscriber, client topologypb.TopologyControlClient) {
	t.Helper()

	go func() {
		err := sub.Run(ctx, client)
		if err != nil && ctx.Err() == nil {
			t.Errorf("subscriber failed: %v", err)
		}
	}()
}

type historyJoiner struct {
	mu      sync.Mutex
	history [][]string
	epochs  []int64
	notify  chan struct{}
}

func (j *historyJoiner) Join(ctx context.Context, envelope *topologypb.TopologyEnvelope) error {
	nodeIDs := make([]string, 0, len(envelope.GetMembers()))
	for _, member := range envelope.GetMembers() {
		nodeIDs = append(nodeIDs, member.GetNodeId())
	}
	sort.Strings(nodeIDs)

	j.mu.Lock()
	j.history = append(j.history, nodeIDs)
	j.epochs = append(j.epochs, envelope.GetEpoch())
	if j.notify != nil {
		close(j.notify)
		j.notify = nil
	}
	j.mu.Unlock()
	return nil
}

func (j *historyJoiner) len() int {
	j.mu.Lock()
	defer j.mu.Unlock()
	return len(j.history)
}

func (j *historyJoiner) lastEpoch() int64 {
	j.mu.Lock()
	defer j.mu.Unlock()
	if len(j.epochs) == 0 {
		return 0
	}
	return j.epochs[len(j.epochs)-1]
}

func (j *historyJoiner) waitFor(t *testing.T, expected []string, startIndex int) {
	t.Helper()

	deadline := time.Now().Add(2 * time.Second)
	for {
		j.mu.Lock()
		for _, seen := range j.history[startIndex:] {
			if reflect.DeepEqual(seen, expected) {
				j.mu.Unlock()
				return
			}
		}
		if time.Now().After(deadline) {
			history := append([][]string(nil), j.history...)
			j.mu.Unlock()
			t.Fatalf("expected topology %v, history=%v", expected, history)
		}
		if j.notify == nil {
			j.notify = make(chan struct{})
		}
		notify := j.notify
		j.mu.Unlock()

		select {
		case <-notify:
		case <-time.After(10 * time.Millisecond):
		}
	}
}

func (j *historyJoiner) waitForRepeatedEpoch(t *testing.T, epoch int64, startIndex int) {
	t.Helper()

	deadline := time.Now().Add(2 * time.Second)
	for {
		j.mu.Lock()
		for _, seen := range j.epochs[startIndex:] {
			if seen == epoch {
				j.mu.Unlock()
				return
			}
		}
		if time.Now().After(deadline) {
			epochs := append([]int64(nil), j.epochs...)
			j.mu.Unlock()
			t.Fatalf("expected repeated epoch %d after index %d, epochs=%v", epoch, startIndex, epochs)
		}
		if j.notify == nil {
			j.notify = make(chan struct{})
		}
		notify := j.notify
		j.mu.Unlock()

		select {
		case <-notify:
		case <-time.After(10 * time.Millisecond):
		}
	}
}
