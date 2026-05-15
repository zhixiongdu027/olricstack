package main

import (
	"context"
	"net"
	"sync/atomic"
	"testing"
	"time"

	topologypb "github.com/zhixiongdu/olricstack/api/topology/v1"
	"github.com/zhixiongdu/olricstack/internal/node"
	"github.com/zhixiongdu/olricstack/internal/topology"
	"google.golang.org/grpc"
)

func TestRunTopologySubscriptionReconnectsAfterPromotion(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	addr, service, cleanup := startTestWatchdog(t, ctx)
	defer cleanup()

	t.Setenv("STACK_ID", "demo")
	t.Setenv("WATCHDOG_SVC_NAME", addr)
	t.Setenv("POD_IP", "10.0.0.2")
	t.Setenv("POD_NAME", "demo-a")
	t.Setenv("NODE_ID", "node-a")
	t.Setenv("NODE_INCARNATION", "1")
	t.Setenv("HEARTBEAT_INTERVAL", "10ms")
	t.Setenv("WATCHDOG_RECONNECT_INTERVAL", "20ms")

	lease := node.NewLeaseTracker()
	done := make(chan struct{})
	go func() {
		defer close(done)
		runTopologySubscription(ctx, lease, nil)
	}()

	waitForLeaseState(t, lease, true, 2*time.Second)
	waitForWatchCalls(t, service, 1, 2*time.Second)

	service.SetLeadership(topologypb.WatchdogRole_WATCHDOG_ROLE_STANDBY, 1)
	waitForLeaseState(t, lease, false, 2*time.Second)
	waitForWatchCalls(t, service, 2, 2*time.Second)

	callsBeforePromotion := service.watchCalls.Load()
	service.SetLeadership(topologypb.WatchdogRole_WATCHDOG_ROLE_PRIMARY, 2)
	waitForLeaseState(t, lease, true, 2*time.Second)
	waitForWatchCalls(t, service, callsBeforePromotion+1, 2*time.Second)

	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("topology subscription loop did not stop on context cancel")
	}
}

func TestTopologyPeersSkipsSelfAndDeduplicates(t *testing.T) {
	envelope := &topologypb.TopologyEnvelope{Members: []*topologypb.Member{
		{NodeId: "node-a", PodIp: "10.0.0.2"},
		{NodeId: "node-b", PodIp: "10.0.0.3"},
		{NodeId: "node-c", PodIp: "10.0.0.3"},
		{NodeId: "node-d"},
	}}

	peers := topologyPeers(envelope, "node-a", 3322)
	if len(peers) != 1 || peers[0] != "10.0.0.3:3322" {
		t.Fatalf("unexpected peers: %v", peers)
	}
}

type countingTopologyServer struct {
	topologypb.UnimplementedTopologyControlServer

	service    *topology.Service
	watchCalls atomic.Int64
}

func (s *countingTopologyServer) GetTopology(ctx context.Context, req *topologypb.TopologyQuery) (*topologypb.TopologyEnvelope, error) {
	return s.service.GetTopology(ctx, req)
}

func (s *countingTopologyServer) Watch(stream topologypb.TopologyControl_WatchServer) error {
	s.watchCalls.Add(1)
	return s.service.Watch(stream)
}

func (s *countingTopologyServer) SetLeadership(role topologypb.WatchdogRole, generation int64) {
	s.service.SetLeadership(role, generation)
}

func startTestWatchdog(t *testing.T, ctx context.Context) (string, *countingTopologyServer, func()) {
	t.Helper()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}

	service := &countingTopologyServer{
		service: topology.NewServiceWithConfig(topology.Config{
			SuspectAfter:     250 * time.Millisecond,
			ExpireAfter:      500 * time.Millisecond,
			ReapInterval:     50 * time.Millisecond,
			BookwormInterval: 20 * time.Millisecond,
			LeaseTTL:         200 * time.Millisecond,
			WatchdogID:       "wd-primary",
			Generation:       1,
			Role:             topologypb.WatchdogRole_WATCHDOG_ROLE_PRIMARY,
		}),
	}
	go service.service.RunReaper(ctx)
	go service.service.RunBookworm(ctx)

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

func waitForWatchCalls(t *testing.T, service *countingTopologyServer, wantAtLeast int64, timeout time.Duration) {
	t.Helper()

	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if service.watchCalls.Load() >= wantAtLeast {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}

	t.Fatalf("watch calls did not reach %d within %s; got %d", wantAtLeast, timeout, service.watchCalls.Load())
}
