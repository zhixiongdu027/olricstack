package topology

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"

	topologypb "github.com/zhixiongdu/olricstack/api/topology/v1"
	stackwatchdog "github.com/zhixiongdu/olricstack/internal/watchdog"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	healthgrpc "google.golang.org/grpc/health/grpc_health_v1"
)

func TestServiceRegistersHeartbeatAndReturnsTopology(t *testing.T) {
	service := NewService()
	service.registerHeartbeat(heartbeat("stack-a", "node-a", "pod-a", "10.0.0.2", 1), &subscriber{ch: make(chan *topologypb.TopologyEnvelope, 1)})

	resp, err := service.GetTopology(context.Background(), &topologypb.TopologyQuery{StackId: "stack-a"})
	if err != nil {
		t.Fatalf("get topology: %v", err)
	}

	if resp.GetEpoch() != 1 {
		t.Fatalf("expected epoch 1, got %d", resp.GetEpoch())
	}
	if len(resp.GetMembers()) != 1 || resp.GetMembers()[0].GetNodeId() != "node-a" {
		t.Fatalf("unexpected members: %v", resp.GetMembers())
	}
	if resp.GetWatchdogRole() != topologypb.WatchdogRole_WATCHDOG_ROLE_PRIMARY {
		t.Fatalf("expected primary role, got %s", resp.GetWatchdogRole())
	}
}

func TestServiceKeepsStacksIsolated(t *testing.T) {
	service := NewService()
	service.registerHeartbeat(heartbeat("stack-a", "node-a", "pod-a", "10.0.0.2", 1), &subscriber{ch: make(chan *topologypb.TopologyEnvelope, 1)})
	service.registerHeartbeat(heartbeat("stack-b", "node-b", "pod-b", "10.0.1.2", 1), &subscriber{ch: make(chan *topologypb.TopologyEnvelope, 1)})

	resp, err := service.GetTopology(context.Background(), &topologypb.TopologyQuery{StackId: "stack-a"})
	if err != nil {
		t.Fatalf("get topology: %v", err)
	}

	if len(resp.GetMembers()) != 1 || resp.GetMembers()[0].GetNodeId() != "node-a" {
		t.Fatalf("unexpected members for stack-a: %v", resp.GetMembers())
	}
}

func TestServicePrunesExpiredHeartbeats(t *testing.T) {
	service := NewServiceWithConfig(Config{SuspectAfter: time.Millisecond, ExpireAfter: 2 * time.Millisecond})
	service.registerHeartbeat(heartbeat("stack-a", "node-a", "pod-a", "10.0.0.2", 1), &subscriber{ch: make(chan *topologypb.TopologyEnvelope, 1)})

	time.Sleep(3 * time.Millisecond)
	resp, err := service.GetTopology(context.Background(), &topologypb.TopologyQuery{StackId: "stack-a"})
	if err != nil {
		t.Fatalf("get topology: %v", err)
	}

	if len(resp.GetMembers()) != 0 {
		t.Fatalf("expected expired topology to be empty, got %v", resp.GetMembers())
	}
	if resp.GetEpoch() != 2 {
		t.Fatalf("expected epoch 2 after prune, got %d", resp.GetEpoch())
	}
}

func TestServiceBookwormPushesSnapshotWithoutEpochChange(t *testing.T) {
	service := NewService()
	sub := &subscriber{ch: make(chan *topologypb.TopologyEnvelope, 4)}
	service.registerHeartbeat(heartbeat("stack-a", "node-a", "pod-a", "10.0.0.2", 1), sub)

	initial := <-sub.ch
	service.broadcastSnapshots(topologypb.TopologyReason_TOPOLOGY_REASON_BOOKWORM)
	snapshot := <-sub.ch

	if snapshot.GetEpoch() != initial.GetEpoch() {
		t.Fatalf("expected unchanged epoch %d, got %d", initial.GetEpoch(), snapshot.GetEpoch())
	}
	if len(snapshot.GetMembers()) != len(initial.GetMembers()) {
		t.Fatalf("expected same member count")
	}
}

func TestServicePodObservationDoesNotCreateTopologyMembership(t *testing.T) {
	service := NewService()
	service.ObservePods("stack-a", []stackwatchdog.PodObservation{{
		PodName: "pod-a",
		PodIP:   "10.0.0.2",
		Ready:   true,
		Phase:   "Running",
		SeenAt:  time.Now(),
	}})

	resp, err := service.GetTopology(context.Background(), &topologypb.TopologyQuery{StackId: "stack-a"})
	if err != nil {
		t.Fatalf("get topology: %v", err)
	}
	if len(resp.GetMembers()) != 0 {
		t.Fatalf("pod-only observation must not create membership, got %v", resp.GetMembers())
	}
}

func TestServicePodObservationCanExcludeHeartbeatingTerminalNode(t *testing.T) {
	service := NewService()
	sub := &subscriber{ch: make(chan *topologypb.TopologyEnvelope, 4)}
	service.registerHeartbeat(heartbeat("stack-a", "node-a", "pod-a", "10.0.0.2", 1), sub)
	<-sub.ch

	service.ObservePods("stack-a", []stackwatchdog.PodObservation{{
		PodName: "pod-a",
		PodIP:   "10.0.0.2",
		Ready:   false,
		Phase:   "Failed",
		SeenAt:  time.Now(),
	}})

	resp := <-sub.ch
	if len(resp.GetMembers()) != 0 {
		t.Fatalf("expected failed pod excluded from topology, got %v", resp.GetMembers())
	}
	if resp.GetEpoch() != 2 {
		t.Fatalf("expected epoch 2, got %d", resp.GetEpoch())
	}
}

func TestStandbyRejectsTopologyOwnership(t *testing.T) {
	service := NewServiceWithConfig(Config{Role: topologypb.WatchdogRole_WATCHDOG_ROLE_STANDBY})
	service.ObservePods("stack-a", []stackwatchdog.PodObservation{{
		PodName: "pod-a",
		PodIP:   "10.0.0.2",
		Ready:   true,
		Phase:   "Running",
		SeenAt:  time.Now(),
	}})

	resp, err := service.GetTopology(context.Background(), &topologypb.TopologyQuery{StackId: "stack-a"})
	if err != nil {
		t.Fatalf("get topology: %v", err)
	}
	if len(resp.GetMembers()) != 0 {
		t.Fatalf("standby should not own members, got %v", resp.GetMembers())
	}
	if resp.GetWatchdogRole() != topologypb.WatchdogRole_WATCHDOG_ROLE_STANDBY {
		t.Fatalf("expected standby role")
	}
}

func TestStandbyGetTopologyDoesNotReturnOwnedMembers(t *testing.T) {
	service := NewServiceWithConfig(Config{Role: topologypb.WatchdogRole_WATCHDOG_ROLE_PRIMARY})
	service.registerHeartbeat(heartbeat("stack-a", "node-a", "pod-a", "10.0.0.2", 1), &subscriber{ch: make(chan *topologypb.TopologyEnvelope, 1)})
	service.SetLeadership(topologypb.WatchdogRole_WATCHDOG_ROLE_STANDBY, 1)

	resp, err := service.GetTopology(context.Background(), &topologypb.TopologyQuery{StackId: "stack-a"})
	if err != nil {
		t.Fatalf("get topology: %v", err)
	}
	if len(resp.GetMembers()) != 0 {
		t.Fatalf("standby must not return stale owned members, got %v", resp.GetMembers())
	}
	if resp.GetWatchdogRole() != topologypb.WatchdogRole_WATCHDOG_ROLE_STANDBY {
		t.Fatalf("expected standby role")
	}
}

func TestServiceReplacesOlderSubscriberWithNewerIncarnation(t *testing.T) {
	service := NewService()
	oldSub := &subscriber{ch: make(chan *topologypb.TopologyEnvelope, 1), done: make(chan struct{})}
	newSub := &subscriber{ch: make(chan *topologypb.TopologyEnvelope, 1), done: make(chan struct{})}

	service.registerHeartbeat(heartbeat("stack-a", "node-a", "pod-a", "10.0.0.2", 1), oldSub)
	service.registerHeartbeat(heartbeat("stack-a", "node-a", "pod-a", "10.0.0.3", 2), newSub)

	select {
	case <-oldSub.done:
	default:
		t.Fatal("expected old subscriber to be closed")
	}

	resp, err := service.GetTopology(context.Background(), &topologypb.TopologyQuery{StackId: "stack-a"})
	if err != nil {
		t.Fatalf("get topology: %v", err)
	}
	if got := resp.GetMembers()[0].GetPodIp(); got != "10.0.0.3" {
		t.Fatalf("expected newer pod ip, got %q", got)
	}
}

func TestServiceDoesNotReplaceWithStaleIncarnation(t *testing.T) {
	service := NewService()
	currentSub := &subscriber{ch: make(chan *topologypb.TopologyEnvelope, 1), done: make(chan struct{})}
	staleSub := &subscriber{ch: make(chan *topologypb.TopologyEnvelope, 1), done: make(chan struct{})}

	service.registerHeartbeat(heartbeat("stack-a", "node-a", "pod-a", "10.0.0.2", 2), currentSub)
	service.registerHeartbeat(heartbeat("stack-a", "node-a", "pod-old", "10.0.0.9", 1), staleSub)

	select {
	case <-currentSub.done:
		t.Fatal("current subscriber must not be closed by stale heartbeat")
	default:
	}

	resp, err := service.GetTopology(context.Background(), &topologypb.TopologyQuery{StackId: "stack-a"})
	if err != nil {
		t.Fatalf("get topology: %v", err)
	}
	if got := resp.GetMembers()[0].GetPodIp(); got != "10.0.0.2" {
		t.Fatalf("stale heartbeat overwrote pod ip: %q", got)
	}
}

func TestServiceRecordsEpochPersistenceFailure(t *testing.T) {
	service := NewServiceWithConfig(Config{EpochStore: failingEpochStore{err: errors.New("boom")}})
	health := stackwatchdog.NewLeadershipHealth()
	service.AddLeadershipObserver(health)
	service.registerHeartbeat(heartbeat("stack-a", "node-a", "pod-a", "10.0.0.2", 1), &subscriber{ch: make(chan *topologypb.TopologyEnvelope, 1)})

	if service.EpochError() == nil {
		t.Fatal("expected epoch persistence error")
	}
	if got := healthStatus(t, health); got != "NOT_SERVING" {
		t.Fatalf("expected degraded health, got %s", got)
	}
}

func TestServiceDoesNotBroadcastWhenEpochSaveFails(t *testing.T) {
	store := &flakyEpochStore{err: errors.New("boom")}
	service := NewServiceWithConfig(Config{EpochStore: store})

	sub := &subscriber{ch: make(chan *topologypb.TopologyEnvelope, 1), done: make(chan struct{})}
	service.registerHeartbeat(heartbeat("stack-a", "node-a", "pod-a", "10.0.0.2", 1), sub)

	if service.EpochError() == nil {
		t.Fatal("expected epoch persistence error")
	}
	// W2: when SaveEpoch fails, no envelope must be enqueued. Otherwise a
	// subsequent primary that recovers the older persisted epoch would be
	// unable to push any envelope the lease tracker accepts (P0-2 deadlock).
	select {
	case env := <-sub.ch:
		t.Fatalf("envelope must not be broadcast while epoch save is failing: %#v", env)
	default:
	}
}

func TestServiceBookwormPausedWhenDegraded(t *testing.T) {
	store := &flakyEpochStore{err: errors.New("boom")}
	service := NewServiceWithConfig(Config{
		EpochStore:       store,
		BookwormInterval: 5 * time.Millisecond,
		ReapInterval:     5 * time.Millisecond,
	})

	sub := &subscriber{ch: make(chan *topologypb.TopologyEnvelope, 4), done: make(chan struct{})}
	service.registerHeartbeat(heartbeat("stack-a", "node-a", "pod-a", "10.0.0.2", 1), sub)
	if service.EpochError() == nil {
		t.Fatal("expected epoch persistence error")
	}
	// drain anything from the heartbeat path; broadcastLocked already filtered
	// it because save failed, but the subscriber may still hold standby data.
	for {
		select {
		case <-sub.ch:
			continue
		default:
		}
		break
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go service.RunBookworm(ctx)
	go service.RunReaper(ctx)

	// Give bookworm/reaper several tick windows. Neither must enqueue a
	// degraded envelope.
	time.Sleep(50 * time.Millisecond)
	select {
	case env := <-sub.ch:
		t.Fatalf("bookworm/reaper must not broadcast while degraded, got %#v", env)
	default:
	}
}

func TestServiceClearsEpochPersistenceFailureAfterSuccess(t *testing.T) {
	store := &flakyEpochStore{err: errors.New("boom")}
	service := NewServiceWithConfig(Config{EpochStore: store})
	health := stackwatchdog.NewLeadershipHealth()
	service.AddLeadershipObserver(health)
	service.registerHeartbeat(heartbeat("stack-a", "node-a", "pod-a", "10.0.0.2", 1), &subscriber{ch: make(chan *topologypb.TopologyEnvelope, 1), done: make(chan struct{})})
	if service.EpochError() == nil {
		t.Fatal("expected epoch persistence error")
	}
	if got := healthStatus(t, health); got != "NOT_SERVING" {
		t.Fatalf("expected degraded health, got %s", got)
	}

	store.err = nil
	service.registerHeartbeat(heartbeat("stack-a", "node-a", "pod-a", "10.0.0.3", 2), &subscriber{ch: make(chan *topologypb.TopologyEnvelope, 1), done: make(chan struct{})})
	if service.EpochError() != nil {
		t.Fatalf("expected epoch persistence error to clear, got %v", service.EpochError())
	}
	if got := healthStatus(t, health); got != "SERVING" {
		t.Fatalf("expected recovered health, got %s", got)
	}
}

func TestServiceObserverAddedAfterEpochErrorSeesDegradedHealth(t *testing.T) {
	service := NewService()
	service.setEpochError(errors.New("boom"))

	health := stackwatchdog.NewLeadershipHealth()
	service.AddLeadershipObserver(health)

	if got := healthStatus(t, health); got != "NOT_SERVING" {
		t.Fatalf("expected degraded health for late observer, got %s", got)
	}
}

func TestServiceRetriesEpochLoadAfterFailure(t *testing.T) {
	store := &flakyEpochStore{loadErr: errors.New("boom"), epoch: 7}
	service := NewServiceWithConfig(Config{EpochStore: store})
	health := stackwatchdog.NewLeadershipHealth()
	service.AddLeadershipObserver(health)

	service.registerHeartbeat(heartbeat("stack-a", "node-a", "pod-a", "10.0.0.2", 1), &subscriber{ch: make(chan *topologypb.TopologyEnvelope, 1)})
	if service.EpochError() == nil {
		t.Fatal("expected epoch load failure")
	}
	if got := healthStatus(t, health); got != "NOT_SERVING" {
		t.Fatalf("expected degraded health after load failure, got %s", got)
	}
	if _, err := service.GetTopology(context.Background(), &topologypb.TopologyQuery{StackId: "stack-a"}); err == nil {
		t.Fatal("expected get topology to fail while epoch load is blocked")
	}

	store.loadErr = nil
	service.registerHeartbeat(heartbeat("stack-a", "node-b", "pod-b", "10.0.0.3", 2), &subscriber{ch: make(chan *topologypb.TopologyEnvelope, 1)})
	if service.EpochError() != nil {
		t.Fatalf("expected epoch load failure to clear, got %v", service.EpochError())
	}
	if got := healthStatus(t, health); got != "SERVING" {
		t.Fatalf("expected recovered health, got %s", got)
	}

	resp, err := service.GetTopology(context.Background(), &topologypb.TopologyQuery{StackId: "stack-a"})
	if err != nil {
		t.Fatalf("get topology: %v", err)
	}
	if resp.GetEpoch() < 7 {
		t.Fatalf("expected loaded epoch to be applied, got %d", resp.GetEpoch())
	}
	if len(resp.GetMembers()) != 1 || resp.GetMembers()[0].GetNodeId() != "node-b" {
		t.Fatalf("expected only recovered heartbeat member, got %v", resp.GetMembers())
	}
}

func TestServiceRegisterHeartbeatFailsClosedWhenEpochLoadFails(t *testing.T) {
	store := &flakyEpochStore{loadErr: errors.New("boom")}
	service := NewServiceWithConfig(Config{EpochStore: store})
	sub := &subscriber{ch: make(chan *topologypb.TopologyEnvelope, 1), done: make(chan struct{})}

	err := service.registerHeartbeat(heartbeat("stack-a", "node-a", "pod-a", "10.0.0.2", 1), sub)
	if err == nil {
		t.Fatal("expected register heartbeat to fail on epoch load error")
	}

	service.mu.Lock()
	defer service.mu.Unlock()
	if got := len(service.stateFor("stack-a").subscribers); got != 0 {
		t.Fatalf("expected no subscriber registration after epoch load failure, got %d", got)
	}
}

func TestServiceDemotionClosesSubscribers(t *testing.T) {
	service := NewServiceWithConfig(Config{Role: topologypb.WatchdogRole_WATCHDOG_ROLE_PRIMARY, Generation: 1})
	sub := &subscriber{ch: make(chan *topologypb.TopologyEnvelope, 1), done: make(chan struct{})}
	service.registerHeartbeat(heartbeat("stack-a", "node-a", "pod-a", "10.0.0.2", 1), sub)

	service.SetLeadership(topologypb.WatchdogRole_WATCHDOG_ROLE_STANDBY, 1)

	foundStandby := false
	for {
		select {
		case envelope := <-sub.ch:
			if envelope.GetWatchdogRole() == topologypb.WatchdogRole_WATCHDOG_ROLE_STANDBY {
				foundStandby = true
			}
		default:
			if !foundStandby {
				t.Fatal("expected standby envelope before subscriber close")
			}
			goto closed
		}
	}

closed:
	select {
	case <-sub.done:
	default:
		t.Fatal("expected subscriber to be closed on leadership demotion")
	}
	service.mu.Lock()
	defer service.mu.Unlock()
	if got := len(service.stateFor("stack-a").subscribers); got != 0 {
		t.Fatalf("expected subscribers to be cleared, got %d", got)
	}
}

func heartbeat(stackID, nodeID, podName, podIP string, incarnation int64) *topologypb.Heartbeat {
	return &topologypb.Heartbeat{
		StackId:         stackID,
		NodeId:          nodeID,
		PodName:         podName,
		PodIp:           podIP,
		Incarnation:     incarnation,
		ObservedEpoch:   0,
		State:           topologypb.NodeState_NODE_STATE_READY,
		ProtocolVersion: "v2",
	}
}

func healthStatus(t *testing.T, health *stackwatchdog.LeadershipHealth) string {
	t.Helper()

	server := grpc.NewServer()
	health.Register(server)
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer listener.Close()
	go func() {
		_ = server.Serve(listener)
	}()
	defer server.Stop()

	conn, err := grpc.Dial(listener.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()
	client := healthgrpc.NewHealthClient(conn)

	resp, err := client.Check(context.Background(), &healthgrpc.HealthCheckRequest{})
	if err != nil {
		t.Fatalf("health check: %v", err)
	}
	return resp.GetStatus().String()
}

type failingEpochStore struct {
	err error
}

func (s failingEpochStore) LoadEpoch(context.Context, string) (int64, error) {
	return 0, nil
}

func (s failingEpochStore) SaveEpoch(context.Context, string, int64) error {
	return s.err
}

type flakyEpochStore struct {
	err     error
	loadErr error
	epoch   int64
}

func (s *flakyEpochStore) LoadEpoch(context.Context, string) (int64, error) {
	if s.loadErr != nil {
		return 0, s.loadErr
	}
	return s.epoch, nil
}

func (s *flakyEpochStore) SaveEpoch(context.Context, string, int64) error {
	return s.err
}
