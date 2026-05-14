package topology

import (
	"context"
	"errors"
	"testing"
	"time"

	topologypb "github.com/zhixiongdu/olricstack/api/topology/v1"
	stackwatchdog "github.com/zhixiongdu/olricstack/internal/watchdog"
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
	service.registerHeartbeat(heartbeat("stack-a", "node-a", "pod-a", "10.0.0.2", 1), &subscriber{ch: make(chan *topologypb.TopologyEnvelope, 1)})

	if service.EpochError() == nil {
		t.Fatal("expected epoch persistence error")
	}
}

func TestServiceClearsEpochPersistenceFailureAfterSuccess(t *testing.T) {
	store := &flakyEpochStore{err: errors.New("boom")}
	service := NewServiceWithConfig(Config{EpochStore: store})
	service.registerHeartbeat(heartbeat("stack-a", "node-a", "pod-a", "10.0.0.2", 1), &subscriber{ch: make(chan *topologypb.TopologyEnvelope, 1), done: make(chan struct{})})
	if service.EpochError() == nil {
		t.Fatal("expected epoch persistence error")
	}

	store.err = nil
	service.registerHeartbeat(heartbeat("stack-a", "node-a", "pod-a", "10.0.0.3", 2), &subscriber{ch: make(chan *topologypb.TopologyEnvelope, 1), done: make(chan struct{})})
	if service.EpochError() != nil {
		t.Fatalf("expected epoch persistence error to clear, got %v", service.EpochError())
	}
}

func TestServiceDemotionClosesSubscribers(t *testing.T) {
	service := NewServiceWithConfig(Config{Role: topologypb.WatchdogRole_WATCHDOG_ROLE_PRIMARY, Generation: 1})
	sub := &subscriber{ch: make(chan *topologypb.TopologyEnvelope, 1), done: make(chan struct{})}
	service.registerHeartbeat(heartbeat("stack-a", "node-a", "pod-a", "10.0.0.2", 1), sub)

	service.SetLeadership(topologypb.WatchdogRole_WATCHDOG_ROLE_STANDBY, 1)

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
	err error
}

func (s *flakyEpochStore) LoadEpoch(context.Context, string) (int64, error) {
	return 0, nil
}

func (s *flakyEpochStore) SaveEpoch(context.Context, string, int64) error {
	return s.err
}
