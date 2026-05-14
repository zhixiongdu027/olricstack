package watchdog

import (
	"testing"
	"time"

	topologypb "github.com/zhixiongdu/olricstack/api/topology/v1"
)

func TestClusterStateHeartbeatIsPrimarySource(t *testing.T) {
	state := NewClusterState()
	now := time.Now()

	changed := state.ApplyPodObservations([]PodObservation{{
		PodName: "pod-a",
		PodIP:   "10.0.0.2",
		Ready:   true,
		Phase:   "Running",
		SeenAt:  now,
	}}, time.Minute, now)
	if changed {
		t.Fatal("pod observation alone must not create topology membership")
	}
	members, _ := state.Members(time.Minute, now)
	if len(members) != 0 {
		t.Fatalf("expected empty topology from pod-only observation, got %v", members)
	}

	state.ApplyHeartbeat(HeartbeatObservation{
		NodeID:      "node-a",
		PodName:     "pod-a",
		PodIP:       "10.0.0.2",
		Incarnation: 1,
		SeenAt:      now,
	})
	members, epoch := state.Members(time.Minute, now)
	if epoch != 1 {
		t.Fatalf("expected epoch 1, got %d", epoch)
	}
	if len(members) != 1 || members[0].GetNodeId() != "node-a" {
		t.Fatalf("unexpected members %v", members)
	}
}

func TestClusterStateIgnoresStaleIncarnation(t *testing.T) {
	state := NewClusterState()
	now := time.Now()
	state.ApplyHeartbeat(HeartbeatObservation{NodeID: "node-a", PodName: "pod-a", PodIP: "10.0.0.2", Incarnation: 2, SeenAt: now})

	result := state.ApplyHeartbeat(HeartbeatObservation{NodeID: "node-a", PodName: "pod-old", PodIP: "10.0.0.9", Incarnation: 1, SeenAt: now})
	if !result.Stale {
		t.Fatal("expected stale incarnation")
	}
	if result.Changed {
		t.Fatal("stale incarnation should not change state")
	}
	members, epoch := state.Members(time.Minute, now)
	if epoch != 1 {
		t.Fatalf("expected epoch 1, got %d", epoch)
	}
	if members[0].GetPodIp() != "10.0.0.2" {
		t.Fatalf("stale heartbeat overwrote pod ip: %v", members[0])
	}
}

func TestClusterStatePodObservationCanExcludeTerminalNode(t *testing.T) {
	state := NewClusterState()
	now := time.Now()
	state.ApplyHeartbeat(HeartbeatObservation{NodeID: "node-a", PodName: "pod-a", PodIP: "10.0.0.2", Incarnation: 1, SeenAt: now})

	state.ApplyPodObservations([]PodObservation{{
		PodName: "pod-a",
		PodIP:   "10.0.0.2",
		Ready:   false,
		Phase:   "Failed",
		SeenAt:  now,
	}}, time.Minute, now)

	members, epoch := state.Members(time.Minute, now)
	if len(members) != 0 {
		t.Fatalf("expected terminal pod excluded, got %v", members)
	}
	if epoch != 2 {
		t.Fatalf("expected epoch 2 after pod state change, got %d", epoch)
	}
}

func TestClusterStatePrunesHeartbeatExpiredNode(t *testing.T) {
	state := NewClusterState()
	now := time.Now()
	state.ApplyHeartbeat(HeartbeatObservation{NodeID: "node-a", PodName: "pod-a", PodIP: "10.0.0.2", Incarnation: 1, SeenAt: now})

	changed := state.PruneExpired(time.Second, 2*time.Second, now.Add(3*time.Second))
	if !changed {
		t.Fatal("expected expired heartbeat to change state")
	}
	members, epoch := state.Members(time.Second, now.Add(3*time.Second))
	if len(members) != 0 {
		t.Fatalf("expected empty topology after prune, got %v", members)
	}
	if epoch != 2 {
		t.Fatalf("expected epoch 2 after prune, got %d", epoch)
	}
}

func TestClusterStateMarksSuspectBeforePrune(t *testing.T) {
	state := NewClusterState()
	now := time.Now()
	state.ApplyHeartbeat(HeartbeatObservation{NodeID: "node-a", PodName: "pod-a", PodIP: "10.0.0.2", Incarnation: 1, SeenAt: now})

	changed := state.PruneExpired(time.Second, 5*time.Second, now.Add(2*time.Second))
	if !changed {
		t.Fatal("expected suspect transition")
	}
	members, _ := state.Members(5*time.Second, now.Add(2*time.Second))
	if len(members) != 1 || members[0].GetState() != topologypb.NodeState_NODE_STATE_SUSPECT {
		t.Fatalf("expected suspect member, got %v", members)
	}
}

func TestClusterStateHeartbeatRecoversSuspectNode(t *testing.T) {
	state := NewClusterState()
	now := time.Now()
	state.ApplyHeartbeat(HeartbeatObservation{NodeID: "node-a", PodName: "pod-a", PodIP: "10.0.0.2", Incarnation: 1, SeenAt: now})
	state.PruneExpired(time.Second, 5*time.Second, now.Add(2*time.Second))

	result := state.ApplyHeartbeat(HeartbeatObservation{NodeID: "node-a", PodName: "pod-a", PodIP: "10.0.0.2", Incarnation: 1, SeenAt: now.Add(3 * time.Second)})
	if !result.Changed {
		t.Fatal("expected heartbeat to recover suspect node")
	}
	members, epoch := state.Members(5*time.Second, now.Add(3*time.Second))
	if epoch != 3 {
		t.Fatalf("expected epoch 3 after suspect recovery, got %d", epoch)
	}
	if len(members) != 1 || members[0].GetState() != topologypb.NodeState_NODE_STATE_READY {
		t.Fatalf("expected recovered ready member, got %v", members)
	}
}

func TestClusterStateDrainingHeartbeatDoesNotReturnToReady(t *testing.T) {
	state := NewClusterState()
	now := time.Now()
	state.ApplyHeartbeat(HeartbeatObservation{NodeID: "node-a", PodName: "pod-a", PodIP: "10.0.0.2", Incarnation: 1, ReportedState: topologypb.NodeState_NODE_STATE_DRAINING, SeenAt: now})

	result := state.ApplyHeartbeat(HeartbeatObservation{NodeID: "node-a", PodName: "pod-a", PodIP: "10.0.0.2", Incarnation: 1, ReportedState: topologypb.NodeState_NODE_STATE_READY, SeenAt: now.Add(time.Second)})
	if result.Changed {
		t.Fatal("ready heartbeat must not cancel an explicit drain workflow")
	}
	members, epoch := state.Members(time.Minute, now.Add(time.Second))
	if epoch != 1 {
		t.Fatalf("expected epoch to remain 1, got %d", epoch)
	}
	if len(members) != 1 || members[0].GetState() != topologypb.NodeState_NODE_STATE_DRAINING {
		t.Fatalf("expected draining member, got %v", members)
	}
}

func TestClusterStateTerminalPodCannotBeRevivedBySameIncarnation(t *testing.T) {
	state := NewClusterState()
	now := time.Now()
	state.ApplyHeartbeat(HeartbeatObservation{NodeID: "node-a", PodName: "pod-a", PodIP: "10.0.0.2", Incarnation: 1, SeenAt: now})
	state.ApplyPodObservations([]PodObservation{{
		PodName: "pod-a",
		PodIP:   "10.0.0.2",
		Ready:   false,
		Phase:   "Failed",
		SeenAt:  now.Add(time.Second),
	}}, time.Minute, now.Add(time.Second))

	result := state.ApplyHeartbeat(HeartbeatObservation{NodeID: "node-a", PodName: "pod-a", PodIP: "10.0.0.2", Incarnation: 1, SeenAt: now.Add(2 * time.Second)})
	if result.Changed {
		t.Fatal("terminal pod state must fence same-incarnation heartbeat")
	}
	members, epoch := state.Members(time.Minute, now.Add(2*time.Second))
	if epoch != 2 {
		t.Fatalf("expected epoch to remain 2, got %d", epoch)
	}
	if len(members) != 0 {
		t.Fatalf("terminal node should remain excluded, got %v", members)
	}
}

func TestClusterStateNewIncarnationReplacesTerminalPod(t *testing.T) {
	state := NewClusterState()
	now := time.Now()
	state.ApplyHeartbeat(HeartbeatObservation{NodeID: "node-a", PodName: "pod-a", PodIP: "10.0.0.2", Incarnation: 1, SeenAt: now})
	state.ApplyPodObservations([]PodObservation{{
		PodName: "pod-a",
		PodIP:   "10.0.0.2",
		Ready:   false,
		Phase:   "Failed",
		SeenAt:  now.Add(time.Second),
	}}, time.Minute, now.Add(time.Second))

	result := state.ApplyHeartbeat(HeartbeatObservation{NodeID: "node-a", PodName: "pod-b", PodIP: "10.0.0.3", Incarnation: 2, SeenAt: now.Add(2 * time.Second)})
	if !result.Changed {
		t.Fatal("new incarnation should replace terminal pod state")
	}
	members, epoch := state.Members(time.Minute, now.Add(2*time.Second))
	if epoch != 3 {
		t.Fatalf("expected epoch 3 after replacement, got %d", epoch)
	}
	if len(members) != 1 || members[0].GetState() != topologypb.NodeState_NODE_STATE_READY || members[0].GetPodIp() != "10.0.0.3" {
		t.Fatalf("expected new ready incarnation, got %v", members)
	}
}

func TestClusterStatePodObservationPrefersPodNameOverReusedIP(t *testing.T) {
	state := NewClusterState()
	now := time.Now()
	state.ApplyHeartbeat(HeartbeatObservation{NodeID: "node-a", PodName: "pod-a", PodIP: "10.0.0.2", Incarnation: 1, SeenAt: now})

	changed := state.ApplyPodObservations([]PodObservation{{
		PodName: "pod-b",
		PodIP:   "10.0.0.2",
		Ready:   false,
		Phase:   "Failed",
		SeenAt:  now.Add(time.Second),
	}}, time.Minute, now.Add(time.Second))
	if changed {
		t.Fatal("reused IP from another pod must not mark existing pod terminal")
	}

	members, epoch := state.Members(time.Minute, now.Add(time.Second))
	if epoch != 1 {
		t.Fatalf("expected epoch to remain 1, got %d", epoch)
	}
	if len(members) != 1 || members[0].GetState() != topologypb.NodeState_NODE_STATE_READY {
		t.Fatalf("expected original ready member to remain, got %v", members)
	}
}
