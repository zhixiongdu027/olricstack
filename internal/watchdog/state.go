package watchdog

import (
	"context"
	"sort"
	"time"

	"github.com/looplab/fsm"
	topologypb "github.com/zhixiongdu/olricstack/api/topology/v1"
)

const (
	nodeEventHeartbeat = "heartbeat"
	nodeEventSuspect   = "suspect"
	nodeEventExpire    = "expire"
	nodeEventTerminal  = "terminal"
	nodeEventDrain     = "drain"
	nodeEventLeave     = "leave"
	nodeEventRemove    = "remove"
)

type PodObservation struct {
	PodName string
	PodIP   string
	Ready   bool
	Phase   string
	SeenAt  time.Time
}

type HeartbeatObservation struct {
	NodeID          string
	PodName         string
	PodIP           string
	Incarnation     int64
	ReportedState   topologypb.NodeState
	ProtocolVersion string
	SeenAt          time.Time
}

type HeartbeatResult struct {
	Changed bool
	Stale   bool
}

type ClusterState struct {
	nodes map[string]*NodeState
	epoch int64
}

type NodeState struct {
	NodeID          string
	PodName         string
	PodIP           string
	Incarnation     int64
	State           topologypb.NodeState
	ProtocolVersion string
	LastHeartbeat   time.Time
	LastPodSeen     time.Time
	PodReady        bool
	PodPhase        string
	lifecycle       *fsm.FSM
}

func NewClusterState() *ClusterState {
	return &ClusterState{nodes: make(map[string]*NodeState)}
}

func (s *ClusterState) ApplyHeartbeat(obs HeartbeatObservation) HeartbeatResult {
	if obs.SeenAt.IsZero() {
		obs.SeenAt = time.Now()
	}
	if obs.ReportedState == topologypb.NodeState_NODE_STATE_UNSPECIFIED {
		obs.ReportedState = topologypb.NodeState_NODE_STATE_READY
	}

	node, exists := s.nodes[obs.NodeID]
	if !exists {
		node = &NodeState{
			NodeID:          obs.NodeID,
			PodName:         obs.PodName,
			PodIP:           obs.PodIP,
			Incarnation:     obs.Incarnation,
			ProtocolVersion: obs.ProtocolVersion,
			LastHeartbeat:   obs.SeenAt,
		}
		node.setLifecycle(obs.ReportedState)
		s.nodes[obs.NodeID] = node
		s.epoch++
		return HeartbeatResult{Changed: true}
	}

	if obs.Incarnation < node.Incarnation {
		return HeartbeatResult{Stale: true}
	}

	changed := obs.Incarnation > node.Incarnation ||
		node.PodName != obs.PodName ||
		node.PodIP != obs.PodIP ||
		node.ProtocolVersion != obs.ProtocolVersion
	newIncarnation := obs.Incarnation > node.Incarnation
	previousState := node.State

	node.PodName = obs.PodName
	node.PodIP = obs.PodIP
	node.Incarnation = obs.Incarnation
	node.ProtocolVersion = obs.ProtocolVersion
	node.LastHeartbeat = obs.SeenAt

	transitioned := false
	if newIncarnation {
		node.setLifecycle(obs.ReportedState)
		transitioned = node.State != previousState
	} else {
		transitioned = node.applyHeartbeatState(obs.ReportedState)
	}
	if changed || transitioned {
		s.epoch++
	}
	return HeartbeatResult{Changed: changed || transitioned}
}

func (s *ClusterState) ApplyPodObservations(observations []PodObservation, expireAfter time.Duration, now time.Time) bool {
	if now.IsZero() {
		now = time.Now()
	}

	observedByIP := make(map[string]PodObservation, len(observations))
	for _, obs := range observations {
		if obs.PodIP == "" {
			continue
		}
		if obs.SeenAt.IsZero() {
			obs.SeenAt = now
		}
		observedByIP[obs.PodIP] = obs
	}

	var changed bool
	for _, node := range s.nodes {
		obs, exists := observedByIP[node.PodIP]
		if !exists {
			if now.Sub(node.LastHeartbeat) > expireAfter {
				delete(s.nodes, node.NodeID)
				changed = true
			}
			continue
		}
		if obs.Phase == "Failed" || obs.Phase == "Succeeded" {
			if node.transition(nodeEventTerminal) {
				changed = true
			}
		}
		if node.PodReady != obs.Ready || node.PodPhase != obs.Phase || node.PodName != obs.PodName {
			changed = true
		}
		node.PodName = obs.PodName
		node.PodReady = obs.Ready
		node.PodPhase = obs.Phase
		node.LastPodSeen = obs.SeenAt
	}

	if changed {
		s.epoch++
	}
	return changed
}

func (s *ClusterState) PruneExpired(suspectAfter, expireAfter time.Duration, now time.Time) bool {
	if now.IsZero() {
		now = time.Now()
	}

	var changed bool
	for nodeID, node := range s.nodes {
		sinceHeartbeat := now.Sub(node.LastHeartbeat)
		switch {
		case sinceHeartbeat > expireAfter:
			if node.transition(nodeEventExpire) {
				changed = true
			}
			if node.transition(nodeEventRemove) {
				changed = true
			}
			if node.State == topologypb.NodeState_NODE_STATE_REMOVED {
				delete(s.nodes, nodeID)
			}
		case sinceHeartbeat > suspectAfter:
			if node.transition(nodeEventSuspect) {
				changed = true
			}
		}
	}
	if changed {
		s.epoch++
	}
	return changed
}

func (s *ClusterState) Members(expireAfter time.Duration, now time.Time) ([]*topologypb.Member, int64) {
	if now.IsZero() {
		now = time.Now()
	}

	members := make([]*topologypb.Member, 0, len(s.nodes))
	for _, node := range s.nodes {
		if now.Sub(node.LastHeartbeat) > expireAfter {
			continue
		}
		if node.State == topologypb.NodeState_NODE_STATE_REMOVED ||
			node.State == topologypb.NodeState_NODE_STATE_UNREACHABLE {
			continue
		}
		members = append(members, &topologypb.Member{
			NodeId:         node.NodeID,
			PodName:        node.PodName,
			PodIp:          node.PodIP,
			Incarnation:    node.Incarnation,
			State:          node.State,
			LastSeenUnixMs: node.LastHeartbeat.UnixMilli(),
		})
	}
	sort.Slice(members, func(i, j int) bool {
		return members[i].GetNodeId() < members[j].GetNodeId()
	})
	return members, s.epoch
}

func (s *ClusterState) Epoch() int64 {
	return s.epoch
}

func (s *ClusterState) BumpEpochAtLeast(epoch int64) {
	if epoch > s.epoch {
		s.epoch = epoch
	}
}

func (n *NodeState) setLifecycle(state topologypb.NodeState) {
	if state == topologypb.NodeState_NODE_STATE_UNSPECIFIED {
		state = topologypb.NodeState_NODE_STATE_READY
	}
	n.State = state
	n.lifecycle = newNodeLifecycleFSM(state)
}

func (n *NodeState) ensureLifecycle() {
	if n.lifecycle == nil {
		n.setLifecycle(n.State)
	}
}

func (n *NodeState) applyHeartbeatState(state topologypb.NodeState) bool {
	n.ensureLifecycle()
	if state == topologypb.NodeState_NODE_STATE_UNSPECIFIED {
		state = topologypb.NodeState_NODE_STATE_READY
	}
	switch state {
	case topologypb.NodeState_NODE_STATE_DRAINING:
		return n.transition(nodeEventDrain)
	case topologypb.NodeState_NODE_STATE_LEAVING:
		return n.transition(nodeEventLeave)
	case topologypb.NodeState_NODE_STATE_REMOVED:
		return n.transition(nodeEventRemove)
	default:
		return n.transition(nodeEventHeartbeat)
	}
}

func (n *NodeState) transition(event string) bool {
	n.ensureLifecycle()
	previous := n.State
	if err := n.lifecycle.Event(context.Background(), event); err != nil {
		return false
	}
	n.State = nodeStateFromString(n.lifecycle.Current())
	return n.State != previous
}

func newNodeLifecycleFSM(initial topologypb.NodeState) *fsm.FSM {
	ready := nodeStateString(topologypb.NodeState_NODE_STATE_READY)
	joining := nodeStateString(topologypb.NodeState_NODE_STATE_JOINING)
	suspect := nodeStateString(topologypb.NodeState_NODE_STATE_SUSPECT)
	unreachable := nodeStateString(topologypb.NodeState_NODE_STATE_UNREACHABLE)
	draining := nodeStateString(topologypb.NodeState_NODE_STATE_DRAINING)
	leaving := nodeStateString(topologypb.NodeState_NODE_STATE_LEAVING)
	removed := nodeStateString(topologypb.NodeState_NODE_STATE_REMOVED)

	return fsm.NewFSM(nodeStateString(initial), fsm.Events{
		{Name: nodeEventHeartbeat, Src: []string{joining, ready, suspect, unreachable}, Dst: ready},
		{Name: nodeEventHeartbeat, Src: []string{draining}, Dst: draining},
		{Name: nodeEventHeartbeat, Src: []string{leaving}, Dst: leaving},
		{Name: nodeEventHeartbeat, Src: []string{removed}, Dst: removed},
		{Name: nodeEventSuspect, Src: []string{joining, ready}, Dst: suspect},
		{Name: nodeEventExpire, Src: []string{joining, ready, suspect, draining, leaving}, Dst: unreachable},
		{Name: nodeEventTerminal, Src: []string{joining, ready, suspect, unreachable, draining, leaving}, Dst: removed},
		{Name: nodeEventDrain, Src: []string{joining, ready, suspect}, Dst: draining},
		{Name: nodeEventLeave, Src: []string{ready, suspect, draining}, Dst: leaving},
		{Name: nodeEventRemove, Src: []string{unreachable, leaving, removed}, Dst: removed},
	}, nil)
}

func nodeStateString(state topologypb.NodeState) string {
	if state == topologypb.NodeState_NODE_STATE_UNSPECIFIED {
		state = topologypb.NodeState_NODE_STATE_READY
	}
	return state.String()
}

func nodeStateFromString(state string) topologypb.NodeState {
	if value, ok := topologypb.NodeState_value[state]; ok {
		return topologypb.NodeState(value)
	}
	return topologypb.NodeState_NODE_STATE_UNSPECIFIED
}
