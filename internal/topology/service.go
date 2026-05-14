package topology

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sync"
	"time"

	topologypb "github.com/zhixiongdu/olricstack/api/topology/v1"
	stackwatchdog "github.com/zhixiongdu/olricstack/internal/watchdog"
)

type Config struct {
	SuspectAfter     time.Duration
	ExpireAfter      time.Duration
	ReapInterval     time.Duration
	BookwormInterval time.Duration
	LeaseTTL         time.Duration
	WatchdogID       string
	Generation       int64
	Role             topologypb.WatchdogRole
	EpochStore       EpochStore
}

type EpochStore interface {
	LoadEpoch(ctx context.Context, stackID string) (int64, error)
	SaveEpoch(ctx context.Context, stackID string, epoch int64) error
}

func (c Config) withDefaults() Config {
	if c.SuspectAfter <= 0 {
		c.SuspectAfter = 20 * time.Second
	}
	if c.ExpireAfter <= 0 {
		c.ExpireAfter = 30 * time.Second
	}
	if c.ReapInterval <= 0 {
		c.ReapInterval = c.SuspectAfter / 2
	}
	if c.ReapInterval <= 0 {
		c.ReapInterval = time.Second
	}
	if c.BookwormInterval <= 0 {
		c.BookwormInterval = 10 * time.Second
	}
	if c.LeaseTTL <= 0 {
		c.LeaseTTL = c.ExpireAfter
	}
	if c.WatchdogID == "" {
		c.WatchdogID = "watchdog"
	}
	if c.Generation <= 0 {
		c.Generation = time.Now().UnixNano()
	}
	if c.Role == topologypb.WatchdogRole_WATCHDOG_ROLE_UNSPECIFIED {
		c.Role = topologypb.WatchdogRole_WATCHDOG_ROLE_PRIMARY
	}
	return c
}

type Service struct {
	topologypb.UnimplementedTopologyControlServer

	cfgMu sync.RWMutex
	cfg   Config

	mu     sync.RWMutex
	stacks map[string]*stackState

	epochErrMu sync.RWMutex
	epochErr   error
}

type stackState struct {
	cluster     *stackwatchdog.ClusterState
	subscribers map[string]*subscriber
	loadedEpoch bool
}

type subscriber struct {
	nodeID string
	ch     chan *topologypb.TopologyEnvelope
	done   chan struct{}
}

func NewService() *Service {
	return NewServiceWithConfig(Config{})
}

func NewServiceWithConfig(cfg Config) *Service {
	return &Service{
		cfg:    cfg.withDefaults(),
		stacks: make(map[string]*stackState),
	}
}

func (s *Service) RunReaper(ctx context.Context) {
	cfg := s.config()
	ticker := time.NewTicker(cfg.ReapInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case now := <-ticker.C:
			s.pruneExpired(now)
		}
	}
}

func (s *Service) RunBookworm(ctx context.Context) {
	cfg := s.config()
	ticker := time.NewTicker(cfg.BookwormInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.broadcastSnapshots(topologypb.TopologyReason_TOPOLOGY_REASON_BOOKWORM)
		}
	}
}

func (s *Service) ObservePods(stackID string, observations []stackwatchdog.PodObservation) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if !s.isPrimary() {
		return
	}
	state := s.stateFor(stackID)
	s.loadEpochLocked(context.Background(), stackID, state)
	cfg := s.config()
	if state.cluster.ApplyPodObservations(observations, cfg.ExpireAfter, time.Now()) {
		s.broadcastLocked(stackID, state, topologypb.TopologyReason_TOPOLOGY_REASON_RECONCILE)
	}
}

func (s *Service) GetTopology(ctx context.Context, req *topologypb.TopologyQuery) (*topologypb.TopologyEnvelope, error) {
	if req.GetStackId() == "" {
		return nil, errors.New("stack_id is required")
	}
	if !s.isPrimary() {
		return s.standbyEnvelope(req.GetStackId()), nil
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	state := s.stateFor(req.GetStackId())
	s.loadEpochLocked(ctx, req.GetStackId(), state)
	s.pruneExpiredLocked(req.GetStackId(), state, time.Now())
	return s.envelopeFor(req.GetStackId(), state, topologypb.TopologyReason_TOPOLOGY_REASON_BOOKWORM), nil
}

func (s *Service) Watch(stream topologypb.TopologyControl_WatchServer) error {
	first, err := stream.Recv()
	if err != nil {
		return err
	}
	if err := validateHeartbeat(first); err != nil {
		return err
	}
	if !s.isPrimary() {
		return stream.Send(s.standbyEnvelope(first.GetStackId()))
	}

	sub := &subscriber{
		nodeID: first.GetNodeId(),
		ch:     make(chan *topologypb.TopologyEnvelope, 8),
		done:   make(chan struct{}),
	}
	s.registerHeartbeat(first, sub)
	defer s.unregister(first.GetStackId(), first.GetNodeId(), sub)

	ctx, cancel := context.WithCancel(stream.Context())
	defer cancel()

	errCh := make(chan error, 1)
	go s.receiveHeartbeats(ctx, first.GetStackId(), first.GetNodeId(), first.GetIncarnation(), sub, stream, errCh)

	for {
		select {
		case <-stream.Context().Done():
			return stream.Context().Err()
		case err := <-errCh:
			return err
		case <-sub.done:
			return errors.New("subscriber replaced")
		case envelope := <-sub.ch:
			if err := stream.Send(envelope); err != nil {
				return err
			}
		}
	}
}

func (s *Service) receiveHeartbeats(ctx context.Context, stackID, nodeID string, incarnation int64, sub *subscriber, stream topologypb.TopologyControl_WatchServer, errCh chan<- error) {
	for {
		heartbeat, err := stream.Recv()
		if err != nil {
			errCh <- err
			return
		}
		if heartbeat.GetStackId() != stackID || heartbeat.GetNodeId() != nodeID || heartbeat.GetIncarnation() != incarnation {
			errCh <- errors.New("heartbeat identity changed")
			return
		}
		if err := validateHeartbeat(heartbeat); err != nil {
			errCh <- err
			return
		}
		s.registerHeartbeat(heartbeat, sub)

		select {
		case <-ctx.Done():
			return
		default:
		}
	}
}

func (s *Service) registerHeartbeat(heartbeat *topologypb.Heartbeat, sub *subscriber) {
	s.mu.Lock()
	defer s.mu.Unlock()

	now := time.Now()
	state := s.stateFor(heartbeat.GetStackId())
	s.loadEpochLocked(context.Background(), heartbeat.GetStackId(), state)
	s.pruneExpiredLocked(heartbeat.GetStackId(), state, now)

	result := state.cluster.ApplyHeartbeat(stackwatchdog.HeartbeatObservation{
		NodeID:          heartbeat.GetNodeId(),
		PodName:         heartbeat.GetPodName(),
		PodIP:           heartbeat.GetPodIp(),
		Incarnation:     heartbeat.GetIncarnation(),
		ReportedState:   heartbeat.GetState(),
		ProtocolVersion: heartbeat.GetProtocolVersion(),
		SeenAt:          now,
	})
	if result.Stale {
		return
	}
	if current := state.subscribers[heartbeat.GetNodeId()]; current != nil && current != sub {
		closeSubscriber(current)
	}
	state.subscribers[heartbeat.GetNodeId()] = sub

	if result.Changed {
		s.broadcastLocked(heartbeat.GetStackId(), state, topologypb.TopologyReason_TOPOLOGY_REASON_EVENT)
		return
	}
	if heartbeat.GetObservedEpoch() < state.cluster.Epoch() {
		s.sendLatestLocked(heartbeat.GetStackId(), state, sub, topologypb.TopologyReason_TOPOLOGY_REASON_BOOKWORM)
	}
}

func (s *Service) unregister(stackID, nodeID string, sub *subscriber) {
	s.mu.Lock()
	defer s.mu.Unlock()

	state := s.stateFor(stackID)
	if current := state.subscribers[nodeID]; current == sub {
		delete(state.subscribers, nodeID)
	}
}

func (s *Service) pruneExpired(now time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if !s.isPrimary() {
		return
	}
	for stackID, state := range s.stacks {
		s.pruneExpiredLocked(stackID, state, now)
	}
}

func (s *Service) broadcastSnapshots(reason topologypb.TopologyReason) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if !s.isPrimary() {
		return
	}
	for stackID, state := range s.stacks {
		s.broadcastLocked(stackID, state, reason)
	}
}

func (s *Service) stateFor(stackID string) *stackState {
	state, exists := s.stacks[stackID]
	if !exists {
		state = &stackState{
			cluster:     stackwatchdog.NewClusterState(),
			subscribers: make(map[string]*subscriber),
		}
		s.stacks[stackID] = state
	}
	return state
}

func (s *Service) pruneExpiredLocked(stackID string, state *stackState, now time.Time) {
	cfg := s.config()
	if !state.cluster.PruneExpired(cfg.SuspectAfter, cfg.ExpireAfter, now) {
		return
	}

	members, _ := state.cluster.Members(cfg.ExpireAfter, now)
	active := make(map[string]struct{}, len(members))
	for _, member := range members {
		active[member.GetNodeId()] = struct{}{}
	}
	for nodeID := range state.subscribers {
		if _, ok := active[nodeID]; !ok {
			delete(state.subscribers, nodeID)
		}
	}
	s.broadcastLocked(stackID, state, topologypb.TopologyReason_TOPOLOGY_REASON_PRUNE)
}

func (s *Service) broadcastLocked(stackID string, state *stackState, reason topologypb.TopologyReason) {
	envelope := s.envelopeFor(stackID, state, reason)
	s.saveEpochLocked(context.Background(), stackID, envelope.GetEpoch())
	for _, sub := range state.subscribers {
		select {
		case sub.ch <- cloneEnvelope(envelope):
		default:
		}
	}
}

func (s *Service) sendLatestLocked(stackID string, state *stackState, sub *subscriber, reason topologypb.TopologyReason) {
	envelope := s.envelopeFor(stackID, state, reason)
	select {
	case sub.ch <- envelope:
	default:
	}
}

func (s *Service) envelopeFor(stackID string, state *stackState, reason topologypb.TopologyReason) *topologypb.TopologyEnvelope {
	cfg := s.config()
	now := time.Now()
	members, epoch := state.cluster.Members(cfg.ExpireAfter, now)
	return &topologypb.TopologyEnvelope{
		StackId:            stackID,
		Members:            members,
		Epoch:              epoch,
		Reason:             reason,
		ValidUntilUnixMs:   now.Add(cfg.LeaseTTL).UnixMilli(),
		WatchdogId:         cfg.WatchdogID,
		WatchdogGeneration: cfg.Generation,
		WatchdogRole:       cfg.Role,
	}
}

func (s *Service) standbyEnvelope(stackID string) *topologypb.TopologyEnvelope {
	cfg := s.config()
	return &topologypb.TopologyEnvelope{
		StackId:            stackID,
		Reason:             topologypb.TopologyReason_TOPOLOGY_REASON_PRIMARY_CHANGE,
		WatchdogId:         cfg.WatchdogID,
		WatchdogGeneration: cfg.Generation,
		WatchdogRole:       cfg.Role,
	}
}

func cloneEnvelope(envelope *topologypb.TopologyEnvelope) *topologypb.TopologyEnvelope {
	out := *envelope
	out.Members = append([]*topologypb.Member(nil), envelope.GetMembers()...)
	return &out
}

func closeSubscriber(sub *subscriber) {
	if sub.done == nil {
		return
	}
	close(sub.done)
}

func validateHeartbeat(heartbeat *topologypb.Heartbeat) error {
	if heartbeat.GetStackId() == "" {
		return errors.New("stack_id is required")
	}
	if heartbeat.GetNodeId() == "" {
		return errors.New("node_id is required")
	}
	if heartbeat.GetPodIp() == "" {
		return errors.New("pod_ip is required")
	}
	if heartbeat.GetIncarnation() <= 0 {
		return errors.New("incarnation is required")
	}
	return nil
}

func (s *Service) isPrimary() bool {
	return s.config().Role == topologypb.WatchdogRole_WATCHDOG_ROLE_PRIMARY
}

func (s *Service) SetLeadership(role topologypb.WatchdogRole, generation int64) {
	var demoted bool
	s.cfgMu.Lock()
	if s.cfg.Role == topologypb.WatchdogRole_WATCHDOG_ROLE_PRIMARY && role != topologypb.WatchdogRole_WATCHDOG_ROLE_PRIMARY {
		demoted = true
	}
	s.cfg.Role = role
	if generation > 0 {
		s.cfg.Generation = generation
	}
	s.cfgMu.Unlock()

	if demoted {
		s.closeSubscribersForLeadershipChange()
	}
}

func (s *Service) closeSubscribersForLeadershipChange() {
	s.mu.Lock()
	defer s.mu.Unlock()

	for _, state := range s.stacks {
		for nodeID, sub := range state.subscribers {
			closeSubscriber(sub)
			delete(state.subscribers, nodeID)
		}
	}
}

func (s *Service) SetEpochStore(store EpochStore) {
	s.cfgMu.Lock()
	defer s.cfgMu.Unlock()
	s.cfg.EpochStore = store
}

func (s *Service) config() Config {
	s.cfgMu.RLock()
	defer s.cfgMu.RUnlock()
	return s.cfg
}

func (s *Service) loadEpochLocked(ctx context.Context, stackID string, state *stackState) {
	if state.loadedEpoch {
		return
	}
	state.loadedEpoch = true
	store := s.config().EpochStore
	if store == nil {
		return
	}
	epoch, err := store.LoadEpoch(ctx, stackID)
	if err != nil {
		return
	}
	state.cluster.BumpEpochAtLeast(epoch)
}

func (s *Service) saveEpochLocked(ctx context.Context, stackID string, epoch int64) {
	store := s.config().EpochStore
	if store == nil {
		s.setEpochError(nil)
		return
	}
	if err := store.SaveEpoch(ctx, stackID, epoch); err != nil {
		wrapped := fmt.Errorf("save topology epoch: %w", err)
		s.setEpochError(wrapped)
		log.Printf("watchdog topology epoch persistence failed for stack %q epoch %d: %v", stackID, epoch, err)
		return
	}
	s.setEpochError(nil)
}

func (s *Service) EpochError() error {
	s.epochErrMu.RLock()
	defer s.epochErrMu.RUnlock()
	return s.epochErr
}

func (s *Service) setEpochError(err error) {
	s.epochErrMu.Lock()
	defer s.epochErrMu.Unlock()
	s.epochErr = err
}
