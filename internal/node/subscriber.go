package node

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	topologypb "github.com/zhixiongdu/olricstack/api/topology/v1"
)

var (
	ErrTopologyLeaseExpired    = errors.New("topology lease is expired")
	ErrTopologyNotPrimary      = errors.New("topology envelope is not from primary watchdog")
	ErrStaleTopologyEpoch      = errors.New("stale topology epoch")
	ErrStaleWatchdogGeneration = errors.New("stale watchdog generation")
)

type Joiner interface {
	Join(ctx context.Context, envelope *topologypb.TopologyEnvelope) error
}

type SubscriberConfig struct {
	StackID           string
	NodeID            string
	PodName           string
	PodIP             string
	Incarnation       int64
	ProtocolVersion   string
	HeartbeatInterval time.Duration
	Lease             *LeaseTracker
	Joiner            Joiner
}

func (c SubscriberConfig) withDefaults() SubscriberConfig {
	if c.HeartbeatInterval <= 0 {
		c.HeartbeatInterval = 10 * time.Second
	}
	if c.ProtocolVersion == "" {
		c.ProtocolVersion = "v2"
	}
	if c.Incarnation <= 0 {
		c.Incarnation = time.Now().UnixNano()
	}
	return c
}

type Subscriber struct {
	cfg   SubscriberConfig
	lease *LeaseTracker
}

func NewSubscriber(cfg SubscriberConfig) (*Subscriber, error) {
	cfg = cfg.withDefaults()
	if cfg.StackID == "" {
		return nil, errors.New("stack id is required")
	}
	if cfg.NodeID == "" {
		return nil, errors.New("node id is required")
	}
	if cfg.PodIP == "" {
		return nil, errors.New("pod ip is required")
	}
	lease := cfg.Lease
	if lease == nil {
		lease = NewLeaseTracker()
	}
	return &Subscriber{cfg: cfg, lease: lease}, nil
}

func (s *Subscriber) Run(ctx context.Context, client topologypb.TopologyControlClient) error {
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	stream, err := client.Watch(runCtx)
	if err != nil {
		return err
	}
	defer func() {
		_ = stream.CloseSend()
	}()

	sendHeartbeat := func(observedEpoch int64) error {
		return stream.Send(&topologypb.Heartbeat{
			StackId:         s.cfg.StackID,
			NodeId:          s.cfg.NodeID,
			PodName:         s.cfg.PodName,
			PodIp:           s.cfg.PodIP,
			Incarnation:     s.cfg.Incarnation,
			ObservedEpoch:   observedEpoch,
			State:           topologypb.NodeState_NODE_STATE_READY,
			ProtocolVersion: s.cfg.ProtocolVersion,
		})
	}

	var observedEpoch atomic.Int64
	if err := sendHeartbeat(observedEpoch.Load()); err != nil {
		return err
	}

	errCh := make(chan error, 1)
	go func() {
		ticker := time.NewTicker(s.cfg.HeartbeatInterval)
		defer ticker.Stop()

		for {
			select {
			case <-runCtx.Done():
				errCh <- runCtx.Err()
				return
			case <-ticker.C:
				if err := sendHeartbeat(observedEpoch.Load()); err != nil {
					errCh <- err
					return
				}
			}
		}
	}()

	for {
		envelope, err := stream.Recv()
		if err != nil {
			return err
		}
		if err := s.lease.Apply(envelope, time.Now()); err != nil {
			if shouldRevokeLease(err) {
				s.lease.Revoke()
			}
			return err
		}
		observedEpoch.Store(envelope.GetEpoch())
		if s.cfg.Joiner != nil {
			if err := s.cfg.Joiner.Join(runCtx, envelope); err != nil {
				return err
			}
		}

		select {
		case err := <-errCh:
			return err
		default:
		}
	}
}

type LeaseTracker struct {
	mu               sync.RWMutex
	generation       int64
	epoch            int64
	validUntilUnixMs int64
}

func NewLeaseTracker() *LeaseTracker {
	return &LeaseTracker{}
}

// Apply validates and ingests a topology envelope. The state transition is:
//
//   - role != PRIMARY                   ⇒ ErrTopologyNotPrimary
//   - validUntil <= now                 ⇒ ErrTopologyLeaseExpired
//   - env.generation <  cur.generation  ⇒ ErrStaleWatchdogGeneration
//   - env.generation >  cur.generation  ⇒ accept, reset epoch baseline (N1)
//   - env.generation == cur.generation:
//     env.epoch    <  cur.epoch       ⇒ ErrStaleTopologyEpoch
//     env.epoch    >= cur.epoch       ⇒ accept
//
// On accept (generation, epoch, validUntilUnixMs) are persisted atomically
// under l.mu so that ServingAllowed/SnapshotForWrite cannot read a torn state.
func (l *LeaseTracker) Apply(envelope *topologypb.TopologyEnvelope, now time.Time) error {
	if envelope.GetWatchdogRole() != topologypb.WatchdogRole_WATCHDOG_ROLE_PRIMARY {
		return ErrTopologyNotPrimary
	}
	if envelope.GetValidUntilUnixMs() <= now.UnixMilli() {
		return ErrTopologyLeaseExpired
	}

	l.mu.Lock()
	defer l.mu.Unlock()

	switch {
	case envelope.GetWatchdogGeneration() < l.generation:
		return ErrStaleWatchdogGeneration
	case envelope.GetWatchdogGeneration() > l.generation:
		// N1: a new primary resets the epoch baseline. The new generation's
		// epoch sequence is independent of the previous one's, so we accept
		// any epoch the new primary publishes (including 0 on a fresh stack).
		l.generation = envelope.GetWatchdogGeneration()
		l.epoch = envelope.GetEpoch()
		l.validUntilUnixMs = envelope.GetValidUntilUnixMs()
		return nil
	default:
		if envelope.GetEpoch() < l.epoch {
			return ErrStaleTopologyEpoch
		}
		l.epoch = envelope.GetEpoch()
		l.validUntilUnixMs = envelope.GetValidUntilUnixMs()
		return nil
	}
}

func (l *LeaseTracker) Revoke() {
	l.mu.Lock()
	l.validUntilUnixMs = 0
	l.mu.Unlock()
}

func (l *LeaseTracker) ServingAllowed(now time.Time) bool {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.validUntilUnixMs > now.UnixMilli()
}

// SnapshotForWrite returns the current (generation, epoch) atomically with the
// lease validity check. ok is false if the lease has expired, in which case the
// caller MUST abort the write before mutating any durable state.
func (l *LeaseTracker) SnapshotForWrite(now time.Time) (generation, epoch int64, ok bool) {
	l.mu.RLock()
	defer l.mu.RUnlock()
	if l.validUntilUnixMs <= now.UnixMilli() {
		return 0, 0, false
	}
	return l.generation, l.epoch, true
}

func (l *LeaseTracker) WaitExpired(ctx context.Context, pollInterval time.Duration) error {
	if pollInterval <= 0 {
		pollInterval = time.Second
	}
	ticker := time.NewTicker(pollInterval)
	defer ticker.Stop()

	for {
		if !l.ServingAllowed(time.Now()) {
			return ErrTopologyLeaseExpired
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

// VerifyFence checks that a fence (generation, epoch) snapshotted earlier is
// still compatible with the current lease at time `now`. It is the in-lock
// re-validation barrier used by hooks that prepare a durable record outside
// the fragment lock and want to commit it inside.
//
// Compatible means:
//
//   - The lease has not expired (validUntil > now).
//   - The current generation has not changed since the snapshot. A generation
//     bump means leadership moved to a different PRIMARY; a record stamped
//     under the old PRIMARY must not be acknowledged.
//   - The current epoch is >= snapshotted epoch. A monotonic epoch advance
//     under the same PRIMARY is harmless because the fence triple at MySQL
//     remains lex-comparable.
//
// VerifyFence does not return the new (G, E): callers should re-snapshot if
// they need fresh values.
func (l *LeaseTracker) VerifyFence(generation, epoch int64, now time.Time) bool {
	l.mu.RLock()
	defer l.mu.RUnlock()
	if l.validUntilUnixMs <= now.UnixMilli() {
		return false
	}
	if l.generation != generation {
		return false
	}
	if l.epoch < epoch {
		return false
	}
	return true
}

func shouldRevokeLease(err error) bool {
	return errors.Is(err, ErrTopologyLeaseExpired) ||
		errors.Is(err, ErrTopologyNotPrimary) ||
		errors.Is(err, ErrStaleTopologyEpoch) ||
		errors.Is(err, ErrStaleWatchdogGeneration)
}
