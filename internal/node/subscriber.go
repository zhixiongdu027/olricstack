package node

import (
	"context"
	"errors"
	"sync/atomic"
	"time"

	topologypb "github.com/zhixiongdu/olricstack/api/topology/v1"
)

var (
	ErrTopologyLeaseExpired = errors.New("topology lease is expired")
	ErrTopologyNotPrimary   = errors.New("topology envelope is not from primary watchdog")
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
	stream, err := client.Watch(ctx)
	if err != nil {
		return err
	}

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
			case <-ctx.Done():
				errCh <- ctx.Err()
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
			return err
		}
		observedEpoch.Store(envelope.GetEpoch())
		if s.cfg.Joiner != nil {
			if err := s.cfg.Joiner.Join(ctx, envelope); err != nil {
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
	validUntilUnixMs atomic.Int64
	generation       atomic.Int64
}

func NewLeaseTracker() *LeaseTracker {
	return &LeaseTracker{}
}

func (l *LeaseTracker) Apply(envelope *topologypb.TopologyEnvelope, now time.Time) error {
	if envelope.GetWatchdogRole() != topologypb.WatchdogRole_WATCHDOG_ROLE_PRIMARY {
		return ErrTopologyNotPrimary
	}
	if envelope.GetValidUntilUnixMs() <= now.UnixMilli() {
		return ErrTopologyLeaseExpired
	}
	if current := l.generation.Load(); envelope.GetWatchdogGeneration() < current {
		return errors.New("stale watchdog generation")
	}
	l.generation.Store(envelope.GetWatchdogGeneration())
	l.validUntilUnixMs.Store(envelope.GetValidUntilUnixMs())
	return nil
}

func (l *LeaseTracker) ServingAllowed(now time.Time) bool {
	validUntil := l.validUntilUnixMs.Load()
	return validUntil > now.UnixMilli()
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
