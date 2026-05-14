package node

import (
	"context"
	"errors"
	"testing"
	"time"

	topologypb "github.com/zhixiongdu/olricstack/api/topology/v1"
)

func TestNewSubscriberRequiresIdentity(t *testing.T) {
	_, err := NewSubscriber(SubscriberConfig{StackID: "demo", PodIP: "10.0.0.2"})
	if err == nil {
		t.Fatal("expected missing node id error")
	}

	_, err = NewSubscriber(SubscriberConfig{NodeID: "node-a", PodIP: "10.0.0.2"})
	if err == nil {
		t.Fatal("expected missing stack id error")
	}
}

func TestLeaseTrackerRejectsExpiredEnvelope(t *testing.T) {
	tracker := NewLeaseTracker()
	err := tracker.Apply(&topologypb.TopologyEnvelope{
		WatchdogRole:       topologypb.WatchdogRole_WATCHDOG_ROLE_PRIMARY,
		WatchdogGeneration: 1,
		ValidUntilUnixMs:   time.Now().Add(-time.Second).UnixMilli(),
	}, time.Now())
	if !errors.Is(err, ErrTopologyLeaseExpired) {
		t.Fatalf("expected ErrTopologyLeaseExpired, got %v", err)
	}
}

func TestLeaseTrackerRejectsStandbyEnvelope(t *testing.T) {
	tracker := NewLeaseTracker()
	err := tracker.Apply(&topologypb.TopologyEnvelope{
		WatchdogRole:       topologypb.WatchdogRole_WATCHDOG_ROLE_STANDBY,
		WatchdogGeneration: 1,
		ValidUntilUnixMs:   time.Now().Add(time.Second).UnixMilli(),
	}, time.Now())
	if !errors.Is(err, ErrTopologyNotPrimary) {
		t.Fatalf("expected ErrTopologyNotPrimary, got %v", err)
	}
}

func TestLeaseTrackerRejectsStaleGeneration(t *testing.T) {
	tracker := NewLeaseTracker()
	now := time.Now()
	if err := tracker.Apply(&topologypb.TopologyEnvelope{
		WatchdogRole:       topologypb.WatchdogRole_WATCHDOG_ROLE_PRIMARY,
		WatchdogGeneration: 2,
		ValidUntilUnixMs:   now.Add(time.Second).UnixMilli(),
	}, now); err != nil {
		t.Fatalf("apply generation 2: %v", err)
	}
	if err := tracker.Apply(&topologypb.TopologyEnvelope{
		WatchdogRole:       topologypb.WatchdogRole_WATCHDOG_ROLE_PRIMARY,
		WatchdogGeneration: 1,
		ValidUntilUnixMs:   now.Add(time.Second).UnixMilli(),
	}, now); err == nil {
		t.Fatal("expected stale generation error")
	}
}

func TestLeaseTrackerServingAllowed(t *testing.T) {
	tracker := NewLeaseTracker()
	now := time.Now()
	if tracker.ServingAllowed(now) {
		t.Fatal("serving should be denied without lease")
	}
	if err := tracker.Apply(&topologypb.TopologyEnvelope{
		WatchdogRole:       topologypb.WatchdogRole_WATCHDOG_ROLE_PRIMARY,
		WatchdogGeneration: 1,
		ValidUntilUnixMs:   now.Add(time.Second).UnixMilli(),
	}, now); err != nil {
		t.Fatalf("apply lease: %v", err)
	}
	if !tracker.ServingAllowed(now) {
		t.Fatal("serving should be allowed with valid lease")
	}
}

func TestLeaseTrackerWaitExpired(t *testing.T) {
	tracker := NewLeaseTracker()
	now := time.Now()
	if err := tracker.Apply(&topologypb.TopologyEnvelope{
		WatchdogRole:       topologypb.WatchdogRole_WATCHDOG_ROLE_PRIMARY,
		WatchdogGeneration: 1,
		ValidUntilUnixMs:   now.Add(20 * time.Millisecond).UnixMilli(),
	}, now); err != nil {
		t.Fatalf("apply lease: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := tracker.WaitExpired(ctx, 5*time.Millisecond); !errors.Is(err, ErrTopologyLeaseExpired) {
		t.Fatalf("expected lease expiry, got %v", err)
	}
}

type recordingJoiner struct {
	envelope *topologypb.TopologyEnvelope
}

func (j *recordingJoiner) Join(ctx context.Context, envelope *topologypb.TopologyEnvelope) error {
	j.envelope = envelope
	return nil
}

func TestNewSubscriberDefaults(t *testing.T) {
	sub, err := NewSubscriber(SubscriberConfig{
		StackID: "demo",
		NodeID:  "node-a",
		PodIP:   "10.0.0.2",
	})
	if err != nil {
		t.Fatalf("new subscriber: %v", err)
	}
	if sub.cfg.HeartbeatInterval != 10*time.Second {
		t.Fatalf("expected default heartbeat interval, got %s", sub.cfg.HeartbeatInterval)
	}
	if sub.cfg.Incarnation <= 0 {
		t.Fatalf("expected generated incarnation, got %d", sub.cfg.Incarnation)
	}
	if sub.cfg.ProtocolVersion != "v2" {
		t.Fatalf("expected protocol v2, got %q", sub.cfg.ProtocolVersion)
	}
}
