package node

import (
	"context"
	"errors"
	"testing"
	"time"

	topologypb "github.com/zhixiongdu/olricstack/api/topology/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/proto"
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

func TestLeaseTrackerRejectsStaleEpoch(t *testing.T) {
	tracker := NewLeaseTracker()
	now := time.Now()
	if err := tracker.Apply(&topologypb.TopologyEnvelope{
		WatchdogRole:       topologypb.WatchdogRole_WATCHDOG_ROLE_PRIMARY,
		WatchdogGeneration: 1,
		Epoch:              2,
		ValidUntilUnixMs:   now.Add(time.Second).UnixMilli(),
	}, now); err != nil {
		t.Fatalf("apply epoch 2: %v", err)
	}
	if err := tracker.Apply(&topologypb.TopologyEnvelope{
		WatchdogRole:       topologypb.WatchdogRole_WATCHDOG_ROLE_PRIMARY,
		WatchdogGeneration: 1,
		Epoch:              1,
		ValidUntilUnixMs:   now.Add(time.Second).UnixMilli(),
	}, now); !errors.Is(err, ErrStaleTopologyEpoch) {
		t.Fatalf("expected stale epoch error, got %v", err)
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

func TestLeaseTrackerRevokeClearsServing(t *testing.T) {
	tracker := NewLeaseTracker()
	now := time.Now()
	if err := tracker.Apply(&topologypb.TopologyEnvelope{
		WatchdogRole:       topologypb.WatchdogRole_WATCHDOG_ROLE_PRIMARY,
		WatchdogGeneration: 1,
		ValidUntilUnixMs:   now.Add(time.Minute).UnixMilli(),
	}, now); err != nil {
		t.Fatalf("apply lease: %v", err)
	}
	tracker.Revoke()
	if tracker.ServingAllowed(now) {
		t.Fatal("serving should be denied after revoke")
	}
}

func TestLeaseTrackerNewGenerationResetsEpochBaseline(t *testing.T) {
	tracker := NewLeaseTracker()
	now := time.Now()

	// Establish a high epoch under generation 1.
	if err := tracker.Apply(&topologypb.TopologyEnvelope{
		WatchdogRole:       topologypb.WatchdogRole_WATCHDOG_ROLE_PRIMARY,
		WatchdogGeneration: 1,
		Epoch:              42,
		ValidUntilUnixMs:   now.Add(time.Minute).UnixMilli(),
	}, now); err != nil {
		t.Fatalf("seed gen=1 epoch=42: %v", err)
	}

	// New primary publishes a smaller epoch under a higher generation. This
	// must be accepted (N1) — otherwise live nodes deadlock on stale-epoch.
	if err := tracker.Apply(&topologypb.TopologyEnvelope{
		WatchdogRole:       topologypb.WatchdogRole_WATCHDOG_ROLE_PRIMARY,
		WatchdogGeneration: 2,
		Epoch:              5,
		ValidUntilUnixMs:   now.Add(time.Minute).UnixMilli(),
	}, now); err != nil {
		t.Fatalf("expected gen=2 epoch=5 accept after generation bump, got %v", err)
	}

	g, e, ok := tracker.SnapshotForWrite(now)
	if !ok || g != 2 || e != 5 {
		t.Fatalf("expected snapshot (g=2,e=5,ok=true), got (%d,%d,%v)", g, e, ok)
	}

	// Within the new generation, epoch must remain monotonic.
	if err := tracker.Apply(&topologypb.TopologyEnvelope{
		WatchdogRole:       topologypb.WatchdogRole_WATCHDOG_ROLE_PRIMARY,
		WatchdogGeneration: 2,
		Epoch:              4,
		ValidUntilUnixMs:   now.Add(time.Minute).UnixMilli(),
	}, now); !errors.Is(err, ErrStaleTopologyEpoch) {
		t.Fatalf("expected stale epoch within same generation, got %v", err)
	}
}

func TestLeaseTrackerSnapshotForWriteHonorsExpiry(t *testing.T) {
	tracker := NewLeaseTracker()
	now := time.Now()
	if err := tracker.Apply(&topologypb.TopologyEnvelope{
		WatchdogRole:       topologypb.WatchdogRole_WATCHDOG_ROLE_PRIMARY,
		WatchdogGeneration: 3,
		Epoch:              7,
		ValidUntilUnixMs:   now.Add(50 * time.Millisecond).UnixMilli(),
	}, now); err != nil {
		t.Fatalf("apply lease: %v", err)
	}

	if g, e, ok := tracker.SnapshotForWrite(now); !ok || g != 3 || e != 7 {
		t.Fatalf("expected live snapshot, got (%d,%d,%v)", g, e, ok)
	}
	if _, _, ok := tracker.SnapshotForWrite(now.Add(time.Second)); ok {
		t.Fatal("expected snapshot to deny once lease has expired")
	}
}

func TestSubscriberRunRevokesLeaseOnStandbyEnvelope(t *testing.T) {
	now := time.Now()
	lease := NewLeaseTracker()
	if err := lease.Apply(&topologypb.TopologyEnvelope{
		WatchdogRole:       topologypb.WatchdogRole_WATCHDOG_ROLE_PRIMARY,
		WatchdogGeneration: 1,
		Epoch:              2,
		ValidUntilUnixMs:   now.Add(time.Minute).UnixMilli(),
	}, now); err != nil {
		t.Fatalf("seed lease: %v", err)
	}

	sub, err := NewSubscriber(SubscriberConfig{
		StackID:           "demo",
		NodeID:            "node-a",
		PodName:           "pod-a",
		PodIP:             "10.0.0.2",
		Incarnation:       1,
		HeartbeatInterval: time.Hour,
		Lease:             lease,
	})
	if err != nil {
		t.Fatalf("new subscriber: %v", err)
	}

	stream := &fakeWatchStream{
		ctx: context.Background(),
		recvEnvelopes: []*topologypb.TopologyEnvelope{{
			WatchdogRole:       topologypb.WatchdogRole_WATCHDOG_ROLE_STANDBY,
			WatchdogGeneration: 1,
			Epoch:              2,
			ValidUntilUnixMs:   now.Add(time.Minute).UnixMilli(),
		}},
	}
	client := &fakeTopologyClient{stream: stream}

	err = sub.Run(context.Background(), client)
	if !errors.Is(err, ErrTopologyNotPrimary) {
		t.Fatalf("expected standby envelope error, got %v", err)
	}
	if lease.ServingAllowed(now) {
		t.Fatal("lease should be revoked after standby envelope")
	}
	if len(stream.sentHeartbeats) != 1 {
		t.Fatalf("expected initial heartbeat send, got %d", len(stream.sentHeartbeats))
	}
	if !stream.closeSendCalled {
		t.Fatal("expected stream CloseSend on exit")
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

type fakeTopologyClient struct {
	stream grpc.BidiStreamingClient[topologypb.Heartbeat, topologypb.TopologyEnvelope]
}

func (c *fakeTopologyClient) GetTopology(context.Context, *topologypb.TopologyQuery, ...grpc.CallOption) (*topologypb.TopologyEnvelope, error) {
	return nil, errors.New("not implemented")
}

func (c *fakeTopologyClient) Watch(context.Context, ...grpc.CallOption) (grpc.BidiStreamingClient[topologypb.Heartbeat, topologypb.TopologyEnvelope], error) {
	return c.stream, nil
}

type fakeWatchStream struct {
	ctx             context.Context
	sentHeartbeats  []*topologypb.Heartbeat
	recvEnvelopes   []*topologypb.TopologyEnvelope
	recvErr         error
	closeSendCalled bool
}

func (s *fakeWatchStream) Send(msg *topologypb.Heartbeat) error {
	s.sentHeartbeats = append(s.sentHeartbeats, msg)
	return nil
}

func (s *fakeWatchStream) Recv() (*topologypb.TopologyEnvelope, error) {
	if len(s.recvEnvelopes) > 0 {
		envelope := s.recvEnvelopes[0]
		s.recvEnvelopes = s.recvEnvelopes[1:]
		return envelope, nil
	}
	if s.recvErr != nil {
		return nil, s.recvErr
	}
	return nil, context.Canceled
}

func (s *fakeWatchStream) Header() (metadata.MD, error) {
	return nil, nil
}

func (s *fakeWatchStream) Trailer() metadata.MD {
	return nil
}

func (s *fakeWatchStream) CloseSend() error {
	s.closeSendCalled = true
	return nil
}

func (s *fakeWatchStream) Context() context.Context {
	if s.ctx != nil {
		return s.ctx
	}
	return context.Background()
}

func (s *fakeWatchStream) SendMsg(m any) error {
	msg, ok := m.(*topologypb.Heartbeat)
	if !ok {
		return errors.New("unexpected send message type")
	}
	return s.Send(msg)
}

func (s *fakeWatchStream) RecvMsg(m any) error {
	envelope, err := s.Recv()
	if err != nil {
		return err
	}
	target, ok := m.(*topologypb.TopologyEnvelope)
	if !ok {
		return errors.New("unexpected recv message type")
	}
	proto.Reset(target)
	proto.Merge(target, envelope)
	return nil
}
