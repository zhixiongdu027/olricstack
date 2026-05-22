package watchdog

import (
	"context"
	"errors"
	"net"
	"testing"

	topologypb "github.com/zhixiongdu/olricstack/api/topology/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	healthgrpc "google.golang.org/grpc/health/grpc_health_v1"
)

func TestLeadershipHealthFollowsRole(t *testing.T) {
	t.Parallel()
	health := NewLeadershipHealth()
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
		t.Fatalf("check standby health: %v", err)
	}
	if resp.GetStatus() != healthgrpc.HealthCheckResponse_NOT_SERVING {
		t.Fatalf("expected standby not serving, got %s", resp.GetStatus())
	}

	health.SetLeadership(topologypb.WatchdogRole_WATCHDOG_ROLE_PRIMARY, 1)
	resp, err = client.Check(context.Background(), &healthgrpc.HealthCheckRequest{})
	if err != nil {
		t.Fatalf("check primary health: %v", err)
	}
	if resp.GetStatus() != healthgrpc.HealthCheckResponse_SERVING {
		t.Fatalf("expected primary serving, got %s", resp.GetStatus())
	}

	health.SetDegraded(errors.New("epoch persistence failed"))
	resp, err = client.Check(context.Background(), &healthgrpc.HealthCheckRequest{})
	if err != nil {
		t.Fatalf("check degraded health: %v", err)
	}
	if resp.GetStatus() != healthgrpc.HealthCheckResponse_NOT_SERVING {
		t.Fatalf("expected degraded not serving, got %s", resp.GetStatus())
	}

	health.SetDegraded(nil)
	resp, err = client.Check(context.Background(), &healthgrpc.HealthCheckRequest{})
	if err != nil {
		t.Fatalf("check recovered health: %v", err)
	}
	if resp.GetStatus() != healthgrpc.HealthCheckResponse_SERVING {
		t.Fatalf("expected recovered serving, got %s", resp.GetStatus())
	}
}
