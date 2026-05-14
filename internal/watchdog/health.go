package watchdog

import (
	topologypb "github.com/zhixiongdu/olricstack/api/topology/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	healthgrpc "google.golang.org/grpc/health/grpc_health_v1"
)

type LeadershipHealth struct {
	server *health.Server
}

func NewLeadershipHealth() *LeadershipHealth {
	h := &LeadershipHealth{server: health.NewServer()}
	h.SetLeadership(topologypb.WatchdogRole_WATCHDOG_ROLE_STANDBY, 0)
	return h
}

func (h *LeadershipHealth) Register(registrar grpc.ServiceRegistrar) {
	healthgrpc.RegisterHealthServer(registrar, h.server)
}

func (h *LeadershipHealth) SetLeadership(role topologypb.WatchdogRole, generation int64) {
	status := healthgrpc.HealthCheckResponse_NOT_SERVING
	if role == topologypb.WatchdogRole_WATCHDOG_ROLE_PRIMARY {
		status = healthgrpc.HealthCheckResponse_SERVING
	}
	h.server.SetServingStatus("", status)
	h.server.SetServingStatus("topology.v1.TopologyControl", status)
}
