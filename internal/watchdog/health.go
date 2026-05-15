package watchdog

import (
	"sync"

	topologypb "github.com/zhixiongdu/olricstack/api/topology/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	healthgrpc "google.golang.org/grpc/health/grpc_health_v1"
)

type LeadershipHealth struct {
	server   *health.Server
	mu       sync.Mutex
	role     topologypb.WatchdogRole
	degraded bool
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
	h.mu.Lock()
	defer h.mu.Unlock()

	h.role = role
	h.refreshLocked()
}

func (h *LeadershipHealth) SetDegraded(err error) {
	h.mu.Lock()
	defer h.mu.Unlock()

	h.degraded = err != nil
	h.refreshLocked()
}

func (h *LeadershipHealth) refreshLocked() {
	status := healthgrpc.HealthCheckResponse_NOT_SERVING
	if h.role == topologypb.WatchdogRole_WATCHDOG_ROLE_PRIMARY && !h.degraded {
		status = healthgrpc.HealthCheckResponse_SERVING
	}
	h.server.SetServingStatus("", status)
	h.server.SetServingStatus("topology.v1.TopologyControl", status)
}
