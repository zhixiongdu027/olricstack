package main

import (
	"context"
	"log"
	"net"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	olricv1alpha1 "github.com/zhixiongdu/olricstack/api/olric/v1alpha1"
	topologypb "github.com/zhixiongdu/olricstack/api/topology/v1"
	"github.com/zhixiongdu/olricstack/internal/topology"
	stackwatchdog "github.com/zhixiongdu/olricstack/internal/watchdog"
	"github.com/zhixiongdu/olricstack/internal/watchdog/election"
	"google.golang.org/grpc"
	appsv1 "k8s.io/api/apps/v1"
	coordinationv1 "k8s.io/api/coordination/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	addr := os.Getenv("WATCHDOG_ADDR")
	if addr == "" {
		addr = ":8081"
	}

	listener, err := net.Listen("tcp", addr)
	if err != nil {
		log.Fatalf("listen %s: %v", addr, err)
	}

	initialRole := watchdogRoleFromEnv("WATCHDOG_ROLE")
	if os.Getenv("STACK_ID") != "" {
		initialRole = topologypb.WatchdogRole_WATCHDOG_ROLE_STANDBY
	}
	topologyService := topology.NewServiceWithConfig(topology.Config{
		SuspectAfter:     envDuration("SUSPECT_AFTER", 20*time.Second),
		ExpireAfter:      envDuration("EXPIRE_AFTER", 30*time.Second),
		ReapInterval:     envDuration("REAP_INTERVAL", 15*time.Second),
		BookwormInterval: envDuration("BOOKWORM_INTERVAL", 10*time.Second),
		LeaseTTL:         envDuration("TOPOLOGY_LEASE_TTL", 30*time.Second),
		WatchdogID:       envString("WATCHDOG_ID", os.Getenv("HOSTNAME")),
		Generation:       envInt64("WATCHDOG_GENERATION", time.Now().UnixNano()),
		Role:             initialRole,
	})
	go topologyService.RunReaper(ctx)
	go topologyService.RunBookworm(ctx)

	server := grpc.NewServer()
	topologypb.RegisterTopologyControlServer(server, topologyService)
	go func() {
		<-ctx.Done()
		server.GracefulStop()
	}()

	if err := startStackController(ctx, topologyService); err != nil {
		log.Printf("watchdog stack controller disabled: %v", err)
	}

	log.Printf("watchdog topology service listening on %s", addr)
	if err := server.Serve(listener); err != nil {
		log.Fatalf("serve watchdog: %v", err)
	}
}

func watchdogRoleFromEnv(name string) topologypb.WatchdogRole {
	switch os.Getenv(name) {
	case "standby", "STANDBY":
		return topologypb.WatchdogRole_WATCHDOG_ROLE_STANDBY
	default:
		return topologypb.WatchdogRole_WATCHDOG_ROLE_PRIMARY
	}
}

func envString(name, fallback string) string {
	value := os.Getenv(name)
	if value == "" {
		return fallback
	}
	return value
}

func envInt64(name string, fallback int64) int64 {
	value := os.Getenv(name)
	if value == "" {
		return fallback
	}
	parsed, err := strconv.ParseInt(value, 10, 64)
	if err != nil || parsed <= 0 {
		log.Printf("invalid %s=%q, using %d", name, value, fallback)
		return fallback
	}
	return parsed
}

func startStackController(ctx context.Context, observer stackwatchdog.PodObserver) error {
	stackID := os.Getenv("STACK_ID")
	if stackID == "" {
		return nil
	}
	namespace := os.Getenv("STACK_NAMESPACE")
	if namespace == "" {
		namespace = os.Getenv("POD_NAMESPACE")
	}
	if namespace == "" {
		namespace = "default"
	}

	scheme := runtime.NewScheme()
	utilruntime.Must(clientgoscheme.AddToScheme(scheme))
	utilruntime.Must(appsv1.AddToScheme(scheme))
	utilruntime.Must(corev1.AddToScheme(scheme))
	utilruntime.Must(coordinationv1.AddToScheme(scheme))
	utilruntime.Must(olricv1alpha1.AddToScheme(scheme))

	restConfig, err := ctrl.GetConfig()
	if err != nil {
		return err
	}
	k8sClient, err := client.New(restConfig, client.Options{Scheme: scheme})
	if err != nil {
		return err
	}
	if epochStoreSetter, ok := observer.(interface {
		SetEpochStore(topology.EpochStore)
	}); ok {
		epochStoreSetter.SetEpochStore(stackwatchdog.NewConfigMapEpochStore(k8sClient, namespace, stackID+"-topology"))
	}

	controller, err := stackwatchdog.NewController(k8sClient, scheme, stackwatchdog.ControllerConfig{
		StackID:           stackID,
		Namespace:         namespace,
		ReconcileInterval: envDuration("WATCHDOG_RECONCILE_INTERVAL", 10*time.Second),
	})
	if err != nil {
		return err
	}
	controller.Observer = observer
	if leadershipObserver, ok := observer.(election.Observer); ok {
		if elector, err := election.NewElector(k8sClient, election.Config{
			LeaseName:       stackID + "-watchdog",
			Namespace:       namespace,
			Identity:        envString("WATCHDOG_ID", os.Getenv("HOSTNAME")),
			LeaseDuration:   envDuration("WATCHDOG_LEASE_DURATION", 15*time.Second),
			RenewInterval:   envDuration("WATCHDOG_LEASE_RENEW_INTERVAL", 5*time.Second),
			AcquireInterval: envDuration("WATCHDOG_LEASE_ACQUIRE_INTERVAL", 5*time.Second),
		}, leadershipObserver); err != nil {
			log.Printf("watchdog lease election disabled: %v", err)
		} else {
			go elector.Run(ctx)
		}
	}
	go func() {
		if err := controller.Run(ctx); err != nil && ctx.Err() == nil {
			log.Printf("watchdog stack controller stopped: %v", err)
		}
	}()
	return nil
}

func envDuration(name string, fallback time.Duration) time.Duration {
	value := os.Getenv(name)
	if value == "" {
		return fallback
	}
	parsed, err := time.ParseDuration(value)
	if err != nil || parsed <= 0 {
		log.Printf("invalid %s=%q, using %s", name, value, fallback)
		return fallback
	}
	return parsed
}
