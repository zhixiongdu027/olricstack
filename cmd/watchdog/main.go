package main

import (
	"context"
	"errors"
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
	"google.golang.org/grpc"
	appsv1 "k8s.io/api/apps/v1"
	coordinationv1 "k8s.io/api/coordination/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/client-go/kubernetes"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/leaderelection"
	"k8s.io/client-go/tools/leaderelection/resourcelock"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

type appConfig struct {
	addr      string
	stackID   string
	namespace string
	identity  string
	scheme    *runtime.Scheme
	k8sClient client.Client
	clientset kubernetes.Interface
	health    *stackwatchdog.LeadershipHealth
}

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	cfg, err := loadAppConfig()
	if err != nil {
		log.Fatalf("load watchdog config: %v", err)
	}

	if cfg.stackID == "" {
		if err := runStandalone(ctx, cfg); err != nil && ctx.Err() == nil {
			log.Fatalf("run standalone watchdog: %v", err)
		}
		return
	}

	runWithLeaderElection(ctx, cfg)
}

func loadAppConfig() (*appConfig, error) {
	addr := envString("WATCHDOG_ADDR", ":8081")
	stackID := os.Getenv("STACK_ID")
	namespace := envString("STACK_NAMESPACE", os.Getenv("POD_NAMESPACE"))
	if namespace == "" {
		namespace = "default"
	}
	identity := envString("WATCHDOG_ID", os.Getenv("HOSTNAME"))
	if identity == "" {
		identity = "watchdog"
	}

	scheme := runtime.NewScheme()
	utilruntime.Must(clientgoscheme.AddToScheme(scheme))
	utilruntime.Must(appsv1.AddToScheme(scheme))
	utilruntime.Must(corev1.AddToScheme(scheme))
	utilruntime.Must(coordinationv1.AddToScheme(scheme))
	utilruntime.Must(olricv1alpha1.AddToScheme(scheme))

	cfg := &appConfig{
		addr:      addr,
		stackID:   stackID,
		namespace: namespace,
		identity:  identity,
		scheme:    scheme,
		health:    stackwatchdog.NewLeadershipHealth(),
	}

	if stackID == "" {
		return cfg, nil
	}

	restConfig, err := ctrl.GetConfig()
	if err != nil {
		return nil, err
	}
	k8sClient, err := client.New(restConfig, client.Options{Scheme: scheme})
	if err != nil {
		return nil, err
	}
	clientset, err := kubernetes.NewForConfig(restConfig)
	if err != nil {
		return nil, err
	}
	cfg.k8sClient = k8sClient
	cfg.clientset = clientset
	return cfg, nil
}

func runWithLeaderElection(ctx context.Context, cfg *appConfig) {
	lock, err := resourcelock.New(
		resourcelock.LeasesResourceLock,
		cfg.namespace,
		cfg.stackID+"-watchdog",
		cfg.clientset.CoreV1(),
		cfg.clientset.CoordinationV1(),
		resourcelock.ResourceLockConfig{Identity: cfg.identity},
	)
	if err != nil {
		log.Fatalf("create leader lock: %v", err)
	}

	leaderelection.RunOrDie(ctx, leaderelection.LeaderElectionConfig{
		Lock:            lock,
		LeaseDuration:   envDuration("WATCHDOG_LEASE_DURATION", 15*time.Second),
		RenewDeadline:   envDuration("WATCHDOG_LEASE_RENEW_DEADLINE", 10*time.Second),
		RetryPeriod:     envDuration("WATCHDOG_LEASE_RETRY_PERIOD", 2*time.Second),
		ReleaseOnCancel: true,
		Callbacks: leaderelection.LeaderCallbacks{
			OnStartedLeading: func(leaderCtx context.Context) {
				if err := runPrimary(leaderCtx, cfg); err != nil && leaderCtx.Err() == nil {
					log.Printf("primary watchdog stopped: %v", err)
				}
			},
			OnStoppedLeading: func() {
				cfg.health.SetLeadership(topologypb.WatchdogRole_WATCHDOG_ROLE_STANDBY, 0)
				log.Printf("watchdog lost leadership")
			},
			OnNewLeader: func(identity string) {
				if identity != cfg.identity {
					log.Printf("watchdog leader is %s", identity)
				}
			},
		},
	})
}

func runStandalone(ctx context.Context, cfg *appConfig) error {
	return runPrimary(ctx, cfg)
}

func runPrimary(ctx context.Context, cfg *appConfig) error {
	generation := time.Now().UnixNano()
	cfg.health.SetLeadership(topologypb.WatchdogRole_WATCHDOG_ROLE_PRIMARY, generation)
	defer cfg.health.SetLeadership(topologypb.WatchdogRole_WATCHDOG_ROLE_STANDBY, 0)

	topologyService := topology.NewServiceWithConfig(topology.Config{
		SuspectAfter:     envDuration("SUSPECT_AFTER", 20*time.Second),
		ExpireAfter:      envDuration("EXPIRE_AFTER", 30*time.Second),
		ReapInterval:     envDuration("REAP_INTERVAL", 15*time.Second),
		BookwormInterval: envDuration("BOOKWORM_INTERVAL", 10*time.Second),
		LeaseTTL:         envDuration("TOPOLOGY_LEASE_TTL", 30*time.Second),
		WatchdogID:       cfg.identity,
		Generation:       generation,
		Role:             topologypb.WatchdogRole_WATCHDOG_ROLE_PRIMARY,
	})
	topologyService.AddLeadershipObserver(cfg.health)
	go topologyService.RunReaper(ctx)
	go topologyService.RunBookworm(ctx)

	if cfg.k8sClient != nil {
		topologyService.SetEpochStore(stackwatchdog.NewConfigMapEpochStore(cfg.k8sClient, cfg.namespace, cfg.stackID+"-topology"))
		controller, err := stackwatchdog.NewController(cfg.k8sClient, cfg.scheme, stackwatchdog.ControllerConfig{
			StackID:           cfg.stackID,
			Namespace:         cfg.namespace,
			ReconcileInterval: envDuration("WATCHDOG_RECONCILE_INTERVAL", 10*time.Second),
		})
		if err != nil {
			return err
		}
		controller.Observer = topologyService
		go func() {
			if err := controller.Run(ctx); err != nil && ctx.Err() == nil {
				log.Printf("watchdog stack controller stopped: %v", err)
			}
		}()
	}

	return serveTopology(ctx, cfg, topologyService)
}

func serveTopology(ctx context.Context, cfg *appConfig, topologyService *topology.Service) error {
	listener, err := net.Listen("tcp", cfg.addr)
	if err != nil {
		return err
	}
	defer listener.Close()

	server := grpc.NewServer()
	cfg.health.Register(server)
	if topologyService != nil {
		topologypb.RegisterTopologyControlServer(server, topologyService)
	}

	errCh := make(chan error, 1)
	go func() {
		errCh <- server.Serve(listener)
	}()
	log.Printf("watchdog service listening on %s", cfg.addr)

	select {
	case <-ctx.Done():
		server.GracefulStop()
		return ctx.Err()
	case err := <-errCh:
		if errors.Is(err, grpc.ErrServerStopped) {
			return nil
		}
		return err
	}
}

func envString(name, fallback string) string {
	value := os.Getenv(name)
	if value == "" {
		return fallback
	}
	return value
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
