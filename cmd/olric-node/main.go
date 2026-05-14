package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	topologypb "github.com/zhixiongdu/olricstack/api/topology/v1"
	"github.com/zhixiongdu/olricstack/internal/node"
	"github.com/zhixiongdu/olricstack/internal/store"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"k8s.io/apimachinery/pkg/util/wait"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	dsn := os.Getenv("MYSQL_DSN")
	if dsn == "" {
		log.Fatal("MYSQL_DSN is required")
	}

	db, err := gorm.Open(mysql.Open(dsn), &gorm.Config{})
	if err != nil {
		log.Fatalf("open mysql: %v", err)
	}

	cacheStore, err := store.NewMySQLStore(db, store.Config{
		QueueSize:     envInt("DIRTY_QUEUE_SIZE", 1024),
		FlushInterval: envDuration("FLUSH_INTERVAL", time.Second),
		BatchSize:     envInt("FLUSH_BATCH_SIZE", 256),
		WALPath:       envString("WAL_PATH", "/var/lib/olricstack/cache.wal"),
		NodeID:        envString("NODE_ID", os.Getenv("POD_NAME")),
		FlushBackoff:  envDuration("FLUSH_BACKOFF", time.Second),
	})
	if err != nil {
		log.Fatalf("create cache store: %v", err)
	}
	defer func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := cacheStore.Close(shutdownCtx); err != nil {
			log.Printf("close cache store: %v", err)
		}
	}()

	go runTopologySubscription(ctx)

	log.Printf("olric node bootstrap complete stack_id=%q watchdog=%q", os.Getenv("STACK_ID"), os.Getenv("WATCHDOG_SVC_NAME"))
	<-ctx.Done()
}

type logJoiner struct{}

func (logJoiner) Join(ctx context.Context, envelope *topologypb.TopologyEnvelope) error {
	log.Printf("received topology push epoch=%d reason=%s watchdog=%s/%d members=%v",
		envelope.GetEpoch(),
		envelope.GetReason().String(),
		envelope.GetWatchdogId(),
		envelope.GetWatchdogGeneration(),
		envelope.GetMembers(),
	)
	return nil
}

func runTopologySubscription(ctx context.Context) {
	stackID := os.Getenv("STACK_ID")
	watchdogAddr := os.Getenv("WATCHDOG_SVC_NAME")
	podIP := os.Getenv("POD_IP")
	if stackID == "" || watchdogAddr == "" || podIP == "" {
		log.Printf("topology subscription disabled: STACK_ID, WATCHDOG_SVC_NAME and POD_IP are required")
		return
	}

	subscriber, err := node.NewSubscriber(node.SubscriberConfig{
		StackID:           stackID,
		NodeID:            envString("NODE_ID", os.Getenv("POD_NAME")),
		PodName:           os.Getenv("POD_NAME"),
		PodIP:             podIP,
		Incarnation:       envInt64("NODE_INCARNATION", time.Now().UnixNano()),
		ProtocolVersion:   "v2",
		HeartbeatInterval: envDuration("HEARTBEAT_INTERVAL", 10*time.Second),
		Joiner:            logJoiner{},
	})
	if err != nil {
		log.Printf("create topology subscriber: %v", err)
		return
	}

	retryInterval := envDuration("WATCHDOG_RECONNECT_INTERVAL", 3*time.Second)
	for {
		if err := subscribeOnce(ctx, watchdogAddr, subscriber); err != nil {
			log.Printf("topology subscription ended: %v", err)
		}

		select {
		case <-ctx.Done():
			return
		case <-time.After(wait.Jitter(retryInterval, 0.2)):
		}
	}
}

func subscribeOnce(ctx context.Context, watchdogAddr string, subscriber *node.Subscriber) error {
	dialCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	conn, err := grpc.DialContext(dialCtx, watchdogAddr, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithBlock())
	if err != nil {
		return err
	}
	defer conn.Close()

	client := topologypb.NewTopologyControlClient(conn)
	return subscriber.Run(ctx, client)
}

func envInt(name string, fallback int) int {
	value := os.Getenv(name)
	if value == "" {
		return fallback
	}
	parsed, err := strconv.Atoi(value)
	if err != nil || parsed <= 0 {
		log.Printf("invalid %s=%q, using %d", name, value, fallback)
		return fallback
	}
	return parsed
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
