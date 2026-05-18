package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	olric "github.com/olric-data/olric"
	olricconfig "github.com/olric-data/olric/config"
	topologypb "github.com/zhixiongdu/olricstack/api/topology/v1"
	"github.com/zhixiongdu/olricstack/internal/node"
	"github.com/zhixiongdu/olricstack/internal/olricstore"
	"github.com/zhixiongdu/olricstack/internal/store"
	"github.com/zhixiongdu/olricstack/internal/stringkv"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"k8s.io/apimachinery/pkg/util/wait"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	cacheStore, err := buildCacheStore()
	if err != nil {
		log.Fatalf("create backing store: %v", err)
	}
	topologyLease := node.NewLeaseTracker()
	var (
		leaseGatedStore *node.LeaseGatedStore
		ownerSequence   *node.OwnerSequence
		durableHook     *stringkv.DurableHook
	)
	if cacheStore != nil {
		leaseGatedStore, err = node.NewLeaseGatedStore(cacheStore, topologyLease)
		if err != nil {
			log.Fatalf("create lease-gated store: %v", err)
		}
		ownerSequence, err = node.NewOwnerSequence(ownerSequencePath())
		if err != nil {
			log.Fatalf("open owner sequence: %v", err)
		}
		writerID := envString("NODE_ID", os.Getenv("POD_NAME"))
		if writerID == "" {
			log.Fatalf("writer id is required: set NODE_ID or POD_NAME")
		}
		durableHook, err = stringkv.NewDurableHook(leaseGatedStore, topologyLease, ownerSequence, writerID)
		if err != nil {
			log.Fatalf("create durable hook: %v", err)
		}
	}
	defer func() {
		if leaseGatedStore != nil {
			shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			if err := leaseGatedStore.Close(shutdownCtx); err != nil {
				log.Printf("close cache store: %v", err)
			}
		}
		if ownerSequence != nil {
			if err := ownerSequence.Close(); err != nil {
				log.Printf("close owner sequence: %v", err)
			}
		}
	}()

	var olricBackingStore store.CacheStore
	if leaseGatedStore != nil {
		olricBackingStore = leaseGatedStore
	}
	olricDB, err := startOlric(ctx, olricBackingStore, durableHook)
	if err != nil {
		log.Fatalf("start olric: %v", err)
	}
	go runTopologySubscription(ctx, topologyLease, olricDB, leaseGatedStore)
	if leaseGatedStore != nil {
		provider, err := stringkv.NewOlricProvider(olricDB.NewEmbeddedClient())
		if err != nil {
			log.Fatalf("create string kv olric provider: %v", err)
		}
		stringService, err := stringkv.NewService(provider, topologyLease)
		if err != nil {
			log.Fatalf("create string kv service: %v", err)
		}
		respAddr := net.JoinHostPort(envString("RESP_BIND_ADDR", "0.0.0.0"), strconv.Itoa(envInt("RESP_BIND_PORT", 3321)))
		go func() {
			if err := stringkv.ServeRESP(ctx, stringService, stringkv.RESPServerConfig{
				Addr:           respAddr,
				DefaultDMap:    envString("RESP_DEFAULT_DMAP", stringkv.DefaultDMap),
				CommandTimeout: envDuration("RESP_COMMAND_TIMEOUT", 5*time.Second),
			}); err != nil {
				log.Printf("string kv RESP service stopped: %v", err)
			}
		}()
	}
	defer func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := olricDB.Shutdown(shutdownCtx); err != nil {
			log.Printf("shutdown olric: %v", err)
		}
	}()

	log.Printf("olric node bootstrap complete stack_id=%q watchdog=%q olric_internal=%s:%d resp=%s:%d memberlist=%s:%d",
		os.Getenv("STACK_ID"),
		os.Getenv("WATCHDOG_SVC_NAME"),
		envString("OLRIC_BIND_ADDR", "0.0.0.0"),
		envInt("OLRIC_BIND_PORT", 3320),
		envString("RESP_BIND_ADDR", "0.0.0.0"),
		envInt("RESP_BIND_PORT", 3321),
		envString("OLRIC_MEMBERLIST_BIND_ADDR", envString("OLRIC_BIND_ADDR", "0.0.0.0")),
		envInt("OLRIC_MEMBERLIST_BIND_PORT", 3322),
	)
	<-ctx.Done()
}

// ownerSequencePath derives the persistent owner-sequence path from WAL_PATH
// so a single mounted volume holds both bbolt files. Operators can override it
// explicitly with OWNER_SEQ_PATH.
func ownerSequencePath() string {
	if explicit := os.Getenv("OWNER_SEQ_PATH"); explicit != "" {
		return explicit
	}
	walPath := envString("WAL_PATH", "/var/lib/olricstack/cache.wal")
	return walPath + ".seq"
}

type generationPurger interface {
	PurgeBelowGeneration(ctx context.Context, minGeneration int64) (int, error)
}

type olricJoiner struct {
	db              *olric.Olric
	nodeID          string
	memberlistPort  int
	topologyTimeout time.Duration
	purger          generationPurger
	lastPurgedGen   int64
}

func (j *olricJoiner) Join(ctx context.Context, envelope *topologypb.TopologyEnvelope) error {
	log.Printf("received topology push epoch=%d reason=%s watchdog=%s/%d members=%v",
		envelope.GetEpoch(),
		envelope.GetReason().String(),
		envelope.GetWatchdogId(),
		envelope.GetWatchdogGeneration(),
		envelope.GetMembers(),
	)

	// Drop WAL records stamped with strictly older Watchdog generations.
	// These were written when this node served under a previous fence and
	// can no longer win the MySQL upsert. The dedup on lastPurgedGen keeps
	// bookworm refreshes (every BOOKWORM_INTERVAL) from re-scanning bbolt.
	if j.purger != nil {
		envGen := envelope.GetWatchdogGeneration()
		if envGen > j.lastPurgedGen {
			purgeCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
			purged, err := j.purger.PurgeBelowGeneration(purgeCtx, envGen)
			cancel()
			if err != nil {
				log.Printf("purge stale wal records below generation %d: %v", envGen, err)
			} else {
				if purged > 0 {
					log.Printf("purged %d stale wal records below generation %d", purged, envGen)
				}
				j.lastPurgedGen = envGen
			}
		}
	}

	peers := topologyPeers(envelope, j.nodeID, j.memberlistPort)
	if len(peers) == 0 || j.db == nil {
		return nil
	}
	joinCtx, cancel := context.WithTimeout(ctx, j.topologyTimeout)
	defer cancel()
	joined, err := j.db.Join(joinCtx, peers)
	if err != nil {
		return fmt.Errorf("join topology peers %v: %w", peers, err)
	}
	log.Printf("joined topology peers count=%d peers=%v", joined, peers)
	return nil
}

func runTopologySubscription(ctx context.Context, lease *node.LeaseTracker, olricDB *olric.Olric, purger generationPurger) {
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
		Lease:             lease,
		Joiner: &olricJoiner{
			db:              olricDB,
			nodeID:          envString("NODE_ID", os.Getenv("POD_NAME")),
			memberlistPort:  envInt("OLRIC_MEMBERLIST_ADVERTISE_PORT", envInt("OLRIC_ADVERTISE_PORT", envInt("OLRIC_MEMBERLIST_BIND_PORT", 3322))),
			topologyTimeout: envDuration("TOPOLOGY_JOIN_TIMEOUT", 5*time.Second),
			purger:          purger,
		},
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

func topologyPeers(envelope *topologypb.TopologyEnvelope, nodeID string, memberlistPort int) []string {
	seen := make(map[string]struct{}, len(envelope.GetMembers()))
	peers := make([]string, 0, len(envelope.GetMembers()))
	for _, member := range envelope.GetMembers() {
		if member.GetNodeId() == "" || member.GetNodeId() == nodeID {
			continue
		}
		podIP := strings.TrimSpace(member.GetPodIp())
		if podIP == "" {
			continue
		}
		peer := net.JoinHostPort(podIP, strconv.Itoa(memberlistPort))
		if _, ok := seen[peer]; ok {
			continue
		}
		seen[peer] = struct{}{}
		peers = append(peers, peer)
	}
	return peers
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

func buildCacheStore() (store.CacheStore, error) {
	mode := strings.ToLower(envString("STORAGE_MODE", ""))
	dsn := os.Getenv("MYSQL_DSN")
	if mode == "" {
		if dsn == "" && os.Getenv("WAL_PATH") == "" {
			mode = "memory"
		} else if dsn == "" {
			mode = "wal"
		} else {
			mode = "mysql"
		}
	}
	if mode == "memory" {
		log.Printf("storage mode: memory only")
		return nil, nil
	}
	cfg := store.Config{
		QueueSize:     envInt("DIRTY_QUEUE_SIZE", 1024),
		FlushInterval: envDuration("FLUSH_INTERVAL", time.Second),
		BatchSize:     envInt("FLUSH_BATCH_SIZE", 256),
		WALPath:       envString("WAL_PATH", "/var/lib/olricstack/cache.wal"),
		FlushBackoff:  envDuration("FLUSH_BACKOFF", time.Second),
	}
	if mode == "wal" {
		log.Printf("storage mode: memory + local wal")
		return store.NewWALStore(cfg)
	}
	if mode != "mysql" {
		return nil, fmt.Errorf("unknown STORAGE_MODE %q", mode)
	}
	if dsn == "" {
		return nil, errors.New("MYSQL_DSN is required when STORAGE_MODE=mysql")
	}
	db, err := gorm.Open(mysql.Open(dsn), &gorm.Config{})
	if err != nil {
		return nil, fmt.Errorf("open mysql: %w", err)
	}
	log.Printf("storage mode: memory + local wal + mysql")
	return store.NewMySQLStore(db, cfg)
}

func startOlric(ctx context.Context, cacheStore store.CacheStore, durableHook *stringkv.DurableHook) (*olric.Olric, error) {
	cfg := olricconfig.New(envString("OLRIC_MEMBERLIST_ENV", "lan"))
	cfg.BindAddr = envString("OLRIC_BIND_ADDR", "0.0.0.0")
	cfg.BindPort = envInt("OLRIC_BIND_PORT", 3320)
	cfg.MemberlistConfig.BindAddr = envString("OLRIC_MEMBERLIST_BIND_ADDR", cfg.BindAddr)
	cfg.MemberlistConfig.BindPort = envInt("OLRIC_MEMBERLIST_BIND_PORT", 3322)
	cfg.MemberlistConfig.AdvertiseAddr = envString("OLRIC_ADVERTISE_ADDR", os.Getenv("POD_IP"))
	cfg.MemberlistConfig.AdvertisePort = envInt("OLRIC_ADVERTISE_PORT", cfg.MemberlistConfig.BindPort)
	cfg.Peers = envCSV("OLRIC_PEERS")
	cfg.ReplicaCount = envInt("OLRIC_REPLICA_COUNT", 1)
	cfg.WriteQuorum = envInt("OLRIC_WRITE_QUORUM", 1)
	cfg.ReadQuorum = envInt("OLRIC_READ_QUORUM", 1)
	cfg.MemberCountQuorum = int32(envInt("OLRIC_MEMBER_COUNT_QUORUM", 1))
	cfg.LogLevel = envString("OLRIC_LOG_LEVEL", "WARN")
	cfg.LogVerbosity = int32(envInt("OLRIC_LOG_VERBOSITY", 3))
	cfg.DMaps.Engine = olricconfig.NewEngine()
	cfg.DMaps.Engine.Implementation = olricstore.New(cacheStore)
	if durableHook != nil {
		cfg.DurableHook = durableHook
	}

	started := make(chan struct{})
	cfg.Started = func() {
		close(started)
	}

	db, err := olric.New(cfg)
	if err != nil {
		return nil, err
	}

	errCh := make(chan error, 1)
	go func() {
		errCh <- db.Start()
	}()

	select {
	case <-started:
		return db, nil
	case err := <-errCh:
		return nil, err
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = db.Shutdown(shutdownCtx)
		return nil, ctx.Err()
	}
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

func envCSV(name string) []string {
	value := os.Getenv(name)
	if value == "" {
		return nil
	}
	parts := strings.Split(value, ",")
	out := make([]string, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part != "" {
			out = append(out, part)
		}
	}
	return out
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
