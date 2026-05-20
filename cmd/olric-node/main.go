// olric-node hosts the Olric data plane and the user-facing RESP entrypoint.
// Durability is delegated to olric-sidecar via shared memory (oplog ring) and
// a unix-socket gRPC control surface.
package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
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
	"github.com/zhixiongdu/olricstack/internal/ring"
	"github.com/zhixiongdu/olricstack/internal/sidecar"
	"github.com/zhixiongdu/olricstack/internal/store"
	"github.com/zhixiongdu/olricstack/internal/stringkv"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"k8s.io/apimachinery/pkg/util/wait"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	topologyLease := node.NewLeaseTracker()

	ringPath := envString("RING_PATH", "/var/lib/olricstack/shared/oplog.ring")
	socketPath := envString("CONTROL_SOCKET", "/var/lib/olricstack/shared/oplog.sock")
	if err := waitForFile(ctx, ringPath, 30*time.Second); err != nil {
		log.Fatalf("wait for ring file: %v", err)
	}
	if err := waitForFile(ctx, socketPath, 30*time.Second); err != nil {
		log.Fatalf("wait for control socket: %v", err)
	}

	producer, err := ring.OpenProducer(ringPath)
	if err != nil {
		log.Fatalf("open ring producer: %v", err)
	}
	defer producer.Close()

	control, err := sidecar.Dial(ctx, socketPath)
	if err != nil {
		log.Fatalf("dial sidecar: %v", err)
	}
	defer control.Close()

	writerID := newWriterID()
	hookControl := &controlAdapter{client: control}
	hook, err := stringkv.NewDurableHook(producer, hookControl, topologyLease, stringkv.NewMemoryFenceSequencer(), writerID, stringkv.Config{
		AppendBudget: envDuration("RING_APPEND_BUDGET", 5*time.Second),
	})
	if err != nil {
		log.Fatalf("create durable hook: %v", err)
	}

	olricDB, err := startOlric(ctx, hook)
	if err != nil {
		log.Fatalf("start olric: %v", err)
	}
	defer func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := olricDB.Shutdown(shutdownCtx); err != nil {
			log.Printf("shutdown olric: %v", err)
		}
	}()

	go runTopologySubscription(ctx, topologyLease, olricDB, control)

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

	log.Printf("olric node bootstrap complete writer_id=%s stack_id=%q watchdog=%q olric_internal=%s:%d resp=%s:%d memberlist=%s:%d",
		writerID,
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

// controlAdapter bridges the sidecar.Client signature to the stringkv
// SidecarControl interface.
type controlAdapter struct {
	client *sidecar.Client
}

func (c *controlAdapter) Notify(ctx context.Context, pending uint64) error {
	return c.client.Notify(ctx, pending)
}

func (c *controlAdapter) LoadFromMySQL(ctx context.Context, dmap, key string, hkey uint64) (store.EntryRecord, bool, error) {
	resp, err := c.client.LoadFromMySQL(ctx, &sidecar.LoadRequest{DMap: dmap, Key: key, HKey: hkey})
	if err != nil {
		return store.EntryRecord{}, false, err
	}
	if !resp.Found {
		return store.EntryRecord{}, false, nil
	}
	return resp.Record, true, nil
}

func (c *controlAdapter) DrainPartition(ctx context.Context, dmap string, partitionID, partitionCount uint64) error {
	return c.client.DrainPartition(ctx, &sidecar.DrainPartitionRequest{
		DMap: dmap, PartitionID: partitionID, PartitionCount: partitionCount,
	})
}

// newWriterID returns a per-pod-instance writer identifier. It is intentionally
// not derived from POD_NAME alone: a StatefulSet-style stable hostname could
// reuse the same writer_id across restarts, breaking the (G, E, writer_id, S)
// uniqueness premise of the in-memory fence sequencer.
func newWriterID() string {
	prefix := envString("NODE_ID", os.Getenv("POD_NAME"))
	if prefix == "" {
		prefix = "node"
	}
	var buf [8]byte
	if _, err := rand.Read(buf[:]); err != nil {
		log.Fatalf("read randomness for writer id: %v", err)
	}
	return fmt.Sprintf("%s-%s", prefix, hex.EncodeToString(buf[:]))
}

// waitForFile blocks until path exists or ctx expires.
func waitForFile(ctx context.Context, path string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for {
		if _, err := os.Stat(path); err == nil {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("file %s not present after %s", path, timeout)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(200 * time.Millisecond):
		}
	}
}

func startOlric(ctx context.Context, durableHook *stringkv.DurableHook) (*olric.Olric, error) {
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
	cfg.DMaps.Engine.Implementation = olricstore.New()
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

type generationPurger interface {
	PurgeBelowGeneration(ctx context.Context, minGeneration int64) (int, error)
}

// sidecarPurger adapts sidecar.Client to the generationPurger interface used
// by olricJoiner.
type sidecarPurger struct {
	client *sidecar.Client
}

func (p *sidecarPurger) PurgeBelowGeneration(ctx context.Context, minGeneration int64) (int, error) {
	return p.client.PurgeBelowGeneration(ctx, minGeneration)
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
	if j.purger != nil {
		envGen := envelope.GetWatchdogGeneration()
		if envGen > j.lastPurgedGen {
			purgeCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
			purged, err := j.purger.PurgeBelowGeneration(purgeCtx, envGen)
			cancel()
			if err != nil {
				log.Printf("purge stale records below generation %d: %v", envGen, err)
			} else {
				if purged > 0 {
					log.Printf("purged %d stale records below generation %d", purged, envGen)
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

func runTopologySubscription(ctx context.Context, lease *node.LeaseTracker, olricDB *olric.Olric, control *sidecar.Client) {
	stackID := os.Getenv("STACK_ID")
	watchdogAddr := os.Getenv("WATCHDOG_SVC_NAME")
	podIP := os.Getenv("POD_IP")
	if stackID == "" || watchdogAddr == "" || podIP == "" {
		log.Printf("topology subscription disabled: STACK_ID, WATCHDOG_SVC_NAME and POD_IP are required")
		return
	}

	var purger generationPurger
	if control != nil {
		purger = &sidecarPurger{client: control}
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
