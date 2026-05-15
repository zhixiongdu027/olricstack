//go:build e2e

package e2e

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	topologypb "github.com/zhixiongdu/olricstack/api/topology/v1"
	"github.com/zhixiongdu/olricstack/internal/topology"
	"google.golang.org/grpc"
)

func TestHostOnlyMySQLDurableWritePath(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	mysql := provisionHostMySQL(t, "127.0.0.1")
	watchdogAddr, _, cleanupWatchdog := startHostWatchdog(t, ctx)
	defer cleanupWatchdog()

	node := startHostOlricNode(t, ctx, watchdogAddr, mysql.hostDSN(), "host-only-node")
	defer node.stop(t)

	key := "host-user:" + strconv.FormatInt(time.Now().UnixNano(), 10)
	if out, err := node.put(ctx, "users", key, "value-host-only"); err != nil {
		t.Fatalf("write host dmap entry: %v (%s)\nnode logs:\n%s", err, strings.TrimSpace(out), strings.TrimSpace(node.logs.String()))
	}

	record := waitForMySQLRecord(t, ctx, mysql.hostDSN(), "users", key)
	if record.WriterID != node.nodeID {
		t.Fatalf("expected mysql writer_id %q, got %q", node.nodeID, record.WriterID)
	}
	if record.Tombstone {
		t.Fatalf("expected mysql record for users/%s to be live, got tombstone", key)
	}
	if record.Version <= 0 {
		t.Fatalf("expected mysql record version > 0, got %d", record.Version)
	}
}

func TestHostOnlyWatchdogDemotionBlocksAndPromotionRestoresWrites(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	mysql := provisionHostMySQL(t, "127.0.0.1")
	watchdogAddr, service, cleanupWatchdog := startHostWatchdog(t, ctx)
	defer cleanupWatchdog()

	node := startHostOlricNode(t, ctx, watchdogAddr, mysql.hostDSN(), "host-only-failover-node")
	defer node.stop(t)

	firstKey := "host-before-demotion:" + strconv.FormatInt(time.Now().UnixNano(), 10)
	if out, err := node.put(ctx, "users", firstKey, "before-demotion"); err != nil {
		t.Fatalf("write before demotion: %v (%s)\nnode logs:\n%s", err, strings.TrimSpace(out), strings.TrimSpace(node.logs.String()))
	}
	waitForMySQLRecord(t, ctx, mysql.hostDSN(), "users", firstKey)

	service.SetLeadership(topologypb.WatchdogRole_WATCHDOG_ROLE_STANDBY, 1)
	waitForText(t, ctx, node.logs, "topology subscription ended")
	rejectedKey := "host-during-demotion:" + strconv.FormatInt(time.Now().UnixNano(), 10)
	waitForPutFailure(t, ctx, node, "users", rejectedKey, "during-demotion")

	service.SetLeadership(topologypb.WatchdogRole_WATCHDOG_ROLE_PRIMARY, 2)
	waitForText(t, ctx, node.logs, "watchdog=wd-primary/2")

	recoveredKey := "host-after-promotion:" + strconv.FormatInt(time.Now().UnixNano(), 10)
	if out, err := node.put(ctx, "users", recoveredKey, "after-promotion"); err != nil {
		t.Fatalf("write after promotion: %v (%s)\nnode logs:\n%s", err, strings.TrimSpace(out), strings.TrimSpace(node.logs.String()))
	}
	record := waitForMySQLRecord(t, ctx, mysql.hostDSN(), "users", recoveredKey)
	if record.WriterID != node.nodeID {
		t.Fatalf("expected mysql writer_id %q after promotion, got %q", node.nodeID, record.WriterID)
	}
}

func TestHostOnlyRestartReadsThroughMySQL(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	mysql := provisionHostMySQL(t, "127.0.0.1")
	watchdogAddr, _, cleanupWatchdog := startHostWatchdog(t, ctx)
	defer cleanupWatchdog()

	key := "host-read-through:" + strconv.FormatInt(time.Now().UnixNano(), 10)
	first := startHostOlricNode(t, ctx, watchdogAddr, mysql.hostDSN(), "host-only-read-through-writer")
	if out, err := first.put(ctx, "users", key, "value-from-mysql"); err != nil {
		first.stop(t)
		t.Fatalf("write before restart: %v (%s)\nnode logs:\n%s", err, strings.TrimSpace(out), strings.TrimSpace(first.logs.String()))
	}
	waitForMySQLRecord(t, ctx, mysql.hostDSN(), "users", key)
	first.stop(t)

	second := startHostOlricNode(t, ctx, watchdogAddr, mysql.hostDSN(), "host-only-read-through-reader")
	defer second.stop(t)

	got, out, err := second.get(ctx, "users", key)
	if err != nil {
		t.Fatalf("read after restart: %v (%s)\nnode logs:\n%s", err, strings.TrimSpace(out), strings.TrimSpace(second.logs.String()))
	}
	if got != "value-from-mysql" {
		t.Fatalf("expected read-through value %q, got %q", "value-from-mysql", got)
	}
}

func TestHostOnlyTwoNodeClusterCrossNodeReadWrite(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	watchdogAddr, _, cleanupWatchdog := startHostWatchdog(t, ctx)
	defer cleanupWatchdog()

	memberlistPort := mustFreeTCPPort(t)
	first := startHostOlricNodeWithConfig(t, ctx, hostNodeConfig{
		watchdogAddr:    watchdogAddr,
		nodeID:          "host-only-cluster-a",
		podIP:           "127.0.0.2",
		bindAddr:        "127.0.0.2",
		memberlistPort:  memberlistPort,
		memberlistPeers: net.JoinHostPort("127.0.0.3", strconv.Itoa(memberlistPort)),
	})
	defer first.stop(t)
	second := startHostOlricNodeWithConfig(t, ctx, hostNodeConfig{
		watchdogAddr:    watchdogAddr,
		nodeID:          "host-only-cluster-b",
		podIP:           "127.0.0.3",
		bindAddr:        "127.0.0.3",
		memberlistPort:  memberlistPort,
		memberlistPeers: net.JoinHostPort("127.0.0.2", strconv.Itoa(memberlistPort)),
	})
	defer second.stop(t)

	waitForText(t, ctx, first.logs, `node_id:"host-only-cluster-b"`)
	waitForText(t, ctx, second.logs, `node_id:"host-only-cluster-a"`)
	waitForText(t, ctx, first.logs, "joined topology peers")
	waitForText(t, ctx, second.logs, "joined topology peers")

	key := "host-cluster:" + strconv.FormatInt(time.Now().UnixNano(), 10)
	if out, err := first.put(ctx, "users", key, "value-from-node-a"); err != nil {
		t.Fatalf("write through first node: %v (%s)\nfirst logs:\n%s\nsecond logs:\n%s",
			err, strings.TrimSpace(out), strings.TrimSpace(first.logs.String()), strings.TrimSpace(second.logs.String()))
	}
	got, out, err := second.get(ctx, "users", key)
	if err != nil {
		t.Fatalf("read through second node: %v (%s)\nfirst logs:\n%s\nsecond logs:\n%s",
			err, strings.TrimSpace(out), strings.TrimSpace(first.logs.String()), strings.TrimSpace(second.logs.String()))
	}
	if got != "value-from-node-a" {
		t.Fatalf("expected cross-node value %q, got %q", "value-from-node-a", got)
	}
}

func TestHostOnlyTwoNodeClusterJoinsFromWatchdogTopologyOnly(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	watchdogAddr, _, cleanupWatchdog := startHostWatchdog(t, ctx)
	defer cleanupWatchdog()

	memberlistPort := mustFreeTCPPort(t)
	first := startHostOlricNodeWithConfig(t, ctx, hostNodeConfig{
		watchdogAddr:   watchdogAddr,
		nodeID:         "host-only-topology-a",
		podIP:          "127.0.0.2",
		bindAddr:       "127.0.0.2",
		memberlistPort: memberlistPort,
	})
	defer first.stop(t)
	second := startHostOlricNodeWithConfig(t, ctx, hostNodeConfig{
		watchdogAddr:   watchdogAddr,
		nodeID:         "host-only-topology-b",
		podIP:          "127.0.0.3",
		bindAddr:       "127.0.0.3",
		memberlistPort: memberlistPort,
	})
	defer second.stop(t)

	waitForText(t, ctx, first.logs, `node_id:"host-only-topology-b"`)
	waitForText(t, ctx, second.logs, `node_id:"host-only-topology-a"`)
	waitForText(t, ctx, first.logs, "joined topology peers")
	waitForText(t, ctx, second.logs, "joined topology peers")
	waitForText(t, ctx, first.logs, "Node joined:")
	waitForText(t, ctx, second.logs, "Node joined:")

	key := "host-topology-only:" + strconv.FormatInt(time.Now().UnixNano(), 10)
	if out, err := first.put(ctx, "users", key, "value-from-topology-only"); err != nil {
		t.Fatalf("write through first topology-only node: %v (%s)\nfirst logs:\n%s\nsecond logs:\n%s",
			err, strings.TrimSpace(out), strings.TrimSpace(first.logs.String()), strings.TrimSpace(second.logs.String()))
	}
	got, out, err := second.get(ctx, "users", key)
	if err != nil {
		t.Fatalf("read through second topology-only node: %v (%s)\nfirst logs:\n%s\nsecond logs:\n%s",
			err, strings.TrimSpace(out), strings.TrimSpace(first.logs.String()), strings.TrimSpace(second.logs.String()))
	}
	if got != "value-from-topology-only" {
		t.Fatalf("expected topology-only cross-node value %q, got %q", "value-from-topology-only", got)
	}
}

func TestHostOnlyKilledNodePrunedAndReplacementRejoins(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	watchdogAddr, _, cleanupWatchdog := startHostWatchdogWithConfig(t, ctx, hostWatchdogConfig{
		suspectAfter:     300 * time.Millisecond,
		expireAfter:      600 * time.Millisecond,
		reapInterval:     100 * time.Millisecond,
		bookwormInterval: 100 * time.Millisecond,
		leaseTTL:         time.Second,
	})
	defer cleanupWatchdog()

	memberlistPort := mustFreeTCPPort(t)
	first := startHostOlricNodeWithConfig(t, ctx, hostNodeConfig{
		watchdogAddr:   watchdogAddr,
		nodeID:         "host-only-kill-a",
		podIP:          "127.0.0.2",
		bindAddr:       "127.0.0.2",
		memberlistPort: memberlistPort,
	})
	defer first.stop(t)
	victim := startHostOlricNodeWithConfig(t, ctx, hostNodeConfig{
		watchdogAddr:   watchdogAddr,
		nodeID:         "host-only-kill-b",
		podIP:          "127.0.0.3",
		bindAddr:       "127.0.0.3",
		memberlistPort: memberlistPort,
	})

	waitForText(t, ctx, first.logs, `node_id:"host-only-kill-b"`)
	killOffset := first.logs.Len()
	victim.kill(t)
	waitForTopologyWithoutMemberAfter(t, ctx, first.logs, killOffset, "host-only-kill-b")

	replacement := startHostOlricNodeWithConfig(t, ctx, hostNodeConfig{
		watchdogAddr:   watchdogAddr,
		nodeID:         "host-only-kill-c",
		podIP:          "127.0.0.4",
		bindAddr:       "127.0.0.4",
		memberlistPort: memberlistPort,
	})
	defer replacement.stop(t)

	waitForText(t, ctx, first.logs, `node_id:"host-only-kill-c"`)
	waitForText(t, ctx, replacement.logs, `node_id:"host-only-kill-a"`)
	waitForText(t, ctx, first.logs, "joined topology peers")
	waitForText(t, ctx, replacement.logs, "joined topology peers")

	key := "host-kill-rejoin:" + strconv.FormatInt(time.Now().UnixNano(), 10)
	if out, err := first.put(ctx, "users", key, "value-after-rejoin"); err != nil {
		t.Fatalf("write after replacement rejoin: %v (%s)\nfirst logs:\n%s\nreplacement logs:\n%s",
			err, strings.TrimSpace(out), strings.TrimSpace(first.logs.String()), strings.TrimSpace(replacement.logs.String()))
	}
	waitForGetValue(t, ctx, replacement, "users", key, "value-after-rejoin", first, replacement)
}

func TestHostOnlyTwoNodeWriteStorm(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	watchdogAddr, _, cleanupWatchdog := startHostWatchdog(t, ctx)
	defer cleanupWatchdog()

	memberlistPort := mustFreeTCPPort(t)
	first := startHostOlricNodeWithConfig(t, ctx, hostNodeConfig{
		watchdogAddr:   watchdogAddr,
		nodeID:         "host-only-storm-a",
		podIP:          "127.0.0.2",
		bindAddr:       "127.0.0.2",
		memberlistPort: memberlistPort,
	})
	defer first.stop(t)
	second := startHostOlricNodeWithConfig(t, ctx, hostNodeConfig{
		watchdogAddr:   watchdogAddr,
		nodeID:         "host-only-storm-b",
		podIP:          "127.0.0.3",
		bindAddr:       "127.0.0.3",
		memberlistPort: memberlistPort,
	})
	defer second.stop(t)

	waitForText(t, ctx, first.logs, `node_id:"host-only-storm-b"`)
	waitForText(t, ctx, second.logs, `node_id:"host-only-storm-a"`)
	waitForText(t, ctx, first.logs, "joined topology peers")
	waitForText(t, ctx, second.logs, "joined topology peers")

	const writers = 8
	const writesPerWorker = 12
	var failures atomic.Int64
	var wg sync.WaitGroup
	for worker := 0; worker < writers; worker++ {
		worker := worker
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < writesPerWorker; i++ {
				target := first
				if (worker+i)%2 == 1 {
					target = second
				}
				key := fmt.Sprintf("storm:%d:%d:%d", time.Now().UnixNano(), worker, i)
				value := "value-" + key
				if out, err := target.put(ctx, "users", key, value); err != nil {
					t.Logf("storm put failed target=%s key=%s err=%v out=%s", target.nodeID, key, err, strings.TrimSpace(out))
					failures.Add(1)
					continue
				}
				reader := second
				if target == second {
					reader = first
				}
				got, out, err := reader.get(ctx, "users", key)
				if err != nil || got != value {
					t.Logf("storm get failed reader=%s key=%s got=%q err=%v out=%s", reader.nodeID, key, got, err, strings.TrimSpace(out))
					failures.Add(1)
				}
			}
		}()
	}
	wg.Wait()
	if got := failures.Load(); got != 0 {
		t.Fatalf("write storm had %d failures\nfirst logs:\n%s\nsecond logs:\n%s",
			got, strings.TrimSpace(first.logs.String()), strings.TrimSpace(second.logs.String()))
	}
}

func TestHostOnlyWatchdogNetworkJitterExpiresAndRecoversLease(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	watchdogAddr, _, cleanupWatchdog := startHostWatchdogWithConfig(t, ctx, hostWatchdogConfig{
		suspectAfter:     600 * time.Millisecond,
		expireAfter:      1200 * time.Millisecond,
		reapInterval:     100 * time.Millisecond,
		bookwormInterval: 100 * time.Millisecond,
		leaseTTL:         500 * time.Millisecond,
	})
	defer cleanupWatchdog()

	proxy := startTCPProxy(t, ctx, watchdogAddr)
	defer proxy.close(t)

	memberlistPort := mustFreeTCPPort(t)
	first := startHostOlricNodeWithConfig(t, ctx, hostNodeConfig{
		watchdogAddr:   proxy.addr(),
		nodeID:         "host-only-jitter-a",
		podIP:          "127.0.0.2",
		bindAddr:       "127.0.0.2",
		memberlistPort: memberlistPort,
	})
	defer first.stop(t)
	second := startHostOlricNodeWithConfig(t, ctx, hostNodeConfig{
		watchdogAddr:   proxy.addr(),
		nodeID:         "host-only-jitter-b",
		podIP:          "127.0.0.3",
		bindAddr:       "127.0.0.3",
		memberlistPort: memberlistPort,
	})
	defer second.stop(t)

	waitForText(t, ctx, first.logs, `node_id:"host-only-jitter-b"`)
	waitForText(t, ctx, second.logs, `node_id:"host-only-jitter-a"`)
	waitForText(t, ctx, first.logs, "joined topology peers")
	waitForText(t, ctx, second.logs, "joined topology peers")

	beforeKey := "host-jitter-before:" + strconv.FormatInt(time.Now().UnixNano(), 10)
	if out, err := first.put(ctx, "users", beforeKey, "before-jitter"); err != nil {
		t.Fatalf("write before watchdog jitter: %v (%s)\nfirst logs:\n%s\nsecond logs:\n%s",
			err, strings.TrimSpace(out), strings.TrimSpace(first.logs.String()), strings.TrimSpace(second.logs.String()))
	}
	if got, out, err := second.get(ctx, "users", beforeKey); err != nil || got != "before-jitter" {
		t.Fatalf("cross-node read before watchdog jitter got=%q err=%v (%s)\nfirst logs:\n%s\nsecond logs:\n%s",
			got, err, strings.TrimSpace(out), strings.TrimSpace(first.logs.String()), strings.TrimSpace(second.logs.String()))
	}

	proxy.pause()
	waitForText(t, ctx, first.logs, "topology subscription ended")
	waitForText(t, ctx, second.logs, "topology subscription ended")
	waitForPutFailure(t, ctx, first, "users", "host-jitter-blocked:"+strconv.FormatInt(time.Now().UnixNano(), 10), "during-jitter")

	firstRecoveryOffset := first.logs.Len()
	secondRecoveryOffset := second.logs.Len()
	proxy.resume()
	waitForTextAfter(t, ctx, first.logs, firstRecoveryOffset, "TOPOLOGY_REASON_BOOKWORM")
	waitForTextAfter(t, ctx, second.logs, secondRecoveryOffset, "TOPOLOGY_REASON_BOOKWORM")

	afterKey := "host-jitter-after:" + strconv.FormatInt(time.Now().UnixNano(), 10)
	if out, err := first.put(ctx, "users", afterKey, "after-jitter"); err != nil {
		t.Fatalf("write after watchdog jitter recovery: %v (%s)\nfirst logs:\n%s\nsecond logs:\n%s",
			err, strings.TrimSpace(out), strings.TrimSpace(first.logs.String()), strings.TrimSpace(second.logs.String()))
	}
	got, out, err := second.get(ctx, "users", afterKey)
	if err != nil {
		t.Fatalf("cross-node read after watchdog jitter recovery: %v (%s)\nfirst logs:\n%s\nsecond logs:\n%s",
			err, strings.TrimSpace(out), strings.TrimSpace(first.logs.String()), strings.TrimSpace(second.logs.String()))
	}
	if got != "after-jitter" {
		t.Fatalf("expected recovered cross-node value %q, got %q", "after-jitter", got)
	}
}

type hostOlricNode struct {
	nodeID       string
	repoRoot     string
	clientBinary string
	bindAddr     string
	olricPort    int
	cmd          *exec.Cmd
	errCh        chan error
	logs         *lockedBuffer
}

func (n *hostOlricNode) kill(t *testing.T) {
	t.Helper()
	if n.cmd.Process != nil {
		_ = n.cmd.Process.Kill()
	}
	select {
	case <-n.errCh:
	case <-time.After(10 * time.Second):
		t.Fatalf("host olric-node did not exit after SIGKILL\nlogs:\n%s", strings.TrimSpace(n.logs.String()))
	}
}

func startHostOlricNode(t *testing.T, parent context.Context, watchdogAddr, mysqlDSN, nodeID string) *hostOlricNode {
	t.Helper()
	return startHostOlricNodeWithConfig(t, parent, hostNodeConfig{
		watchdogAddr: watchdogAddr,
		mysqlDSN:     mysqlDSN,
		nodeID:       nodeID,
		podIP:        "127.0.0.1",
		bindAddr:     "127.0.0.1",
	})
}

type hostNodeConfig struct {
	watchdogAddr    string
	mysqlDSN        string
	nodeID          string
	podIP           string
	bindAddr        string
	memberlistPort  int
	memberlistPeers string
}

func startHostOlricNodeWithConfig(t *testing.T, parent context.Context, cfg hostNodeConfig) *hostOlricNode {
	t.Helper()
	if cfg.podIP == "" {
		cfg.podIP = "127.0.0.1"
	}
	if cfg.bindAddr == "" {
		cfg.bindAddr = cfg.podIP
	}

	repoRoot := repoRoot(t)
	binDir := t.TempDir()
	nodeBinary := buildBinary(t, parent, repoRoot, filepath.Join(binDir, "olric-node"), "./cmd/olric-node", nil)
	clientBinary := buildBinary(t, parent, repoRoot, filepath.Join(binDir, "olric-e2e-client"), "./cmd/olric-e2e-client", nil)
	olricPort := mustFreeTCPPort(t)
	memberlistPort := mustFreeTCPPort(t)
	if cfg.memberlistPort > 0 {
		memberlistPort = cfg.memberlistPort
	}
	logs := &lockedBuffer{}

	cmd := exec.Command(nodeBinary)
	cmd.Dir = repoRoot
	cmd.Stdout = logs
	cmd.Stderr = logs
	cmd.Env = append(os.Environ(),
		"STACK_ID=host-only",
		"WATCHDOG_SVC_NAME="+cfg.watchdogAddr,
		"POD_NAME="+cfg.nodeID,
		"NODE_ID="+cfg.nodeID,
		"POD_IP="+cfg.podIP,
		"STORAGE_MODE="+hostStorageMode(cfg.mysqlDSN),
		"MYSQL_DSN="+cfg.mysqlDSN,
		"WAL_PATH="+filepath.Join(t.TempDir(), "cache.wal"),
		"FLUSH_INTERVAL=100ms",
		"FLUSH_BACKOFF=100ms",
		"HEARTBEAT_INTERVAL=100ms",
		"WATCHDOG_RECONNECT_INTERVAL=100ms",
		"OLRIC_BIND_ADDR="+cfg.bindAddr,
		"OLRIC_BIND_PORT="+strconv.Itoa(olricPort),
		"OLRIC_MEMBERLIST_BIND_ADDR="+cfg.bindAddr,
		"OLRIC_MEMBERLIST_BIND_PORT="+strconv.Itoa(memberlistPort),
		"OLRIC_ADVERTISE_ADDR="+cfg.bindAddr,
		"OLRIC_ADVERTISE_PORT="+strconv.Itoa(memberlistPort),
		"OLRIC_PEERS="+cfg.memberlistPeers,
		"OLRIC_MEMBERLIST_ENV=local",
		"OLRIC_LOG_LEVEL=WARN",
	)
	if err := cmd.Start(); err != nil {
		t.Fatalf("start host olric-node: %v", err)
	}

	node := &hostOlricNode{
		nodeID:       cfg.nodeID,
		repoRoot:     repoRoot,
		clientBinary: clientBinary,
		bindAddr:     cfg.bindAddr,
		olricPort:    olricPort,
		cmd:          cmd,
		errCh:        make(chan error, 1),
		logs:         logs,
	}
	go func() {
		node.errCh <- cmd.Wait()
	}()

	waitForText(t, parent, logs, hostStorageLog(cfg.mysqlDSN))
	waitForText(t, parent, logs, "received topology push")
	waitForText(t, parent, logs, "olric node bootstrap complete")
	return node
}

func hostStorageMode(mysqlDSN string) string {
	if mysqlDSN == "" {
		return "wal"
	}
	return "mysql"
}

func hostStorageLog(mysqlDSN string) string {
	if mysqlDSN == "" {
		return "storage mode: memory + local wal"
	}
	return "storage mode: memory + local wal + mysql"
}

func (n *hostOlricNode) put(ctx context.Context, dmap, key, value string) (string, error) {
	cmd := exec.CommandContext(ctx,
		n.clientBinary,
		"-addr", fmt.Sprintf("%s:%d", n.bindAddr, n.olricPort),
		"-op", "put",
		"-dmap", dmap,
		"-key", key,
		"-value", value,
		"-timeout", "10s",
	)
	cmd.Dir = n.repoRoot
	out, err := cmd.CombinedOutput()
	return string(out), err
}

func (n *hostOlricNode) get(ctx context.Context, dmap, key string) (string, string, error) {
	cmd := exec.CommandContext(ctx,
		n.clientBinary,
		"-addr", fmt.Sprintf("%s:%d", n.bindAddr, n.olricPort),
		"-op", "get",
		"-dmap", dmap,
		"-key", key,
		"-timeout", "10s",
	)
	cmd.Dir = n.repoRoot
	out, err := cmd.CombinedOutput()
	return strings.TrimSpace(string(out)), string(out), err
}

func (n *hostOlricNode) stop(t *testing.T) {
	t.Helper()

	if n.cmd.Process != nil {
		_ = n.cmd.Process.Signal(syscall.SIGTERM)
	}
	select {
	case err := <-n.errCh:
		if err != nil {
			t.Logf("host olric-node exited after SIGTERM: %v\nlogs:\n%s", err, strings.TrimSpace(n.logs.String()))
		}
	case <-time.After(10 * time.Second):
		_ = n.cmd.Process.Kill()
		t.Fatalf("host olric-node did not exit after SIGTERM\nlogs:\n%s", strings.TrimSpace(n.logs.String()))
	}
}

func waitForPutFailure(t *testing.T, ctx context.Context, node *hostOlricNode, dmap, key, value string) {
	t.Helper()

	deadline := time.Now().Add(10 * time.Second)
	var lastOut string
	for time.Now().Before(deadline) {
		out, err := node.put(ctx, dmap, key, value)
		lastOut = out
		if err != nil {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatalf("context ended waiting for put failure: %v\nlast output:\n%s\nnode logs:\n%s", ctx.Err(), strings.TrimSpace(lastOut), strings.TrimSpace(node.logs.String()))
		case <-time.After(200 * time.Millisecond):
		}
	}
	t.Fatalf("put unexpectedly kept succeeding after watchdog demotion\nlast output:\n%s\nnode logs:\n%s", strings.TrimSpace(lastOut), strings.TrimSpace(node.logs.String()))
}

func waitForGetValue(t *testing.T, ctx context.Context, reader *hostOlricNode, dmap, key, want string, logNodes ...*hostOlricNode) {
	t.Helper()

	deadline := time.Now().Add(10 * time.Second)
	var lastGot, lastOut string
	var lastErr error
	for time.Now().Before(deadline) {
		got, out, err := reader.get(ctx, dmap, key)
		lastGot, lastOut, lastErr = got, out, err
		if err == nil && got == want {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatalf("context ended waiting for %s/%s=%q through %s: %v\nlast got=%q err=%v out=%s\n%s",
				dmap, key, want, reader.nodeID, ctx.Err(), lastGot, lastErr, strings.TrimSpace(lastOut), hostNodeLogs(logNodes...))
		case <-time.After(200 * time.Millisecond):
		}
	}
	t.Fatalf("timed out waiting for %s/%s=%q through %s\nlast got=%q err=%v out=%s\n%s",
		dmap, key, want, reader.nodeID, lastGot, lastErr, strings.TrimSpace(lastOut), hostNodeLogs(logNodes...))
}

func hostNodeLogs(nodes ...*hostOlricNode) string {
	var b strings.Builder
	for _, node := range nodes {
		if node == nil {
			continue
		}
		fmt.Fprintf(&b, "%s logs:\n%s\n", node.nodeID, strings.TrimSpace(node.logs.String()))
	}
	return strings.TrimSpace(b.String())
}

func startHostWatchdog(t *testing.T, ctx context.Context) (string, *topology.Service, func()) {
	t.Helper()
	return startHostWatchdogWithConfig(t, ctx, hostWatchdogConfig{
		suspectAfter:     time.Second,
		expireAfter:      2 * time.Second,
		reapInterval:     200 * time.Millisecond,
		bookwormInterval: 200 * time.Millisecond,
		leaseTTL:         2 * time.Second,
	})
}

type hostWatchdogConfig struct {
	suspectAfter     time.Duration
	expireAfter      time.Duration
	reapInterval     time.Duration
	bookwormInterval time.Duration
	leaseTTL         time.Duration
}

func startHostWatchdogWithConfig(t *testing.T, ctx context.Context, cfg hostWatchdogConfig) (string, *topology.Service, func()) {
	t.Helper()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen host watchdog: %v", err)
	}

	service := topology.NewServiceWithConfig(topology.Config{
		SuspectAfter:     cfg.suspectAfter,
		ExpireAfter:      cfg.expireAfter,
		ReapInterval:     cfg.reapInterval,
		BookwormInterval: cfg.bookwormInterval,
		LeaseTTL:         cfg.leaseTTL,
		WatchdogID:       "wd-primary",
		Generation:       1,
		Role:             topologypb.WatchdogRole_WATCHDOG_ROLE_PRIMARY,
	})
	go service.RunReaper(ctx)
	go service.RunBookworm(ctx)

	server := grpc.NewServer()
	topologypb.RegisterTopologyControlServer(server, service)
	go func() {
		_ = server.Serve(listener)
	}()

	return listener.Addr().String(), service, func() {
		server.GracefulStop()
		_ = listener.Close()
	}
}

func buildBinary(t *testing.T, ctx context.Context, repoRoot, outputPath, pkg string, env []string) string {
	t.Helper()

	cmd := exec.CommandContext(ctx, "go", "build", "-o", outputPath, pkg)
	cmd.Dir = repoRoot
	cmd.Env = append(os.Environ(), env...)
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("build %s: %v (%s)", pkg, err, strings.TrimSpace(string(out)))
	}
	return outputPath
}

func repoRoot(t *testing.T) string {
	t.Helper()

	wd, err := os.Getwd()
	if err != nil {
		t.Fatalf("get working dir: %v", err)
	}
	root := filepath.Clean(filepath.Join(wd, "..", ".."))
	if _, err := os.Stat(filepath.Join(root, "go.mod")); err != nil {
		t.Fatalf("resolve repo root: %v", err)
	}
	return root
}

type lockedBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *lockedBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *lockedBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

func (b *lockedBuffer) Len() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Len()
}

func (b *lockedBuffer) StringFrom(offset int) string {
	b.mu.Lock()
	defer b.mu.Unlock()
	if offset < 0 {
		offset = 0
	}
	if offset > b.buf.Len() {
		offset = b.buf.Len()
	}
	return b.buf.String()[offset:]
}

func waitForText(t *testing.T, ctx context.Context, buf *lockedBuffer, needle string) {
	t.Helper()

	deadline := time.Now().Add(45 * time.Second)
	for time.Now().Before(deadline) {
		if strings.Contains(buf.String(), needle) {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatalf("context ended waiting for %q: %v\nlogs:\n%s", needle, ctx.Err(), strings.TrimSpace(buf.String()))
		case <-time.After(100 * time.Millisecond):
		}
	}
	t.Fatalf("logs did not contain %q; logs:\n%s", needle, strings.TrimSpace(buf.String()))
}

func waitForTextAfter(t *testing.T, ctx context.Context, buf *lockedBuffer, offset int, needle string) {
	t.Helper()

	deadline := time.Now().Add(45 * time.Second)
	for time.Now().Before(deadline) {
		if strings.Contains(buf.StringFrom(offset), needle) {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatalf("context ended waiting for %q after offset %d: %v\nlogs:\n%s", needle, offset, ctx.Err(), strings.TrimSpace(buf.String()))
		case <-time.After(100 * time.Millisecond):
		}
	}
	t.Fatalf("logs after offset %d did not contain %q; logs:\n%s", offset, needle, strings.TrimSpace(buf.String()))
}

func waitForTopologyWithoutMemberAfter(t *testing.T, ctx context.Context, buf *lockedBuffer, offset int, nodeID string) {
	t.Helper()

	deadline := time.Now().Add(45 * time.Second)
	for time.Now().Before(deadline) {
		for _, line := range strings.Split(buf.StringFrom(offset), "\n") {
			if strings.Contains(line, "received topology push") &&
				strings.Contains(line, "members=[") &&
				!strings.Contains(line, `node_id:"`+nodeID+`"`) {
				return
			}
		}
		select {
		case <-ctx.Done():
			t.Fatalf("context ended waiting for topology without %q after offset %d: %v\nlogs:\n%s", nodeID, offset, ctx.Err(), strings.TrimSpace(buf.String()))
		case <-time.After(100 * time.Millisecond):
		}
	}
	t.Fatalf("logs after offset %d did not contain topology without %q; logs:\n%s", offset, nodeID, strings.TrimSpace(buf.String()))
}

type tcpProxy struct {
	listener net.Listener
	target   string
	mu       sync.Mutex
	paused   bool
	closed   bool
	conns    map[net.Conn]struct{}
}

func startTCPProxy(t *testing.T, ctx context.Context, target string) *tcpProxy {
	t.Helper()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen tcp proxy: %v", err)
	}
	p := &tcpProxy{
		listener: listener,
		target:   target,
		conns:    make(map[net.Conn]struct{}),
	}
	go p.serve(ctx)
	return p
}

func (p *tcpProxy) addr() string {
	return p.listener.Addr().String()
}

func (p *tcpProxy) pause() {
	p.mu.Lock()
	p.paused = true
	conns := p.snapshotConnsLocked()
	p.mu.Unlock()
	for _, conn := range conns {
		_ = conn.Close()
	}
}

func (p *tcpProxy) resume() {
	p.mu.Lock()
	p.paused = false
	p.mu.Unlock()
}

func (p *tcpProxy) close(t *testing.T) {
	t.Helper()

	p.mu.Lock()
	p.closed = true
	conns := p.snapshotConnsLocked()
	p.mu.Unlock()
	for _, conn := range conns {
		_ = conn.Close()
	}
	_ = p.listener.Close()
}

func (p *tcpProxy) serve(ctx context.Context) {
	go func() {
		<-ctx.Done()
		_ = p.listener.Close()
	}()
	for {
		client, err := p.listener.Accept()
		if err != nil {
			return
		}
		if p.isPausedOrClosed() {
			_ = client.Close()
			continue
		}
		go p.handle(client)
	}
}

func (p *tcpProxy) handle(client net.Conn) {
	server, err := net.Dial("tcp", p.target)
	if err != nil {
		_ = client.Close()
		return
	}
	p.track(client)
	p.track(server)
	defer p.untrack(client)
	defer p.untrack(server)
	defer client.Close()
	defer server.Close()

	done := make(chan struct{}, 2)
	go proxyCopy(server, client, done)
	go proxyCopy(client, server, done)
	<-done
}

func proxyCopy(dst, src net.Conn, done chan<- struct{}) {
	_, _ = io.Copy(dst, src)
	_ = dst.Close()
	_ = src.Close()
	done <- struct{}{}
}

func (p *tcpProxy) track(conn net.Conn) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed || p.paused {
		_ = conn.Close()
		return
	}
	p.conns[conn] = struct{}{}
}

func (p *tcpProxy) untrack(conn net.Conn) {
	p.mu.Lock()
	defer p.mu.Unlock()
	delete(p.conns, conn)
}

func (p *tcpProxy) isPausedOrClosed() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.paused || p.closed
}

func (p *tcpProxy) snapshotConnsLocked() []net.Conn {
	conns := make([]net.Conn, 0, len(p.conns))
	for conn := range p.conns {
		conns = append(conns, conn)
	}
	return conns
}
