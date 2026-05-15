//go:build e2e

package e2e

import (
	"bytes"
	"context"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
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

func startHostWatchdog(t *testing.T, ctx context.Context) (string, *topology.Service, func()) {
	t.Helper()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen host watchdog: %v", err)
	}

	service := topology.NewServiceWithConfig(topology.Config{
		SuspectAfter:     time.Second,
		ExpireAfter:      2 * time.Second,
		ReapInterval:     200 * time.Millisecond,
		BookwormInterval: 200 * time.Millisecond,
		LeaseTTL:         2 * time.Second,
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
