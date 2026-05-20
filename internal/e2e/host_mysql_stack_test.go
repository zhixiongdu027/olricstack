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

	node := startHostOlricNode(t, ctx, watchdogAddr, "host-only-node")
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
	if record.OwnerSeq <= 0 {
		t.Fatalf("expected mysql record owner_seq > 0, got %d", record.OwnerSeq)
	}
}

func TestHostOnlyWatchdogDemotionBlocksAndPromotionRestoresWrites(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	mysql := provisionHostMySQL(t, "127.0.0.1")
	watchdogAddr, service, cleanupWatchdog := startHostWatchdog(t, ctx)
	defer cleanupWatchdog()

	node := startHostOlricNode(t, ctx, watchdogAddr, "host-only-failover-node")
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
	first := startHostOlricNode(t, ctx, watchdogAddr, "host-only-read-through-writer")
	if out, err := first.put(ctx, "users", key, "value-from-mysql"); err != nil {
		first.stop(t)
		t.Fatalf("write before restart: %v (%s)\nnode logs:\n%s", err, strings.TrimSpace(out), strings.TrimSpace(first.logs.String()))
	}
	waitForMySQLRecord(t, ctx, mysql.hostDSN(), "users", key)
	first.stop(t)

	second := startHostOlricNode(t, ctx, watchdogAddr, "host-only-read-through-reader")
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

	key := waitForPutSuccess(t, ctx, first, "users", "host-kill-rejoin", "value-after-rejoin", first, replacement)
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

func TestHostOnlyFiveNodeJoinCrashReplacementJitterAndStorm(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
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

	repoRoot := repoRoot(t)
	binDir := t.TempDir()
	binaries := hostNodeBinaries{
		nodeBinary:   buildBinary(t, ctx, repoRoot, filepath.Join(binDir, "olric-node"), "./cmd/olric-node", nil),
		clientBinary: buildBinary(t, ctx, repoRoot, filepath.Join(binDir, "olric-e2e-client"), "./cmd/olric-e2e-client", nil),
	}

	memberlistPort := mustFreeTCPPort(t)
	nodes := make([]*hostOlricNode, 0, 5)
	killedNodes := make(map[*hostOlricNode]bool)
	defer func() {
		for _, node := range nodes {
			if !killedNodes[node] {
				node.stop(t)
			}
		}
	}()
	for i := 0; i < 5; i++ {
		nodeID := fmt.Sprintf("host-only-five-%c", 'a'+rune(i))
		node := startHostOlricNodeWithConfig(t, ctx, hostNodeConfig{
			watchdogAddr:   proxy.addr(),
			nodeID:         nodeID,
			podIP:          fmt.Sprintf("127.0.0.%d", i+2),
			bindAddr:       fmt.Sprintf("127.0.0.%d", i+2),
			memberlistPort: memberlistPort,
			binaries:       binaries,
		})
		nodes = append(nodes, node)
		waitForClusterAwareness(t, ctx, nodes)
	}

	runHostTrafficStorm(t, ctx, nodes, 10, 10, "five-before-jitter")

	proxy.pause()
	for _, node := range nodes {
		waitForText(t, ctx, node.logs, "topology subscription ended")
	}
	waitForPutFailure(t, ctx, nodes[0], "users", "five-jitter-blocked:"+strconv.FormatInt(time.Now().UnixNano(), 10), "during-jitter")

	recoveryOffsets := make(map[*hostOlricNode]int, len(nodes))
	for _, node := range nodes {
		recoveryOffsets[node] = node.logs.Len()
	}
	proxy.resume()
	for _, node := range nodes {
		waitForTextAfter(t, ctx, node.logs, recoveryOffsets[node], "TOPOLOGY_REASON_BOOKWORM")
	}
	runHostTrafficStorm(t, ctx, nodes, 8, 8, "five-after-jitter")

	victim := nodes[2]
	survivors := append([]*hostOlricNode(nil), nodes[:2]...)
	survivors = append(survivors, nodes[3:]...)
	pruneOffset := survivors[0].logs.Len()
	victim.kill(t)
	killedNodes[victim] = true
	waitForTopologyWithoutMemberAfter(t, ctx, survivors[0].logs, pruneOffset, victim.nodeID)

	replacement := startHostOlricNodeWithConfig(t, ctx, hostNodeConfig{
		watchdogAddr:   proxy.addr(),
		nodeID:         "host-only-five-f",
		podIP:          "127.0.0.7",
		bindAddr:       "127.0.0.7",
		memberlistPort: memberlistPort,
		binaries:       binaries,
	})
	nodes = append(nodes, replacement)
	active := append(survivors, replacement)
	waitForClusterAwareness(t, ctx, active)

	runHostTrafficStorm(t, ctx, active, 10, 10, "five-after-replacement")
}

func TestHostOnlyFiveNodeConcurrentCrashJitterReplacementAndTraffic(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
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

	repoRoot := repoRoot(t)
	binDir := t.TempDir()
	binaries := hostNodeBinaries{
		nodeBinary:   buildBinary(t, ctx, repoRoot, filepath.Join(binDir, "olric-node"), "./cmd/olric-node", nil),
		clientBinary: buildBinary(t, ctx, repoRoot, filepath.Join(binDir, "olric-e2e-client"), "./cmd/olric-e2e-client", nil),
	}

	memberlistPort := mustFreeTCPPort(t)
	nodes := make([]*hostOlricNode, 0, 6)
	killedNodes := make(map[*hostOlricNode]bool)
	defer func() {
		for _, node := range nodes {
			if !killedNodes[node] {
				node.stop(t)
			}
		}
	}()

	for i := 0; i < 5; i++ {
		nodeID := fmt.Sprintf("host-only-concurrent-%c", 'a'+rune(i))
		node := startHostOlricNodeWithConfig(t, ctx, hostNodeConfig{
			watchdogAddr:   proxy.addr(),
			nodeID:         nodeID,
			podIP:          fmt.Sprintf("127.0.0.%d", i+2),
			bindAddr:       fmt.Sprintf("127.0.0.%d", i+2),
			memberlistPort: memberlistPort,
			binaries:       binaries,
		})
		nodes = append(nodes, node)
		waitForClusterAwareness(t, ctx, nodes)
	}

	active := append([]*hostOlricNode(nil), nodes...)
	var activeMu sync.RWMutex
	trafficCtx, stopTraffic := context.WithCancel(ctx)
	trafficDone := startBackgroundHostTraffic(t, trafficCtx, &activeMu, &active, 3, "concurrent")

	time.Sleep(500 * time.Millisecond)
	victim := nodes[1]
	proxy.pause()
	victim.kill(t)
	killedNodes[victim] = true
	stopTraffic()
	result := <-trafficDone
	if result.successes == 0 {
		t.Fatalf("concurrent traffic had no successful operations before disruption; failures=%d\n%s", result.failures, hostNodeLogs(nodes...))
	}
	if result.failures == 0 {
		t.Fatalf("concurrent disruption did not produce any transient failures; successes=%d", result.successes)
	}

	survivors := append([]*hostOlricNode(nil), nodes[:1]...)
	survivors = append(survivors, nodes[2:]...)
	activeMu.Lock()
	active = append([]*hostOlricNode(nil), survivors...)
	activeMu.Unlock()

	for _, node := range survivors {
		waitForText(t, ctx, node.logs, "topology subscription ended")
	}

	recoveryOffsets := make(map[*hostOlricNode]int, len(survivors))
	for _, node := range survivors {
		recoveryOffsets[node] = node.logs.Len()
	}
	proxy.resume()
	waitForTopologyWithoutMemberAfter(t, ctx, nodes[0].logs, recoveryOffsets[nodes[0]], victim.nodeID)
	for _, node := range survivors {
		waitForTextAfter(t, ctx, node.logs, recoveryOffsets[node], "TOPOLOGY_REASON_BOOKWORM")
	}

	replacement := startHostOlricNodeWithConfig(t, ctx, hostNodeConfig{
		watchdogAddr:   proxy.addr(),
		nodeID:         "host-only-concurrent-f",
		podIP:          "127.0.0.7",
		bindAddr:       "127.0.0.7",
		memberlistPort: memberlistPort,
		binaries:       binaries,
	})
	nodes = append(nodes, replacement)
	recovered := append(survivors, replacement)
	activeMu.Lock()
	active = append([]*hostOlricNode(nil), recovered...)
	activeMu.Unlock()
	waitForClusterAwareness(t, ctx, recovered)

	runHostTrafficStorm(t, ctx, recovered, 10, 10, "concurrent-after-recovery")
}

type hostOlricNode struct {
	nodeID       string
	repoRoot     string
	clientBinary string
	bindAddr     string
	olricPort    int
	cmd          *exec.Cmd
	sidecarCmd   *exec.Cmd
	errCh        chan error
	sidecarErrCh chan error
	logs         *lockedBuffer
}

type hostNodeBinaries struct {
	nodeBinary   string
	clientBinary string
}

func (n *hostOlricNode) kill(t *testing.T) {
	t.Helper()
	if n.cmd.Process != nil {
		_ = n.cmd.Process.Kill()
	}
	if n.sidecarCmd != nil && n.sidecarCmd.Process != nil {
		_ = n.sidecarCmd.Process.Kill()
	}
	select {
	case <-n.errCh:
	case <-time.After(10 * time.Second):
		t.Fatalf("host olric-node did not exit after SIGKILL\nlogs:\n%s", strings.TrimSpace(n.logs.String()))
	}
	select {
	case <-n.sidecarErrCh:
	case <-time.After(10 * time.Second):
		t.Fatalf("host olric-sidecar did not exit after SIGKILL\nlogs:\n%s", strings.TrimSpace(n.logs.String()))
	}
}

func (n *hostOlricNode) suspend(t *testing.T) {
	t.Helper()
	if n.cmd.Process == nil {
		t.Fatalf("host olric-node %s has no process", n.nodeID)
	}
	if err := n.cmd.Process.Signal(syscall.SIGSTOP); err != nil {
		t.Fatalf("SIGSTOP host olric-node %s: %v", n.nodeID, err)
	}
}

func (n *hostOlricNode) resume(t *testing.T) {
	t.Helper()
	if n.cmd.Process == nil {
		t.Fatalf("host olric-node %s has no process", n.nodeID)
	}
	if err := n.cmd.Process.Signal(syscall.SIGCONT); err != nil {
		t.Fatalf("SIGCONT host olric-node %s: %v", n.nodeID, err)
	}
}

func startHostOlricNode(t *testing.T, parent context.Context, watchdogAddr, nodeID string) *hostOlricNode {
	t.Helper()
	return startHostOlricNodeWithConfig(t, parent, hostNodeConfig{
		watchdogAddr: watchdogAddr,
		nodeID:       nodeID,
		podIP:        "127.0.0.1",
		bindAddr:     "127.0.0.1",
	})
}

type hostNodeConfig struct {
	watchdogAddr    string
	nodeID          string
	podIP           string
	bindAddr        string
	memberlistPort  int
	memberlistPeers string
	binaries        hostNodeBinaries
	extraEnv        []string
	replicaCount    int
	writeQuorum     int
	readQuorum      int
	memberQuorum    int
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
	nodeBinary := cfg.binaries.nodeBinary
	clientBinary := cfg.binaries.clientBinary
	if nodeBinary == "" || clientBinary == "" {
		binDir := t.TempDir()
		nodeBinary = buildBinary(t, parent, repoRoot, filepath.Join(binDir, "olric-node"), "./cmd/olric-node", nil)
		clientBinary = buildBinary(t, parent, repoRoot, filepath.Join(binDir, "olric-e2e-client"), "./cmd/olric-e2e-client", nil)
	}
	olricPort := mustFreeTCPPort(t)
	memberlistPort := mustFreeTCPPort(t)
	if cfg.memberlistPort > 0 {
		memberlistPort = cfg.memberlistPort
	}
	replicaCount := cfg.replicaCount
	if replicaCount <= 0 {
		replicaCount = 1
	}
	writeQuorum := cfg.writeQuorum
	if writeQuorum <= 0 {
		writeQuorum = 1
	}
	readQuorum := cfg.readQuorum
	if readQuorum <= 0 {
		readQuorum = 1
	}
	memberQuorum := cfg.memberQuorum
	if memberQuorum <= 0 {
		memberQuorum = 1
	}
	logs := &lockedBuffer{}

	sharedDir := t.TempDir()
	ringPath := filepath.Join(sharedDir, "oplog.ring")
	socketPath := filepath.Join(sharedDir, "oplog.sock")

	sidecarBinary := buildBinary(t, parent, repoRoot, filepath.Join(t.TempDir(), "olric-sidecar"), "./cmd/olric-sidecar", nil)
	sidecarCmd := exec.Command(sidecarBinary)
	sidecarCmd.Dir = repoRoot
	sidecarCmd.Stdout = logs
	sidecarCmd.Stderr = logs
	sidecarCmd.Env = append(os.Environ(),
		"MYSQL_DSN="+os.Getenv("E2E_MYSQL_DSN"),
		"RING_PATH="+ringPath,
		"CONTROL_SOCKET="+socketPath,
		"RING_CAPACITY_BYTES=8388608",
	)
	if err := sidecarCmd.Start(); err != nil {
		t.Fatalf("start host olric-sidecar: %v", err)
	}

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
		"RING_PATH="+ringPath,
		"CONTROL_SOCKET="+socketPath,
		"HEARTBEAT_INTERVAL=100ms",
		"WATCHDOG_RECONNECT_INTERVAL=100ms",
		"OLRIC_BIND_ADDR="+cfg.bindAddr,
		"OLRIC_BIND_PORT="+strconv.Itoa(olricPort),
		"OLRIC_MEMBERLIST_BIND_ADDR="+cfg.bindAddr,
		"OLRIC_MEMBERLIST_BIND_PORT="+strconv.Itoa(memberlistPort),
		"OLRIC_ADVERTISE_ADDR="+cfg.bindAddr,
		"OLRIC_ADVERTISE_PORT="+strconv.Itoa(memberlistPort),
		"OLRIC_PEERS="+cfg.memberlistPeers,
		"OLRIC_REPLICA_COUNT="+strconv.Itoa(replicaCount),
		"OLRIC_WRITE_QUORUM="+strconv.Itoa(writeQuorum),
		"OLRIC_READ_QUORUM="+strconv.Itoa(readQuorum),
		"OLRIC_MEMBER_COUNT_QUORUM="+strconv.Itoa(memberQuorum),
		"OLRIC_MEMBERLIST_ENV=local",
		"OLRIC_LOG_LEVEL=WARN",
	)
	cmd.Env = append(cmd.Env, cfg.extraEnv...)
	if err := cmd.Start(); err != nil {
		_ = sidecarCmd.Process.Kill()
		t.Fatalf("start host olric-node: %v", err)
	}

	node := &hostOlricNode{
		nodeID:       cfg.nodeID,
		repoRoot:     repoRoot,
		clientBinary: clientBinary,
		bindAddr:     cfg.bindAddr,
		olricPort:    olricPort,
		cmd:          cmd,
		sidecarCmd:   sidecarCmd,
		errCh:        make(chan error, 1),
		sidecarErrCh: make(chan error, 1),
		logs:         logs,
	}
	go func() {
		node.errCh <- cmd.Wait()
	}()
	go func() {
		node.sidecarErrCh <- sidecarCmd.Wait()
	}()

	waitForText(t, parent, logs, "received topology push")
	waitForText(t, parent, logs, "olric node bootstrap complete")
	waitForText(t, parent, logs, "sidecar bootstrap complete")
	return node
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
	if n.sidecarCmd != nil && n.sidecarCmd.Process != nil {
		_ = n.sidecarCmd.Process.Signal(syscall.SIGTERM)
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
	select {
	case err := <-n.sidecarErrCh:
		if err != nil {
			t.Logf("host olric-sidecar exited after SIGTERM: %v\nlogs:\n%s", err, strings.TrimSpace(n.logs.String()))
		}
	case <-time.After(10 * time.Second):
		_ = n.sidecarCmd.Process.Kill()
		t.Fatalf("host olric-sidecar did not exit after SIGTERM\nlogs:\n%s", strings.TrimSpace(n.logs.String()))
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

func waitForPutSuccess(t *testing.T, ctx context.Context, node *hostOlricNode, dmap, keyPrefix, value string, logNodes ...*hostOlricNode) string {
	t.Helper()

	deadline := time.Now().Add(10 * time.Second)
	var lastKey, lastOut string
	var lastErr error
	for time.Now().Before(deadline) {
		lastKey = keyPrefix + ":" + strconv.FormatInt(time.Now().UnixNano(), 10)
		lastOut, lastErr = node.put(ctx, dmap, lastKey, value)
		if lastErr == nil {
			return lastKey
		}
		select {
		case <-ctx.Done():
			t.Fatalf("context ended waiting for put success through %s: %v\nlast key=%s err=%v out=%s\n%s",
				node.nodeID, ctx.Err(), lastKey, lastErr, strings.TrimSpace(lastOut), hostNodeLogs(logNodes...))
		case <-time.After(200 * time.Millisecond):
		}
	}
	t.Fatalf("timed out waiting for put success through %s\nlast key=%s err=%v out=%s\n%s",
		node.nodeID, lastKey, lastErr, strings.TrimSpace(lastOut), hostNodeLogs(logNodes...))
	return ""
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

func waitForClusterAwareness(t *testing.T, ctx context.Context, nodes []*hostOlricNode) {
	t.Helper()

	for _, node := range nodes {
		for _, other := range nodes {
			if node == other {
				continue
			}
			waitForText(t, ctx, node.logs, `node_id:"`+other.nodeID+`"`)
		}
		if len(nodes) > 1 {
			waitForText(t, ctx, node.logs, "joined topology peers")
		}
	}
}

func runHostTrafficStorm(t *testing.T, ctx context.Context, nodes []*hostOlricNode, writers, writesPerWorker int, prefix string) {
	t.Helper()
	if len(nodes) < 2 {
		t.Fatalf("traffic storm requires at least two nodes, got %d", len(nodes))
	}

	var failures atomic.Int64
	var wg sync.WaitGroup
	for worker := 0; worker < writers; worker++ {
		worker := worker
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < writesPerWorker; i++ {
				target := nodes[(worker+i)%len(nodes)]
				reader := nodes[(worker+i+2)%len(nodes)]
				if reader == target {
					reader = nodes[(worker+i+1)%len(nodes)]
				}
				key := fmt.Sprintf("%s:%d:%d:%d", prefix, time.Now().UnixNano(), worker, i)
				value := "value-" + key
				if out, err := target.put(ctx, "users", key, value); err != nil {
					t.Logf("storm put failed prefix=%s target=%s key=%s err=%v out=%s", prefix, target.nodeID, key, err, strings.TrimSpace(out))
					failures.Add(1)
					continue
				}
				got, out, err := reader.get(ctx, "users", key)
				if err != nil || got != value {
					t.Logf("storm get failed prefix=%s reader=%s key=%s got=%q err=%v out=%s", prefix, reader.nodeID, key, got, err, strings.TrimSpace(out))
					failures.Add(1)
				}
			}
		}()
	}
	wg.Wait()
	if got := failures.Load(); got != 0 {
		t.Fatalf("traffic storm %s had %d failures\n%s", prefix, got, hostNodeLogs(nodes...))
	}
}

type backgroundTrafficResult struct {
	successes int64
	failures  int64
}

func startBackgroundHostTraffic(t *testing.T, ctx context.Context, activeMu *sync.RWMutex, active *[]*hostOlricNode, writers int, prefix string) <-chan backgroundTrafficResult {
	t.Helper()
	if writers <= 0 {
		writers = 1
	}

	done := make(chan backgroundTrafficResult, 1)
	var successes atomic.Int64
	var failures atomic.Int64
	var wg sync.WaitGroup
	for worker := 0; worker < writers; worker++ {
		worker := worker
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; ; i++ {
				select {
				case <-ctx.Done():
					return
				default:
				}

				activeMu.RLock()
				nodes := append([]*hostOlricNode(nil), (*active)...)
				activeMu.RUnlock()
				if len(nodes) < 2 {
					failures.Add(1)
					time.Sleep(50 * time.Millisecond)
					continue
				}

				target := nodes[(worker+i)%len(nodes)]
				reader := nodes[(worker+i+1)%len(nodes)]
				key := fmt.Sprintf("%s-bg:%d:%d:%d", prefix, time.Now().UnixNano(), worker, i)
				value := "value-" + key
				if _, err := target.put(ctx, "users", key, value); err != nil {
					failures.Add(1)
					time.Sleep(25 * time.Millisecond)
					continue
				}
				got, _, err := reader.get(ctx, "users", key)
				if err != nil || got != value {
					failures.Add(1)
					time.Sleep(25 * time.Millisecond)
					continue
				}
				successes.Add(1)
			}
		}()
	}
	go func() {
		wg.Wait()
		done <- backgroundTrafficResult{
			successes: successes.Load(),
			failures:  failures.Load(),
		}
	}()
	return done
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
