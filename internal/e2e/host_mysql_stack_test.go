//go:build e2e

package e2e

import (
	"bytes"
	"context"
	"encoding/hex"
	"fmt"
	"io"
	"io/fs"
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
	"github.com/zhixiongdu/olricstack/internal/stringkv"
	"github.com/zhixiongdu/olricstack/internal/topology"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/encoding/protojson"
)

const hostRESPDMap = stringkv.DefaultDMap

func TestHostOnlyMySQLDurableWritePath(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	mysql := provisionHostMySQL(t, "127.0.0.1")
	watchdogAddr, _, cleanupWatchdog := startHostWatchdog(t, ctx)
	defer cleanupWatchdog()

	node := startHostOlricNodeWithConfig(t, ctx, hostNodeConfig{
		watchdogAddr: watchdogAddr,
		nodeID:       "host-only-node",
		mysqlDSN:     mysql.hostDSN(),
	})
	defer node.stop(t)

	key := "host-user:" + strconv.FormatInt(time.Now().UnixNano(), 10)
	if out, err := node.put(ctx, "users", key, "value-host-only"); err != nil {
		t.Fatalf("write host dmap entry: %v (%s)\nnode logs:\n%s", err, strings.TrimSpace(out), strings.TrimSpace(node.logs.String()))
	}

	record := waitForMySQLRecord(t, ctx, mysql.hostDSN(), hostRESPDMap, key)
	if !strings.HasPrefix(record.WriterID, node.nodeID+"-") {
		t.Fatalf("expected mysql writer_id to be derived from %q, got %q", node.nodeID, record.WriterID)
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

	node := startHostOlricNodeWithConfig(t, ctx, hostNodeConfig{
		watchdogAddr: watchdogAddr,
		nodeID:       "host-only-failover-node",
		mysqlDSN:     mysql.hostDSN(),
	})
	defer node.stop(t)

	firstKey := "host-before-demotion:" + strconv.FormatInt(time.Now().UnixNano(), 10)
	if out, err := node.put(ctx, "users", firstKey, "before-demotion"); err != nil {
		t.Fatalf("write before demotion: %v (%s)\nnode logs:\n%s", err, strings.TrimSpace(out), strings.TrimSpace(node.logs.String()))
	}
	waitForMySQLRecord(t, ctx, mysql.hostDSN(), hostRESPDMap, firstKey)

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
	record := waitForMySQLRecord(t, ctx, mysql.hostDSN(), hostRESPDMap, recoveredKey)
	if !strings.HasPrefix(record.WriterID, node.nodeID+"-") {
		t.Fatalf("expected mysql writer_id to be derived from %q after promotion, got %q", node.nodeID, record.WriterID)
	}
}

func TestHostOnlyRestartReadsThroughMySQL(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	mysql := provisionHostMySQL(t, "127.0.0.1")
	watchdogAddr, _, cleanupWatchdog := startHostWatchdog(t, ctx)
	defer cleanupWatchdog()

	key := "host-read-through:" + strconv.FormatInt(time.Now().UnixNano(), 10)
	first := startHostOlricNodeWithConfig(t, ctx, hostNodeConfig{
		watchdogAddr: watchdogAddr,
		nodeID:       "host-only-read-through-writer",
		mysqlDSN:     mysql.hostDSN(),
	})
	if out, err := first.put(ctx, "users", key, "value-from-mysql"); err != nil {
		first.stop(t)
		t.Fatalf("write before restart: %v (%s)\nnode logs:\n%s", err, strings.TrimSpace(out), strings.TrimSpace(first.logs.String()))
	}
	waitForMySQLRecord(t, ctx, mysql.hostDSN(), hostRESPDMap, key)
	first.stop(t)

	second := startHostOlricNodeWithConfig(t, ctx, hostNodeConfig{
		watchdogAddr: watchdogAddr,
		nodeID:       "host-only-read-through-reader",
		mysqlDSN:     mysql.hostDSN(),
	})
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

func TestHostOnlyTwoNodeCRUDAndReadThroughCorrectness(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	mysql := provisionHostMySQL(t, "127.0.0.1")
	watchdogAddr, _, cleanupWatchdog := startHostWatchdog(t, ctx)
	defer cleanupWatchdog()

	memberlistPort := mustFreeTCPPort(t)
	first := startHostOlricNodeWithConfig(t, ctx, hostNodeConfig{
		watchdogAddr:   watchdogAddr,
		nodeID:         "host-only-crud-a",
		podIP:          "127.0.0.2",
		bindAddr:       "127.0.0.2",
		memberlistPort: memberlistPort,
		mysqlDSN:       mysql.hostDSN(),
	})
	second := startHostOlricNodeWithConfig(t, ctx, hostNodeConfig{
		watchdogAddr:   watchdogAddr,
		nodeID:         "host-only-crud-b",
		podIP:          "127.0.0.3",
		bindAddr:       "127.0.0.3",
		memberlistPort: memberlistPort,
		mysqlDSN:       mysql.hostDSN(),
	})

	nodesStopped := false
	defer func() {
		if !nodesStopped {
			first.stop(t)
			second.stop(t)
		}
	}()

	waitForText(t, ctx, first.logs, `node_id:"host-only-crud-b"`)
	waitForText(t, ctx, second.logs, `node_id:"host-only-crud-a"`)
	waitForText(t, ctx, first.logs, "joined topology peers")
	waitForText(t, ctx, second.logs, "joined topology peers")

	key := "host-crud:" + strconv.FormatInt(time.Now().UnixNano(), 10)
	if out, err := first.put(ctx, "users", key, "create-v1"); err != nil {
		t.Fatalf("create through first node: %v (%s)\n%s", err, strings.TrimSpace(out), hostNodeLogs(first, second))
	}
	createRecord := waitForMySQLValue(t, ctx, mysql.hostDSN(), hostRESPDMap, key, "create-v1")
	if createRecord.Tombstone {
		t.Fatalf("expected create record to be live, got tombstone")
	}
	waitForGetValue(t, ctx, second, "users", key, "create-v1", first, second)

	if out, err := second.put(ctx, "users", key, "update-v2"); err != nil {
		t.Fatalf("update through second node: %v (%s)\n%s", err, strings.TrimSpace(out), hostNodeLogs(first, second))
	}
	updateRecord := waitForMySQLValue(t, ctx, mysql.hostDSN(), hostRESPDMap, key, "update-v2")
	if updateRecord.Tombstone {
		t.Fatalf("expected update record to be live, got tombstone")
	}
	waitForGetValue(t, ctx, first, "users", key, "update-v2", first, second)
	waitForGetValue(t, ctx, second, "users", key, "update-v2", first, second)

	if out, err := first.delete(ctx, "users", key); err != nil {
		t.Fatalf("delete through first node: %v (%s)\n%s", err, strings.TrimSpace(out), hostNodeLogs(first, second))
	} else if strings.TrimSpace(out) != "1" {
		t.Fatalf("expected delete to remove one key, got %q\n%s", strings.TrimSpace(out), hostNodeLogs(first, second))
	}
	waitForGetMiss(t, ctx, first, "users", key, first, second)
	waitForGetMiss(t, ctx, second, "users", key, first, second)

	first.stop(t)
	second.stop(t)
	nodesStopped = true

	restarted := startHostOlricNodeWithConfig(t, ctx, hostNodeConfig{
		watchdogAddr: watchdogAddr,
		nodeID:       "host-only-crud-after-delete",
		podIP:        "127.0.0.5",
		bindAddr:     "127.0.0.5",
		mysqlDSN:     mysql.hostDSN(),
	})
	defer restarted.stop(t)
	waitForGetMiss(t, ctx, restarted, "users", key, restarted)
}

func TestHostOnlyTwoNodeExpirePropagatesAndExpiresAcrossNodes(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	mysql := provisionHostMySQL(t, "127.0.0.1")
	watchdogAddr, _, cleanupWatchdog := startHostWatchdog(t, ctx)
	defer cleanupWatchdog()

	memberlistPort := mustFreeTCPPort(t)
	first := startHostOlricNodeWithConfig(t, ctx, hostNodeConfig{
		watchdogAddr:   watchdogAddr,
		nodeID:         "host-only-expire-a",
		podIP:          "127.0.0.2",
		bindAddr:       "127.0.0.2",
		memberlistPort: memberlistPort,
		mysqlDSN:       mysql.hostDSN(),
	})
	second := startHostOlricNodeWithConfig(t, ctx, hostNodeConfig{
		watchdogAddr:   watchdogAddr,
		nodeID:         "host-only-expire-b",
		podIP:          "127.0.0.3",
		bindAddr:       "127.0.0.3",
		memberlistPort: memberlistPort,
		mysqlDSN:       mysql.hostDSN(),
	})
	defer first.stop(t)
	defer second.stop(t)

	waitForText(t, ctx, first.logs, `node_id:"host-only-expire-b"`)
	waitForText(t, ctx, second.logs, `node_id:"host-only-expire-a"`)
	waitForText(t, ctx, first.logs, "joined topology peers")
	waitForText(t, ctx, second.logs, "joined topology peers")

	key := "host-expire:" + strconv.FormatInt(time.Now().UnixNano(), 10)
	if out, err := first.put(ctx, "users", key, "expire-me"); err != nil {
		t.Fatalf("put before expire: %v (%s)\n%s", err, strings.TrimSpace(out), hostNodeLogs(first, second))
	}
	waitForMySQLValue(t, ctx, mysql.hostDSN(), hostRESPDMap, key, "expire-me")
	waitForGetValue(t, ctx, second, "users", key, "expire-me", first, second)

	if out, err := second.expire(ctx, "users", key, 1*time.Second); err != nil {
		t.Fatalf("expire through second node: %v (%s)\n%s", err, strings.TrimSpace(out), hostNodeLogs(first, second))
	} else if strings.TrimSpace(out) != "1" {
		t.Fatalf("expected expire to update one key, got %q\n%s", strings.TrimSpace(out), hostNodeLogs(first, second))
	}

	waitForMySQLValue(t, ctx, mysql.hostDSN(), hostRESPDMap, key, "expire-me")
	time.Sleep(1500 * time.Millisecond)
	waitForGetMiss(t, ctx, first, "users", key, first, second)
	waitForGetMiss(t, ctx, second, "users", key, first, second)

	first.stop(t)
	second.stop(t)

	restarted := startHostOlricNodeWithConfig(t, ctx, hostNodeConfig{
		watchdogAddr: watchdogAddr,
		nodeID:       "host-only-expire-reader",
		podIP:        "127.0.0.5",
		bindAddr:     "127.0.0.5",
		mysqlDSN:     mysql.hostDSN(),
	})
	defer restarted.stop(t)
	waitForGetMiss(t, ctx, restarted, "users", key, restarted)
}

func TestHostOnlyTwoNodeSetWithTTLExpiresAndDoesNotReviveOnRestart(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	mysql := provisionHostMySQL(t, "127.0.0.1")
	watchdogAddr, _, cleanupWatchdog := startHostWatchdog(t, ctx)
	defer cleanupWatchdog()

	memberlistPort := mustFreeTCPPort(t)
	first := startHostOlricNodeWithConfig(t, ctx, hostNodeConfig{
		watchdogAddr:   watchdogAddr,
		nodeID:         "host-only-ttl-a",
		podIP:          "127.0.0.2",
		bindAddr:       "127.0.0.2",
		memberlistPort: memberlistPort,
		mysqlDSN:       mysql.hostDSN(),
	})
	second := startHostOlricNodeWithConfig(t, ctx, hostNodeConfig{
		watchdogAddr:   watchdogAddr,
		nodeID:         "host-only-ttl-b",
		podIP:          "127.0.0.3",
		bindAddr:       "127.0.0.3",
		memberlistPort: memberlistPort,
		mysqlDSN:       mysql.hostDSN(),
	})
	defer bestEffortStopHostNode(first)
	defer bestEffortStopHostNode(second)

	waitForText(t, ctx, first.logs, `node_id:"host-only-ttl-b"`)
	waitForText(t, ctx, second.logs, `node_id:"host-only-ttl-a"`)
	waitForText(t, ctx, first.logs, "joined topology peers")
	waitForText(t, ctx, second.logs, "joined topology peers")

	key := "host-set-ttl:" + strconv.FormatInt(time.Now().UnixNano(), 10)
	if out, err := first.putWithTTL(ctx, "users", key, "ttl-value", 2*time.Second); err != nil {
		t.Fatalf("put with ttl through first node: %v (%s)\n%s", err, strings.TrimSpace(out), hostNodeLogs(first, second))
	}
	waitForMySQLValue(t, ctx, mysql.hostDSN(), hostRESPDMap, key, "ttl-value")
	waitForGetValue(t, ctx, second, "users", key, "ttl-value", first, second)

	time.Sleep(2500 * time.Millisecond)
	waitForGetMiss(t, ctx, first, "users", key, first, second)
	waitForGetMiss(t, ctx, second, "users", key, first, second)

	first.stop(t)
	second.stop(t)

	restarted := startHostOlricNodeWithConfig(t, ctx, hostNodeConfig{
		watchdogAddr: watchdogAddr,
		nodeID:       "host-only-ttl-reader",
		podIP:        "127.0.0.5",
		bindAddr:     "127.0.0.5",
		mysqlDSN:     mysql.hostDSN(),
	})
	defer bestEffortStopHostNode(restarted)
	waitForGetMiss(t, ctx, restarted, "users", key, restarted)
}

func TestHostOnlyRollingRestartPreservesLiveDataAndDropsExpiredData(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()

	suite := newHostDataScenario(t, ctx, hostDataScenarioConfig{
		name:      "host-rolling",
		nodeCount: 2,
	})
	defer suite.close()

	samples := []hostDataSample{
		{Key: suite.key("live-a"), Value: "live-a"},
		{Key: suite.key("live-b"), Value: "live-b"},
		{Key: suite.key("ttl-a"), Value: "ttl-a", TTL: 1500 * time.Millisecond, Expires: true},
		{Key: suite.key("ttl-b"), Value: "ttl-b", TTL: 1500 * time.Millisecond, Expires: true},
	}
	suite.seed(samples)
	suite.expectMySQLValues(samples)
	suite.expectClusterValues(samples)

	time.Sleep(2 * time.Second)
	suite.expectExpiredMissing(samples)

	restarted := suite.restartAsSingleReader("reader")
	suite.expectReaderAfterRestart(restarted, samples)
}

func TestHostDataLifecycleMatrix(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	cases := []struct {
		name    string
		samples []hostDataSample
	}{
		{
			name: "live-restart",
			samples: []hostDataSample{
				{Value: "live-a"},
				{Value: "live-b"},
			},
		},
		{
			name: "ttl-restart",
			samples: []hostDataSample{
				{Value: "ttl-a", TTL: 1200 * time.Millisecond, Expires: true},
				{Value: "ttl-b", TTL: 1200 * time.Millisecond, Expires: true},
			},
		},
		{
			name: "mixed-restart",
			samples: []hostDataSample{
				{Value: "live-c"},
				{Value: "ttl-c", TTL: 1200 * time.Millisecond, Expires: true},
				{Value: "live-d"},
				{Value: "ttl-d", TTL: 1200 * time.Millisecond, Expires: true},
			},
		},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			suite := newHostDataScenario(t, ctx, hostDataScenarioConfig{
				name:      tc.name,
				nodeCount: 2,
			})
			defer suite.close()

			for i := range tc.samples {
				tc.samples[i].Key = suite.key(tc.name + "-" + tc.samples[i].Value)
			}

			suite.seed(tc.samples)
			suite.expectMySQLValues(tc.samples)
			suite.expectClusterValues(tc.samples)

			hasExpiry := false
			for _, sample := range tc.samples {
				if sample.Expires {
					hasExpiry = true
					break
				}
			}
			if hasExpiry {
				time.Sleep(2 * time.Second)
				suite.expectExpiredMissing(tc.samples)
			}

			reader := suite.restartAsSingleReader(tc.name + "-reader")
			suite.expectReaderAfterRestart(reader, tc.samples)
		})
	}
}

func TestHostDataLifecycleUnderBackgroundTraffic(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()

	suite := newHostDataScenario(t, ctx, hostDataScenarioConfig{
		name:      "host-traffic-lifecycle",
		nodeCount: 3,
	})
	defer suite.close()

	stopTraffic, trafficDone, activeMu, active := suite.startBackgroundTraffic(3, "host-lifecycle")
	time.Sleep(750 * time.Millisecond)

	samples := []hostDataSample{
		{Key: suite.key("traffic-live-a"), Value: "traffic-live-a"},
		{Key: suite.key("traffic-live-b"), Value: "traffic-live-b"},
		{Key: suite.key("traffic-ttl-a"), Value: "traffic-ttl-a", TTL: 1500 * time.Millisecond, Expires: true},
		{Key: suite.key("traffic-ttl-b"), Value: "traffic-ttl-b", TTL: 1500 * time.Millisecond, Expires: true},
	}
	suite.seed(samples)
	suite.expectMySQLValues(samples)
	suite.expectClusterValues(samples)

	time.Sleep(2 * time.Second)
	suite.expectExpiredMissing(samples)

	stopTraffic()
	result := <-trafficDone
	if result.successes == 0 {
		t.Fatalf("background traffic made no progress before lifecycle restart; failures=%d\n%s", result.failures, hostNodeLogs(suite.nodes...))
	}
	if result.failures > result.successes {
		t.Fatalf("background traffic was mostly failing before planned shutdown: successes=%d failures=%d\n%s", result.successes, result.failures, hostNodeLogs(suite.nodes...))
	}

	activeMu.Lock()
	*active = nil
	activeMu.Unlock()

	reader := suite.restartAsSingleReader("host-traffic-lifecycle-reader")
	suite.expectReaderAfterRestart(reader, samples)
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
	postReplacementOffsets := make(map[*hostOlricNode]int, len(survivors))
	for _, node := range survivors {
		postReplacementOffsets[node] = node.logs.Len()
	}

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
	for _, node := range survivors {
		waitForTextAfter(t, ctx, node.logs, postReplacementOffsets[node], `node_id:"`+replacement.nodeID+`"`)
		waitForTextAfter(t, ctx, node.logs, postReplacementOffsets[node], "joined topology peers")
		waitForTopologyWithoutMemberAfter(t, ctx, node.logs, postReplacementOffsets[node], victim.nodeID)
	}
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
	postReplacementOffsets := make(map[*hostOlricNode]int, len(survivors))
	for _, node := range survivors {
		postReplacementOffsets[node] = node.logs.Len()
	}
	activeMu.Lock()
	active = append([]*hostOlricNode(nil), recovered...)
	activeMu.Unlock()
	for _, node := range survivors {
		waitForTextAfter(t, ctx, node.logs, postReplacementOffsets[node], `node_id:"`+replacement.nodeID+`"`)
		waitForTextAfter(t, ctx, node.logs, postReplacementOffsets[node], "joined topology peers")
		waitForTopologyWithoutMemberAfter(t, ctx, node.logs, postReplacementOffsets[node], victim.nodeID)
	}
	waitForClusterAwareness(t, ctx, recovered)

	runHostTrafficStorm(t, ctx, recovered, 10, 10, "concurrent-after-recovery")
}

type hostOlricNode struct {
	nodeID       string
	repoRoot     string
	clientBinary string
	bindAddr     string
	olricPort    int
	respPort     int
	cmd          *exec.Cmd
	sidecarCmd   *exec.Cmd
	errCh        chan error
	sidecarErrCh chan error
	logs         *lockedBuffer
	nodeLog       *lockedBuffer  // node stdout+stderr only, raw
	sidecarLog    *lockedBuffer  // sidecar stdout+stderr only, raw
	diagDir       string         // <tempdir>/diag/nodes/<nodeID>
	nodePrefix    *prefixWriter  // for Flush on stop
	sidecarPrefix *prefixWriter  // for Flush on stop
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
	n.nodePrefix.Flush()
	n.sidecarPrefix.Flush()
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
	respPort        int
	memberlistPeers string
	binaries        hostNodeBinaries
	extraEnv        []string
	replicaCount    int
	writeQuorum     int
	readQuorum      int
	memberQuorum    int
	mysqlDSN        string
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
		// Try pre-built binaries from CI artifacts first.
		prebuiltNode := filepath.Join(repoRoot, ".build", "olric-node")
		prebuiltClient := filepath.Join(repoRoot, ".build", "olric-e2e-client")
		if _, err := os.Stat(prebuiltNode); err == nil {
			nodeBinary = prebuiltNode
		} else {
			nodeBinary = buildBinary(t, parent, repoRoot, filepath.Join(t.TempDir(), "olric-node"), "./cmd/olric-node", nil)
		}
		if _, err := os.Stat(prebuiltClient); err == nil {
			clientBinary = prebuiltClient
		} else {
			clientBinary = buildBinary(t, parent, repoRoot, filepath.Join(t.TempDir(), "olric-e2e-client"), "./cmd/olric-e2e-client", nil)
		}
	}
	olricPort := mustFreeTCPPort(t)
	memberlistPort := mustFreeTCPPort(t)
	respPort := cfg.respPort
	if respPort <= 0 {
		respPort = 3321
	}
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
	merged := &lockedBuffer{}
	nodeBuf := &lockedBuffer{}
	sidecarBuf := &lockedBuffer{}

	nodeDiagDir := diagDirForNode(t, cfg.nodeID)
	nodeFile := openDiagFile(t, nodeDiagDir, "node.log")
	sidecarFile := openDiagFile(t, nodeDiagDir, "sidecar.log")

	nodePfx := newPrefixWriter("node", merged)
	sidecarPfx := newPrefixWriter("sidecar", merged)

	nodeW := newTeeWriter(nodeBuf, nodeFile, nodePfx)
	sidecarW := newTeeWriter(sidecarBuf, sidecarFile, sidecarPfx)

	sharedDir := t.TempDir()
	ringPath := filepath.Join(sharedDir, "oplog.ring")
	socketPath := filepath.Join(sharedDir, "oplog.sock")

	// Try pre-built binary from CI artifacts first.
	var sidecarBinary string
	prebuiltSidecar := filepath.Join(repoRoot, ".build", "olric-sidecar")
	if _, err := os.Stat(prebuiltSidecar); err == nil {
		sidecarBinary = prebuiltSidecar
	} else {
		sidecarBinary = buildBinary(t, parent, repoRoot, filepath.Join(t.TempDir(), "olric-sidecar"), "./cmd/olric-sidecar", nil)
	}
	sidecarCmd := exec.Command(sidecarBinary)
	sidecarCmd.Dir = repoRoot
	sidecarCmd.Stdout = sidecarW
	sidecarCmd.Stderr = sidecarW
	sidecarCmd.Env = append(os.Environ(),
		"MYSQL_DSN="+cfg.mysqlDSNOrFallback(),
		"RING_PATH="+ringPath,
		"CONTROL_SOCKET="+socketPath,
		"RING_CAPACITY_BYTES=8388608",
	)
	if err := sidecarCmd.Start(); err != nil {
		t.Fatalf("start host olric-sidecar: %v", err)
	}

	cmd := exec.Command(nodeBinary)
	cmd.Dir = repoRoot
	cmd.Stdout = nodeW
	cmd.Stderr = nodeW
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
		"RESP_BIND_ADDR="+cfg.bindAddr,
		"RESP_BIND_PORT="+strconv.Itoa(respPort),
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
		nodeID:        cfg.nodeID,
		repoRoot:      repoRoot,
		clientBinary:  clientBinary,
		bindAddr:      cfg.bindAddr,
		olricPort:     olricPort,
		respPort:      respPort,
		cmd:           cmd,
		sidecarCmd:    sidecarCmd,
		errCh:         make(chan error, 1),
		sidecarErrCh:  make(chan error, 1),
		logs:          merged,
		nodeLog:       nodeBuf,
		sidecarLog:    sidecarBuf,
		diagDir:       nodeDiagDir,
		nodePrefix:    nodePfx,
		sidecarPrefix: sidecarPfx,
	}
	go func() {
		node.errCh <- cmd.Wait()
	}()
	go func() {
		node.sidecarErrCh <- sidecarCmd.Wait()
	}()

	// Register failure-dump hook for this node: ring file copy + hexdump,
	// per-process log path log, and best-effort ps/lsof per PID.
	{
		n := node
		ringSrc := ringPath
		bundle := e2eDiagFor(t)
		bundle.addDumper(func(ctx context.Context, root string) {
			nodeDiagDir := filepath.Join(root, "nodes", n.nodeID)
			_ = os.MkdirAll(nodeDiagDir, 0o755)

			// Per-process logs are already on disk from Commit 1's teeWriter.
			t.Logf("e2eDiag: node %s logs at %s/{node,sidecar}.log", n.nodeID, n.diagDir)

			// Ring file copy + hexdump of first 4 KiB.
			if data, err := os.ReadFile(ringSrc); err == nil {
				_ = os.WriteFile(filepath.Join(nodeDiagDir, "oplog.ring"), data, 0o644)
				limit := 4096
				if len(data) < limit {
					limit = len(data)
				}
				_ = os.WriteFile(
					filepath.Join(nodeDiagDir, "oplog.ring.hexdump"),
					[]byte(hex.Dump(data[:limit])),
					0o644,
				)
			}

			// Best-effort ps/lsof per PID. Processes are usually torn down by
			// the time t.Cleanup fires (defer stop runs first); the output may
			// be empty, but it's cheap and useful when a test fails mid-flight.
			pids := []int{}
			if n.cmd != nil && n.cmd.Process != nil {
				pids = append(pids, n.cmd.Process.Pid)
			}
			if n.sidecarCmd != nil && n.sidecarCmd.Process != nil {
				pids = append(pids, n.sidecarCmd.Process.Pid)
			}
			for _, pid := range pids {
				if pid <= 0 {
					continue
				}
				if out, err := exec.CommandContext(ctx, "ps", "-p", strconv.Itoa(pid), "-o", "pid,ppid,stat,cmd").CombinedOutput(); err == nil {
					_ = os.WriteFile(filepath.Join(nodeDiagDir, fmt.Sprintf("ps-%d.txt", pid)), out, 0o644)
				}
				if out, err := exec.CommandContext(ctx, "lsof", "-p", strconv.Itoa(pid)).CombinedOutput(); err == nil {
					_ = os.WriteFile(filepath.Join(nodeDiagDir, fmt.Sprintf("lsof-%d.txt", pid)), out, 0o644)
				}
			}
		})
	}

	waitForText(t, parent, merged, "received topology push")
	waitForText(t, parent, merged, "olric node bootstrap complete")
	waitForText(t, parent, merged, "sidecar bootstrap complete")
	return node
}

func (cfg hostNodeConfig) mysqlDSNOrFallback() string {
	if cfg.mysqlDSN != "" {
		return cfg.mysqlDSN
	}
	return envOrDefault("E2E_MYSQL_DSN", "root:password@tcp(127.0.0.1:13306)/mysql?parseTime=true")
}

func (n *hostOlricNode) put(ctx context.Context, dmap, key, value string) (string, error) {
	return n.putWithTTL(ctx, dmap, key, value, 0)
}

func (n *hostOlricNode) putWithTTL(ctx context.Context, dmap, key, value string, ttl time.Duration) (string, error) {
	cmd := exec.CommandContext(ctx,
		n.clientBinary,
		"-addr", fmt.Sprintf("%s:%d", n.bindAddr, n.respPort),
		"-op", "put",
		"-dmap", dmap,
		"-key", key,
		"-value", value,
	)
	if ttl > 0 {
		cmd.Args = append(cmd.Args, "-ttl", ttl.String())
	}
	cmd.Args = append(cmd.Args,
		"-timeout", "10s",
	)
	cmd.Dir = n.repoRoot
	out, err := cmd.CombinedOutput()
	return string(out), err
}

func (n *hostOlricNode) get(ctx context.Context, dmap, key string) (string, string, error) {
	cmd := exec.CommandContext(ctx,
		n.clientBinary,
		"-addr", fmt.Sprintf("%s:%d", n.bindAddr, n.respPort),
		"-op", "get",
		"-dmap", dmap,
		"-key", key,
		"-timeout", "10s",
	)
	cmd.Dir = n.repoRoot
	out, err := cmd.CombinedOutput()
	return strings.TrimSpace(string(out)), string(out), err
}

func (n *hostOlricNode) delete(ctx context.Context, dmap, key string) (string, error) {
	cmd := exec.CommandContext(ctx,
		n.clientBinary,
		"-addr", fmt.Sprintf("%s:%d", n.bindAddr, n.respPort),
		"-op", "delete",
		"-dmap", dmap,
		"-key", key,
		"-timeout", "10s",
	)
	cmd.Dir = n.repoRoot
	out, err := cmd.CombinedOutput()
	return string(out), err
}

func (n *hostOlricNode) expire(ctx context.Context, dmap, key string, ttl time.Duration) (string, error) {
	cmd := exec.CommandContext(ctx,
		n.clientBinary,
		"-addr", fmt.Sprintf("%s:%d", n.bindAddr, n.respPort),
		"-op", "expire",
		"-dmap", dmap,
		"-key", key,
		"-ttl", ttl.String(),
		"-timeout", "10s",
	)
	cmd.Dir = n.repoRoot
	out, err := cmd.CombinedOutput()
	return string(out), err
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
	n.nodePrefix.Flush()
	n.sidecarPrefix.Flush()
}

func bestEffortStopHostNode(n *hostOlricNode) {
	if n == nil {
		return
	}
	if n.cmd != nil && n.cmd.Process != nil {
		_ = n.cmd.Process.Signal(syscall.SIGTERM)
	}
	if n.sidecarCmd != nil && n.sidecarCmd.Process != nil {
		_ = n.sidecarCmd.Process.Signal(syscall.SIGTERM)
	}
	timer := time.NewTimer(3 * time.Second)
	defer timer.Stop()
	select {
	case <-n.errCh:
	case <-timer.C:
		if n.cmd != nil && n.cmd.Process != nil {
			_ = n.cmd.Process.Kill()
		}
	}
	timer.Reset(3 * time.Second)
	select {
	case <-n.sidecarErrCh:
	case <-timer.C:
		if n.sidecarCmd != nil && n.sidecarCmd.Process != nil {
			_ = n.sidecarCmd.Process.Kill()
		}
	}
	if n.nodePrefix != nil {
		n.nodePrefix.Flush()
	}
	if n.sidecarPrefix != nil {
		n.sidecarPrefix.Flush()
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

func waitForGetMiss(t *testing.T, ctx context.Context, reader *hostOlricNode, dmap, key string, logNodes ...*hostOlricNode) {
	t.Helper()

	deadline := time.Now().Add(10 * time.Second)
	var lastGot, lastOut string
	var lastErr error
	for time.Now().Before(deadline) {
		got, out, err := reader.get(ctx, dmap, key)
		lastGot, lastOut, lastErr = got, out, err
		if err != nil && strings.Contains(out, "redis: nil") {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatalf("context ended waiting for %s/%s miss through %s: %v\nlast got=%q err=%v out=%s\n%s",
				dmap, key, reader.nodeID, ctx.Err(), lastGot, lastErr, strings.TrimSpace(lastOut), hostNodeLogs(logNodes...))
		case <-time.After(200 * time.Millisecond):
		}
	}
	t.Fatalf("timed out waiting for %s/%s miss through %s\nlast got=%q err=%v out=%s\n%s",
		dmap, key, reader.nodeID, lastGot, lastErr, strings.TrimSpace(lastOut), hostNodeLogs(logNodes...))
}

type hostDataSample struct {
	Key     string
	Value   string
	TTL     time.Duration
	Expires bool
}

type hostDataScenarioConfig struct {
	name      string
	nodeCount int
}

type hostDataScenario struct {
	t          *testing.T
	ctx        context.Context
	mysql      *hostMySQL
	watchdog   string
	nodes      []*hostOlricNode
	memberPort int
}

func newHostDataScenario(t *testing.T, ctx context.Context, cfg hostDataScenarioConfig) *hostDataScenario {
	t.Helper()

	if cfg.nodeCount < 1 {
		t.Fatalf("host scenario %q needs at least one node", cfg.name)
	}

	mysql := provisionHostMySQL(t, "127.0.0.1")
	watchdogAddr, _, cleanupWatchdog := startHostWatchdog(t, ctx)
	t.Cleanup(cleanupWatchdog)

	memberlistPort := mustFreeTCPPort(t)
	nodes := make([]*hostOlricNode, 0, cfg.nodeCount)
	for i := 0; i < cfg.nodeCount; i++ {
		node := startHostOlricNodeWithConfig(t, ctx, hostNodeConfig{
			watchdogAddr:   watchdogAddr,
			nodeID:         fmt.Sprintf("%s-%c", cfg.name, 'a'+rune(i)),
			podIP:          fmt.Sprintf("127.0.0.%d", i+2),
			bindAddr:       fmt.Sprintf("127.0.0.%d", i+2),
			memberlistPort: memberlistPort,
			mysqlDSN:       mysql.hostDSN(),
		})
		nodes = append(nodes, node)
	}

	suite := &hostDataScenario{
		t:          t,
		ctx:        ctx,
		mysql:      mysql,
		watchdog:   watchdogAddr,
		nodes:      nodes,
		memberPort: memberlistPort,
	}
	suite.waitForTopology()

	// Scenario-level failure dump: global ps aux snapshot.
	bundle := e2eDiagFor(t)
	bundle.addDumper(func(ctx context.Context, root string) {
		if out, err := exec.CommandContext(ctx, "ps", "aux").CombinedOutput(); err == nil {
			_ = os.WriteFile(filepath.Join(root, "ps-aux.txt"), out, 0o644)
		}
	})

	return suite
}

func (s *hostDataScenario) close() {
	for i := len(s.nodes) - 1; i >= 0; i-- {
		bestEffortStopHostNode(s.nodes[i])
	}
}

func (s *hostDataScenario) waitForTopology() {
	s.t.Helper()
	for i, node := range s.nodes {
		for j, other := range s.nodes {
			if i == j {
				continue
			}
			waitForText(s.t, s.ctx, node.logs, `node_id:"`+other.nodeID+`"`)
		}
		if len(s.nodes) > 1 {
			waitForText(s.t, s.ctx, node.logs, "joined topology peers")
		}
	}
}

func (s *hostDataScenario) key(prefix string) string {
	return prefix + ":" + strconv.FormatInt(time.Now().UnixNano(), 10)
}

func (s *hostDataScenario) seed(samples []hostDataSample) {
	s.t.Helper()
	if len(s.nodes) == 0 {
		s.t.Fatal("host scenario has no nodes")
	}
	for i, sample := range samples {
		node := s.nodes[i%len(s.nodes)]
		if sample.TTL > 0 {
			if out, err := node.putWithTTL(s.ctx, "users", sample.Key, sample.Value, sample.TTL); err != nil {
				s.t.Fatalf("seed ttl sample %s via %s: %v (%s)\n%s", sample.Key, node.nodeID, err, strings.TrimSpace(out), hostNodeLogs(s.nodes...))
			}
			continue
		}
		if out, err := node.put(s.ctx, "users", sample.Key, sample.Value); err != nil {
			s.t.Fatalf("seed sample %s via %s: %v (%s)\n%s", sample.Key, node.nodeID, err, strings.TrimSpace(out), hostNodeLogs(s.nodes...))
		}
	}
}

func (s *hostDataScenario) expectMySQLValues(samples []hostDataSample) {
	s.t.Helper()
	for _, sample := range samples {
		waitForMySQLValue(s.t, s.ctx, s.mysql.hostDSN(), hostRESPDMap, sample.Key, sample.Value)
	}
}

func (s *hostDataScenario) expectClusterValues(samples []hostDataSample) {
	s.t.Helper()
	for _, sample := range samples {
		for _, node := range s.nodes {
			waitForGetValue(s.t, s.ctx, node, "users", sample.Key, sample.Value, s.nodes...)
		}
	}
}

func (s *hostDataScenario) expectExpiredMissing(samples []hostDataSample) {
	s.t.Helper()
	for _, sample := range samples {
		if !sample.Expires {
			continue
		}
		for _, node := range s.nodes {
			waitForGetMiss(s.t, s.ctx, node, "users", sample.Key, s.nodes...)
		}
	}
}

func (s *hostDataScenario) restartAsSingleReader(nodeID string) *hostOlricNode {
	s.t.Helper()
	for _, node := range s.nodes {
		bestEffortStopHostNode(node)
	}
	return startHostOlricNodeWithConfig(s.t, s.ctx, hostNodeConfig{
		watchdogAddr: s.watchdog,
		nodeID:       nodeID,
		podIP:        "127.0.0.5",
		bindAddr:     "127.0.0.5",
		mysqlDSN:     s.mysql.hostDSN(),
	})
}

func (s *hostDataScenario) expectReaderAfterRestart(reader *hostOlricNode, samples []hostDataSample) {
	s.t.Helper()
	defer bestEffortStopHostNode(reader)
	for _, sample := range samples {
		if sample.Expires {
			waitForGetMiss(s.t, s.ctx, reader, "users", sample.Key, reader)
			continue
		}
		waitForGetValue(s.t, s.ctx, reader, "users", sample.Key, sample.Value)
	}
}

func (s *hostDataScenario) startBackgroundTraffic(writers int, prefix string) (context.CancelFunc, <-chan backgroundTrafficResult, *sync.RWMutex, *[]*hostOlricNode) {
	s.t.Helper()
	active := append([]*hostOlricNode(nil), s.nodes...)
	var activeMu sync.RWMutex
	trafficCtx, cancel := context.WithCancel(s.ctx)
	done := startBackgroundHostTraffic(s.t, trafficCtx, &activeMu, &active, writers, prefix)
	return cancel, done, &activeMu, &active
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
				live := make([]*hostOlricNode, 0, len(nodes))
				for _, node := range nodes {
					if node != nil && node.cmd != nil && node.cmd.Process != nil && node.cmd.ProcessState == nil {
						live = append(live, node)
					}
				}
				if len(live) < 2 {
					failures.Add(1)
					time.Sleep(25 * time.Millisecond)
					continue
				}
				target := live[(worker+i)%len(live)]
				reader := live[(worker+i+1)%len(live)]
				if reader == target {
					reader = live[(worker+i+2)%len(live)]
				}
				key := fmt.Sprintf("%s:%d:%d:%d", prefix, time.Now().UnixNano(), worker, i)
				value := "value-" + key
				if out, err := target.put(ctx, "users", key, value); err != nil {
					t.Logf("storm put failed prefix=%s target=%s key=%s err=%v out=%s", prefix, target.nodeID, key, err, strings.TrimSpace(out))
					failures.Add(1)
					continue
				}
				if got, out, err := waitForNodeGetValue(ctx, reader, "users", key, value); err != nil {
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
				if got, _, err := waitForNodeGetValue(ctx, reader, "users", key, value); err != nil {
					_ = got
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

func waitForNodeGetValue(ctx context.Context, reader *hostOlricNode, dmap, key, want string) (got string, out string, err error) {
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		got, out, err = reader.get(ctx, dmap, key)
		if err == nil && got == want {
			return got, out, nil
		}
		select {
		case <-ctx.Done():
			return got, out, ctx.Err()
		case <-time.After(100 * time.Millisecond):
		}
	}
	return got, out, fmt.Errorf("expected %q", want)
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

	// Register failure-dump hook: topology JSON snapshot.
	{
		svc := service
		bundle := e2eDiagFor(t)
		bundle.addDumper(func(ctx context.Context, root string) {
			wdDiagDir := filepath.Join(root, "watchdog")
			_ = os.MkdirAll(wdDiagDir, 0o755)

			env, err := svc.GetTopology(ctx, &topologypb.TopologyQuery{StackId: "host-only"})
			if err != nil {
				_ = os.WriteFile(filepath.Join(wdDiagDir, "topology-error.txt"), []byte(err.Error()), 0o644)
				return
			}
			data, err := protojson.Marshal(env)
			if err != nil {
				_ = os.WriteFile(filepath.Join(wdDiagDir, "topology-marshal-error.txt"), []byte(err.Error()), 0o644)
				return
			}
			_ = os.WriteFile(filepath.Join(wdDiagDir, "topology.json"), data, 0o644)
		})
	}

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

func findProjectRoot() (string, error) {
	wd, err := os.Getwd()
	if err != nil {
		return "", err
	}
	dir := wd
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir, nil
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			return "", fmt.Errorf("go.mod not found in any parent directory")
		}
		dir = parent
	}
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

// prefixWriter wraps an io.Writer and prepends "[tag] " at the start of
// every line. It buffers partial lines. Safe for concurrent use.
type prefixWriter struct {
	mu    sync.Mutex
	tag   string
	out   *lockedBuffer
	carry []byte
}

func newPrefixWriter(tag string, out *lockedBuffer) *prefixWriter {
	return &prefixWriter{tag: tag, out: out}
}

func (p *prefixWriter) Write(b []byte) (int, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.carry = append(p.carry, b...)
	for {
		i := bytes.IndexByte(p.carry, '\n')
		if i < 0 {
			break
		}
		line := p.carry[:i+1]
		p.carry = p.carry[i+1:]
		p.out.Write([]byte("[" + p.tag + "] "))
		p.out.Write(line)
	}
	return len(b), nil
}

func (p *prefixWriter) Flush() {
	p.mu.Lock()
	defer p.mu.Unlock()
	if len(p.carry) > 0 {
		p.out.Write([]byte("[" + p.tag + "] "))
		p.out.Write(p.carry)
		p.out.Write([]byte("\n"))
		p.carry = nil
	}
}

// teeWriter fans out one Write call to multiple writers; errors on
// non-primary (first) writers are ignored so a disk-full doesn't break the test.
type teeWriter struct {
	primary io.Writer
	extras  []io.Writer
}

func newTeeWriter(primary io.Writer, extras ...io.Writer) *teeWriter {
	return &teeWriter{primary: primary, extras: extras}
}

func (tw *teeWriter) Write(b []byte) (int, error) {
	n, err := tw.primary.Write(b)
	for _, w := range tw.extras {
		_, _ = w.Write(b)
	}
	return n, err
}

// openDiagFile creates a log file under dir and registers t.Cleanup to close it.
func openDiagFile(t *testing.T, dir, name string) *os.File {
	t.Helper()
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatalf("openDiagFile mkdir: %v", err)
	}
	f, err := os.OpenFile(filepath.Join(dir, name), os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		t.Fatalf("openDiagFile open: %v", err)
	}
	t.Cleanup(func() { _ = f.Close() })
	return f
}

// diagDirForNode returns "<t.TempDir()>/diag/nodes/<nodeID>" and ensures it exists.
func diagDirForNode(t *testing.T, nodeID string) string {
	t.Helper()
	d := filepath.Join(t.TempDir(), "diag", "nodes", nodeID)
	_ = os.MkdirAll(d, 0o755)
	return d
}

// --- e2eDiag: failure diagnostics framework ---

// e2eDiagBundle collects diagnostic dumpers for one test. Each subsystem
// (node, watchdog, mysql) registers its dumper during setup. On t.Cleanup,
// if t.Failed(), all dumpers run and write into the test's diag root.
type e2eDiagBundle struct {
	mu      sync.Mutex
	dumpers []func(ctx context.Context, root string)
}

// diagBundles is keyed by t.Name() so that subsystem helpers can locate the
// per-test bundle without threading it through every function signature.
var diagBundles sync.Map // map[string]*e2eDiagBundle

// e2eDiagFor returns (or creates) the per-test diag bundle. The cleanup that
// fires the dumpers is registered exactly once per test, on first call.
func e2eDiagFor(t *testing.T) *e2eDiagBundle {
	t.Helper()
	if v, ok := diagBundles.Load(t.Name()); ok {
		return v.(*e2eDiagBundle)
	}
	b := &e2eDiagBundle{}
	actual, loaded := diagBundles.LoadOrStore(t.Name(), b)
	if loaded {
		return actual.(*e2eDiagBundle)
	}
	t.Cleanup(func() {
		diagBundles.Delete(t.Name())
		if !t.Failed() {
			return
		}
		root := filepath.Join(t.TempDir(), "diag")
		_ = os.MkdirAll(root, 0o755)
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		b.mu.Lock()
		dumpers := append([]func(ctx context.Context, root string){}, b.dumpers...)
		b.mu.Unlock()
		for _, d := range dumpers {
			func() {
				defer func() { _ = recover() }() // never let a dumper panic kill the cleanup
				d(ctx, root)
			}()
		}
		t.Logf("e2eDiag: failure diagnostics written to %s", root)
		if dir := os.Getenv("E2E_DIAG_DIR"); dir != "" {
			ts := time.Now().UTC().Format("20060102T150405")
			dst := filepath.Join(dir, sanitizeTestName(t.Name()), ts)
			if err := copyTree(root, dst); err != nil {
				t.Logf("e2eDiag: failed to mirror to E2E_DIAG_DIR: %v", err)
			} else {
				t.Logf("e2eDiag: mirrored to %s", dst)
			}
		}
	})
	return b
}

func (b *e2eDiagBundle) addDumper(fn func(ctx context.Context, root string)) {
	b.mu.Lock()
	b.dumpers = append(b.dumpers, fn)
	b.mu.Unlock()
}

func sanitizeTestName(name string) string {
	return strings.NewReplacer("/", "_", " ", "_", ":", "_").Replace(name)
}

func copyTree(src, dst string) error {
	return filepath.WalkDir(src, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return nil
		}
		rel, _ := filepath.Rel(src, path)
		target := filepath.Join(dst, rel)
		if d.IsDir() {
			return os.MkdirAll(target, 0o755)
		}
		in, err := os.Open(path)
		if err != nil {
			return nil
		}
		defer in.Close()
		if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
			return nil
		}
		out, err := os.Create(target)
		if err != nil {
			return nil
		}
		defer out.Close()
		_, _ = io.Copy(out, in)
		return nil
	})
}
