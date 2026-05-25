//go:build e2e

package e2e

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/zhixiongdu/olricstack/internal/olricstore"
)

type hostMySQL struct {
	clusterHost string
	hostPort    string
	database    string
	user        string
	password    string
}

func (m *hostMySQL) clusterDSN() string {
	return fmt.Sprintf("%s:%s@tcp(%s:%s)/%s?parseTime=true", m.user, m.password, m.clusterHost, m.hostPort, m.database)
}

func (m *hostMySQL) hostDSN() string {
	return fmt.Sprintf("%s:%s@tcp(127.0.0.1:%s)/%s?parseTime=true", m.user, m.password, m.hostPort, m.database)
}

func provisionHostMySQL(t *testing.T, clusterHost string) *hostMySQL {
	t.Helper()

	base := &hostMySQL{
		clusterHost: clusterHost,
		hostPort:    envOrDefault("E2E_MYSQL_PORT", "13306"),
		database:    "mysql",
		user:        envOrDefault("E2E_MYSQL_USER", "root"),
		password:    envOrDefault("E2E_MYSQL_PASSWORD", "password"),
	}
	waitForMySQLReady(t, context.Background(), base.hostDSN())

	db, err := sql.Open("mysql", base.hostDSN())
	if err != nil {
		t.Fatalf("open host mysql connection: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	database := "olric_e2e_" + strconv.FormatInt(time.Now().UnixNano(), 10)
	if _, err := db.ExecContext(ctx, "CREATE DATABASE IF NOT EXISTS `"+database+"`"); err != nil {
		t.Fatalf("create e2e mysql database %s: %v", database, err)
	}
	t.Cleanup(func() {
		if testing.Testing() && envOrDefault("E2E_KEEP_MYSQL_DATABASES", "") == "1" {
			_ = db.Close()
			return
		}
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cleanupCancel()
		_, _ = db.ExecContext(cleanupCtx, "DROP DATABASE IF EXISTS `"+database+"`")
		_ = db.Close()
	})

	result := &hostMySQL{
		clusterHost: base.clusterHost,
		hostPort:    base.hostPort,
		database:    database,
		user:        base.user,
		password:    base.password,
	}

	// Register failure-dump hook: snapshot the olric_cache_records table as CSV.
	// Registered AFTER the drop-DB cleanup above so that LIFO ordering runs the
	// diag dump first (while the database still exists).
	{
		dsn := result.hostDSN()
		bundle := e2eDiagFor(t)
		bundle.addDumper(func(ctx context.Context, root string) {
			mysqlDiagDir := filepath.Join(root, "mysql")
			_ = os.MkdirAll(mysqlDiagDir, 0o755)

			dumpDB, err := sql.Open("mysql", dsn)
			if err != nil {
				_ = os.WriteFile(filepath.Join(mysqlDiagDir, "open-error.txt"), []byte(err.Error()), 0o644)
				return
			}
			defer dumpDB.Close()

			rows, err := dumpDB.QueryContext(ctx,
				"SELECT dmap, `key`, hkey, ttl, timestamp, tombstone, generation, epoch, owner_seq, writer_id, updated_at FROM olric_cache_records ORDER BY dmap, hkey")
			if err != nil {
				_ = os.WriteFile(filepath.Join(mysqlDiagDir, "olric_cache_records-error.txt"), []byte(err.Error()), 0o644)
				return
			}
			defer rows.Close()

			var buf bytes.Buffer
			cols, _ := rows.Columns()
			buf.WriteString(strings.Join(cols, ",") + "\n")

			vals := make([]any, len(cols))
			ptrs := make([]any, len(cols))
			for i := range ptrs {
				ptrs[i] = &vals[i]
			}
			for rows.Next() {
				if err := rows.Scan(ptrs...); err != nil {
					break
				}
				for i, v := range vals {
					if i > 0 {
						buf.WriteByte(',')
					}
					switch tv := v.(type) {
					case []byte:
						buf.WriteString(string(tv))
					default:
						fmt.Fprintf(&buf, "%v", tv)
					}
				}
				buf.WriteByte('\n')
			}
			_ = os.WriteFile(filepath.Join(mysqlDiagDir, "olric_cache_records.csv"), buf.Bytes(), 0o644)
		})
	}

	return result
}

type mysqlRecord struct {
	WriterID     string
	OwnerSeq     int64
	Tombstone    bool
	EncodedEntry []byte
}

func waitForMySQLRecord(t *testing.T, ctx context.Context, dsn, dmap, key string) mysqlRecord {
	t.Helper()

	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatalf("open mysql connection: %v", err)
	}
	defer db.Close()

	deadline := time.Now().Add(45 * time.Second)
	var lastErr error
	for time.Now().Before(deadline) {
		pingCtx, pingCancel := context.WithTimeout(ctx, 3*time.Second)
		err := db.PingContext(pingCtx)
		pingCancel()
		if err != nil {
			lastErr = err
			time.Sleep(time.Second)
			continue
		}

		queryCtx, queryCancel := context.WithTimeout(ctx, 3*time.Second)
		var record mysqlRecord
		err = db.QueryRowContext(queryCtx,
			"SELECT writer_id, owner_seq, tombstone, encoded_entry FROM olric_cache_records WHERE dmap = ? AND `key` = ?",
			dmap,
			key,
		).Scan(&record.WriterID, &record.OwnerSeq, &record.Tombstone, &record.EncodedEntry)
		queryCancel()
		if err == nil {
			return record
		}
		lastErr = err
		time.Sleep(time.Second)
	}

	t.Fatalf("mysql durable record %s/%s not observed via %q: %v", dmap, key, dsn, lastErr)
	return mysqlRecord{}
}

func waitForMySQLValue(t *testing.T, ctx context.Context, dsn, dmap, key, want string) mysqlRecord {
	t.Helper()

	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatalf("open mysql connection: %v", err)
	}
	defer db.Close()

	deadline := time.Now().Add(45 * time.Second)
	var lastErr error
	for time.Now().Before(deadline) {
		pingCtx, pingCancel := context.WithTimeout(ctx, 3*time.Second)
		err := db.PingContext(pingCtx)
		pingCancel()
		if err != nil {
			lastErr = err
			time.Sleep(time.Second)
			continue
		}

		queryCtx, queryCancel := context.WithTimeout(ctx, 3*time.Second)
		var record mysqlRecord
		err = db.QueryRowContext(queryCtx,
			"SELECT writer_id, owner_seq, tombstone, encoded_entry FROM olric_cache_records WHERE dmap = ? AND `key` = ?",
			dmap,
			key,
		).Scan(&record.WriterID, &record.OwnerSeq, &record.Tombstone, &record.EncodedEntry)
		queryCancel()
		if err == nil {
			entry := &olricstore.Entry{}
			entry.Decode(record.EncodedEntry)
			if string(entry.Value()) == want {
				return record
			}
			lastErr = fmt.Errorf("unexpected mysql encoded entry value %q", string(entry.Value()))
		} else {
			lastErr = err
		}
		time.Sleep(time.Second)
	}

	t.Fatalf("mysql durable record %s/%s with value %q not observed via %q: %v", dmap, key, want, dsn, lastErr)
	return mysqlRecord{}
}

func waitForMySQLRecordAfterSeq(t *testing.T, ctx context.Context, dsn, dmap, key string, minOwnerSeq int64) mysqlRecord {
	t.Helper()

	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatalf("open mysql connection: %v", err)
	}
	defer db.Close()

	deadline := time.Now().Add(45 * time.Second)
	var lastErr error
	for time.Now().Before(deadline) {
		pingCtx, pingCancel := context.WithTimeout(ctx, 3*time.Second)
		err := db.PingContext(pingCtx)
		pingCancel()
		if err != nil {
			lastErr = err
			time.Sleep(time.Second)
			continue
		}

		queryCtx, queryCancel := context.WithTimeout(ctx, 3*time.Second)
		var record mysqlRecord
		err = db.QueryRowContext(queryCtx,
			"SELECT writer_id, owner_seq, tombstone, encoded_entry FROM olric_cache_records WHERE dmap = ? AND `key` = ? AND owner_seq > ? ORDER BY owner_seq DESC LIMIT 1",
			dmap,
			key,
			minOwnerSeq,
		).Scan(&record.WriterID, &record.OwnerSeq, &record.Tombstone, &record.EncodedEntry)
		queryCancel()
		if err == nil {
			return record
		}
		lastErr = err
		time.Sleep(time.Second)
	}

	t.Fatalf("mysql durable record %s/%s with owner_seq > %d not observed via %q: %v", dmap, key, minOwnerSeq, dsn, lastErr)
	return mysqlRecord{}
}

func waitForMySQLReady(t *testing.T, ctx context.Context, dsn string) {
	t.Helper()

	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatalf("open mysql connection: %v", err)
	}
	defer db.Close()

	deadline := time.Now().Add(90 * time.Second)
	var lastErr error
	for time.Now().Before(deadline) {
		pingCtx, pingCancel := context.WithTimeout(ctx, 3*time.Second)
		err := db.PingContext(pingCtx)
		pingCancel()
		if err == nil {
			return
		}
		lastErr = err
		time.Sleep(time.Second)
	}

	t.Fatalf("mysql did not become ready for %q: %v", dsn, lastErr)
}

func mustFreeTCPPort(t *testing.T) int {
	t.Helper()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("allocate free tcp port: %v", err)
	}
	defer listener.Close()

	return listener.Addr().(*net.TCPAddr).Port
}
