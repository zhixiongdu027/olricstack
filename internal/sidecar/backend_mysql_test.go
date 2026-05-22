package sidecar

import (
	"regexp"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/zhixiongdu/olricstack/internal/store"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
)

func newMockMySQLBackend(t *testing.T) (*MySQLBackend, sqlmock.Sqlmock, func()) {
	t.Helper()

	sqlDB, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}

	db, err := gorm.Open(mysql.New(mysql.Config{
		Conn:                      sqlDB,
		SkipInitializeWithVersion: true,
	}), &gorm.Config{})
	if err != nil {
		_ = sqlDB.Close()
		t.Fatalf("gorm open: %v", err)
	}

	return &MySQLBackend{db: db, batchSize: 1}, mock, func() {
		_ = sqlDB.Close()
	}
}

func TestMySQLBackendLoadFromMySQLDropsExpiredRecord(t *testing.T) {
	t.Parallel()
	backend, mock, cleanup := newMockMySQLBackend(t)
	defer cleanup()

	rows := sqlmock.NewRows([]string{
		"dmap", "key", "hkey", "encoded_entry", "ttl", "timestamp",
		"tombstone", "generation", "epoch", "owner_seq", "writer_id", "updated_at",
	}).AddRow("users", "alice", uint64(7), []byte("expired"), time.Now().Add(-time.Second).UnixMilli(), time.Now().UnixMilli(), false, 1, 1, 1, "node-A", time.Now())

	mock.ExpectQuery(regexp.QuoteMeta("SELECT * FROM `olric_cache_records` WHERE dmap = ? AND hkey = ? ORDER BY `olric_cache_records`.`dmap` LIMIT ?")).
		WithArgs("users", uint64(7), 1).
		WillReturnRows(rows)

	rec, err := backend.LoadFromMySQL(t.Context(), store.EntryRef{DMap: "users", HKey: 7})
	if err == nil || err != store.ErrNotFound {
		t.Fatalf("expected ErrNotFound for expired record, got rec=%#v err=%v", rec, err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("expectations: %v", err)
	}
}

func TestMySQLBackendLoadFromMySQLReturnsLiveRecord(t *testing.T) {
	t.Parallel()
	backend, mock, cleanup := newMockMySQLBackend(t)
	defer cleanup()

	now := time.Now()
	rows := sqlmock.NewRows([]string{
		"dmap", "key", "hkey", "encoded_entry", "ttl", "timestamp",
		"tombstone", "generation", "epoch", "owner_seq", "writer_id", "updated_at",
	}).AddRow("users", "alice", uint64(7), []byte("live"), now.Add(time.Minute).UnixMilli(), now.UnixMilli(), false, 1, 1, 1, "node-A", now)

	mock.ExpectQuery(regexp.QuoteMeta("SELECT * FROM `olric_cache_records` WHERE dmap = ? AND hkey = ? ORDER BY `olric_cache_records`.`dmap` LIMIT ?")).
		WithArgs("users", uint64(7), 1).
		WillReturnRows(rows)

	rec, err := backend.LoadFromMySQL(t.Context(), store.EntryRef{DMap: "users", HKey: 7})
	if err != nil {
		t.Fatalf("expected live record, got err=%v", err)
	}
	if rec.Key != "alice" || string(rec.EncodedEntry) != "live" {
		t.Fatalf("unexpected record: %#v", rec)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("expectations: %v", err)
	}
}
