// Package store defines the wire types crossing between olric-node,
// olric-sidecar, and MySQL.
//
// Prior to the shm-oplog cutover, this package also implemented a node-local
// Pebble WAL + asynchronous MySQL flusher (MySQLStore). All of that moved
// into olric-sidecar; only the data records remain here so both sides agree
// on the on-wire and on-disk shape of a row.
package store

import (
	"errors"
	"time"
)

// ErrNotFound is returned by sidecar/backend lookups when a record is absent
// or tombstoned.
var ErrNotFound = errors.New("cache value not found")

// EntryRef identifies a row by its logical coordinates. The hkey is the
// Olric-computed hash of (dmap, key) and is what every lookup is actually
// keyed by; dmap+key are carried for diagnostics and MySQL projection.
type EntryRef struct {
	DMap string
	Key  string
	HKey uint64
}

// EntryRecord is the single shared row shape:
//
//   - on the shm ring it's the post-decode form (oplog.Entry → EntryRecord)
//     used by the sidecar consumer before upsert
//   - in MySQL it's the table row, with the gorm tags driving AutoMigrate
//     and the OnConflict comparator (see internal/sidecar/backend_mysql.go)
//   - on a LoadFromMySQL miss-path response it's the value the node decodes
//     back into an Olric storage.Entry
//
// WriterID is the per-pod-instance writer identifier — a fresh UUID at each
// node boot. The fence comparator uses strict lex `(generation, epoch,
// owner_seq, writer_id)`; writer_id is the tiebreaker that keeps the
// comparator total even if two writers somehow stamp the same triple.
type EntryRecord struct {
	DMap         string    `gorm:"primaryKey;size:256;column:dmap" json:"dmap"`
	Key          string    `gorm:"column:key;size:512;not null;index:idx_olric_entry_lookup" json:"key"`
	HKey         uint64    `gorm:"primaryKey;column:hkey" json:"hkey"`
	EncodedEntry []byte    `gorm:"column:encoded_entry;type:longblob" json:"encoded_entry"`
	TTL          int64     `gorm:"column:ttl;not null" json:"ttl"`
	Timestamp    int64     `gorm:"column:timestamp;not null" json:"timestamp"`
	Tombstone    bool      `gorm:"column:tombstone;not null" json:"tombstone"`
	Generation   int64     `gorm:"column:generation;not null;index:idx_cache_fence" json:"generation"`
	Epoch        int64     `gorm:"column:epoch;not null;index:idx_cache_fence" json:"epoch"`
	OwnerSeq     int64     `gorm:"column:owner_seq;not null;index:idx_cache_fence" json:"owner_seq"`
	WriterID     string    `gorm:"column:writer_id;size:128;not null;default:''" json:"writer_id"`
	UpdatedAt    time.Time `gorm:"column:updated_at" json:"updated_at"`
}

func (EntryRecord) TableName() string { return "olric_cache_records" }

func (r EntryRecord) Ref() EntryRef {
	return EntryRef{DMap: r.DMap, Key: r.Key, HKey: r.HKey}
}

func (r EntryRecord) Clone() EntryRecord {
	r.EncodedEntry = append([]byte(nil), r.EncodedEntry...)
	return r
}
