// Package oplog defines the wire format for dirty-data entries crossing the
// shm ring from olric-node into olric-sidecar.
//
// An OplogEntry is the *complete*, self-contained description of one mutation
// the sidecar must persist. The sidecar never reads back into node memory —
// the ring is one-way — so every field MySQL needs to upsert lives here.
//
// Encoding is JSON. The wire format is intentionally separate from
// store.EntryRecord (which carries gorm tags and is shaped for the MySQL
// table) so the IPC contract can evolve independently of the persistence
// schema. recordFromEntry in internal/sidecar bridges the two.
package oplog

import (
	"encoding/json"
	"fmt"
)

// Op identifies the kind of mutation carried in an OplogEntry.
type Op string

const (
	OpSet    Op = "set"
	OpDelete Op = "delete"
	OpExpire Op = "expire"
)

// Entry is the wire envelope for one dirty mutation.
type Entry struct {
	Version uint32 `json:"v"`
	Op      Op     `json:"op"`

	DMap string `json:"dmap"`
	Key  string `json:"key"`
	HKey uint64 `json:"hkey"`

	EncodedEntry []byte `json:"entry,omitempty"`
	TTL          int64  `json:"ttl,omitempty"`
	Timestamp    int64  `json:"ts,omitempty"`

	Generation int64  `json:"g"`
	Epoch      int64  `json:"e"`
	OwnerSeq   int64  `json:"s"`
	WriterID   string `json:"w"`

	UpdatedAtUnixNano int64 `json:"u"`
}

const currentVersion uint32 = 1

// Encode serializes entry into a JSON payload suitable for ring.Append. The
// version field is stamped automatically.
func Encode(entry Entry) ([]byte, error) {
	entry.Version = currentVersion
	return json.Marshal(entry)
}

// Decode parses a payload produced by Encode. Unknown versions are rejected
// so the consumer never silently misinterprets a newer wire format.
func Decode(payload []byte) (Entry, error) {
	var e Entry
	if err := json.Unmarshal(payload, &e); err != nil {
		return Entry{}, fmt.Errorf("oplog decode: %w", err)
	}
	if e.Version != currentVersion {
		return Entry{}, fmt.Errorf("oplog decode: unsupported version %d", e.Version)
	}
	switch e.Op {
	case OpSet, OpDelete, OpExpire:
	default:
		return Entry{}, fmt.Errorf("oplog decode: unknown op %q", e.Op)
	}
	if e.DMap == "" {
		return Entry{}, fmt.Errorf("oplog decode: empty dmap")
	}
	if e.Generation <= 0 || e.OwnerSeq <= 0 {
		return Entry{}, fmt.Errorf("oplog decode: missing fence (g=%d s=%d)", e.Generation, e.OwnerSeq)
	}
	if e.WriterID == "" {
		return Entry{}, fmt.Errorf("oplog decode: empty writer id")
	}
	return e, nil
}
