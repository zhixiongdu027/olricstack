package node

import (
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	bolt "go.etcd.io/bbolt"
)

var (
	ErrOwnerSequenceClosed   = errors.New("owner sequence is closed")
	ErrOwnerSequenceRollback = errors.New("owner sequence rollback rejected")
)

var (
	ownerSeqBucket    = []byte("owner_seq")
	ownerSeqStateKey  = []byte("state")
)

type OwnerSequence struct {
	mu sync.Mutex
	db *bolt.DB

	closed     bool
	generation int64
	epoch      int64
	seq        int64
}

func NewOwnerSequence(path string) (*OwnerSequence, error) {
	if path == "" {
		return nil, errors.New("owner sequence path is required")
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, fmt.Errorf("create owner sequence dir: %w", err)
	}
	db, err := bolt.Open(path, 0o600, &bolt.Options{Timeout: time.Second})
	if err != nil {
		return nil, fmt.Errorf("open owner sequence: %w", err)
	}
	o := &OwnerSequence{db: db}
	if err := db.Update(func(tx *bolt.Tx) error {
		bucket, err := tx.CreateBucketIfNotExists(ownerSeqBucket)
		if err != nil {
			return err
		}
		if encoded := bucket.Get(ownerSeqStateKey); encoded != nil {
			if err := decodeOwnerSeqState(encoded, &o.generation, &o.epoch, &o.seq); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		_ = db.Close()
		return nil, err
	}
	return o, nil
}

// Stamp advances the owner sequence under fence (generation, epoch).
//
// Semantics:
//   - If (generation, epoch) > persisted (lastG, lastE): seq restarts at 1.
//   - If (generation, epoch) == persisted (lastG, lastE): seq = lastS + 1.
//   - If (generation, epoch) <  persisted (lastG, lastE): returns
//     ErrOwnerSequenceRollback. Callers must refresh their lease before retrying.
//
// The new (g, e, s) tuple is persisted before return so that a crash after
// Stamp but before the durable hook PrepareEntry cannot resurrect the same s.
func (o *OwnerSequence) Stamp(generation, epoch int64) (int64, int64, int64, error) {
	if generation <= 0 || epoch < 0 {
		return 0, 0, 0, fmt.Errorf("invalid fence generation=%d epoch=%d", generation, epoch)
	}
	o.mu.Lock()
	defer o.mu.Unlock()

	if o.closed {
		return 0, 0, 0, ErrOwnerSequenceClosed
	}

	switch {
	case generation > o.generation || (generation == o.generation && epoch > o.epoch):
		o.generation = generation
		o.epoch = epoch
		o.seq = 1
	case generation == o.generation && epoch == o.epoch:
		o.seq++
	default:
		return 0, 0, 0, fmt.Errorf("%w: have (%d,%d) requested (%d,%d)",
			ErrOwnerSequenceRollback, o.generation, o.epoch, generation, epoch)
	}

	if err := o.persistLocked(); err != nil {
		return 0, 0, 0, err
	}
	return o.generation, o.epoch, o.seq, nil
}

// Snapshot returns the last persisted (generation, epoch, seq) without advancing.
func (o *OwnerSequence) Snapshot() (int64, int64, int64) {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.generation, o.epoch, o.seq
}

func (o *OwnerSequence) Close() error {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.closed {
		return nil
	}
	o.closed = true
	return o.db.Close()
}

func (o *OwnerSequence) persistLocked() error {
	encoded := encodeOwnerSeqState(o.generation, o.epoch, o.seq)
	return o.db.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(ownerSeqBucket)
		if bucket == nil {
			return errors.New("owner sequence bucket missing")
		}
		return bucket.Put(ownerSeqStateKey, encoded)
	})
}

func encodeOwnerSeqState(generation, epoch, seq int64) []byte {
	buf := make([]byte, 24)
	binary.BigEndian.PutUint64(buf[0:8], uint64(generation))
	binary.BigEndian.PutUint64(buf[8:16], uint64(epoch))
	binary.BigEndian.PutUint64(buf[16:24], uint64(seq))
	return buf
}

func decodeOwnerSeqState(encoded []byte, generation, epoch, seq *int64) error {
	if len(encoded) != 24 {
		return fmt.Errorf("owner sequence state corrupted: len=%d", len(encoded))
	}
	*generation = int64(binary.BigEndian.Uint64(encoded[0:8]))
	*epoch = int64(binary.BigEndian.Uint64(encoded[8:16]))
	*seq = int64(binary.BigEndian.Uint64(encoded[16:24]))
	return nil
}
