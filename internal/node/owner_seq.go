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
	ownerSeqBucket   = []byte("owner_seq")
	ownerSeqStateKey = []byte("state")
)

// DefaultOwnerSequenceReservation is how many seq numbers a single fsync
// reserves under one (generation, epoch). Larger values amortize fsync cost
// across more Stamp() calls; smaller values shrink the post-crash gap. 256
// is empirically a good balance: under default flush settings it reduces the
// per-write fsync count from 1-per-write to 1-per-256-writes while leaving
// at most 256 unused seq numbers per crash, which is invisible to MySQL's
// strict-lex upsert comparator.
const DefaultOwnerSequenceReservation = 256

type OwnerSequence struct {
	mu sync.Mutex
	db *bolt.DB

	closed         bool
	reservationLen int64

	// generation, epoch identify the current fence window.
	generation int64
	epoch      int64

	// seq is the highest seq value already returned to a caller for the
	// current (generation, epoch). It is the in-memory truth.
	seq int64

	// reservedHigh is the highest seq value persisted to bbolt for the
	// current (generation, epoch). seq <= reservedHigh holds at all times
	// after a successful Stamp; this is the safety contract that survives
	// a crash.
	reservedHigh int64
}

func NewOwnerSequence(path string) (*OwnerSequence, error) {
	return NewOwnerSequenceWithReservation(path, DefaultOwnerSequenceReservation)
}

// NewOwnerSequenceWithReservation lets tests pin the reservation window. A
// reservation of 1 yields the pre-batching one-fsync-per-stamp behaviour.
func NewOwnerSequenceWithReservation(path string, reservation int64) (*OwnerSequence, error) {
	if path == "" {
		return nil, errors.New("owner sequence path is required")
	}
	if reservation <= 0 {
		reservation = DefaultOwnerSequenceReservation
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, fmt.Errorf("create owner sequence dir: %w", err)
	}
	db, err := bolt.Open(path, 0o600, &bolt.Options{Timeout: time.Second})
	if err != nil {
		return nil, fmt.Errorf("open owner sequence: %w", err)
	}
	o := &OwnerSequence{db: db, reservationLen: reservation}
	if err := db.Update(func(tx *bolt.Tx) error {
		bucket, err := tx.CreateBucketIfNotExists(ownerSeqBucket)
		if err != nil {
			return err
		}
		if encoded := bucket.Get(ownerSeqStateKey); encoded != nil {
			if err := decodeOwnerSeqState(encoded, &o.generation, &o.epoch, &o.reservedHigh); err != nil {
				return err
			}
			// On restart we cannot replay which seq values were actually
			// returned to callers vs. merely reserved. Jump seq to the top
			// of the persisted reservation: every value in
			// (previous_seq, reservedHigh] is treated as already-consumed.
			// MySQL's strict-lex upsert is gap-tolerant, so the resulting
			// holes have no correctness impact.
			o.seq = o.reservedHigh
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
// Persistence model (reservation-based):
//
//	The bbolt record stores (generation, epoch, reservedHigh). reservedHigh
//	is the largest seq we have *promised* not to reuse. Stamp() returns
//	values one at a time, and only fsyncs when seq would exceed reservedHigh
//	— at which point reservedHigh is bumped by reservationLen in a single
//	tx. Crash-safety follows from the invariant
//
//	    ∀ returned-by-Stamp triple (G, E, S):  S ≤ reservedHigh
//	    AND  reservedHigh is durable on disk before Stamp returned S
//
//	After a crash, the next Stamp picks up at reservedHigh + 1, leaving a
//	hole of at most reservationLen unused seq values per (G, E). The hole
//	does not violate any invariant because MySQL's upsert clause uses
//	strict lex `>` over (G, E, S) and never assumes S is contiguous.
//
// Semantics for fence transitions (unchanged from the pre-reservation API):
//
//   - (G, E) > persisted (lastG, lastE):  reset window, seq starts at 1.
//   - (G, E) = persisted (lastG, lastE):  seq = lastSeq + 1.
//   - (G, E) < persisted (lastG, lastE):  ErrOwnerSequenceRollback.
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
		// New fence window — close out the old reservation by overwriting
		// state, then allocate the first slot of the new window.
		o.generation = generation
		o.epoch = epoch
		o.seq = 1
		o.reservedHigh = o.reservationLen
		if err := o.persistLocked(); err != nil {
			return 0, 0, 0, err
		}
	case generation == o.generation && epoch == o.epoch:
		o.seq++
		if o.seq > o.reservedHigh {
			// Need to extend the reservation. fsync once, cover seq plus
			// reservationLen-1 more values in advance.
			o.reservedHigh = o.seq + o.reservationLen - 1
			if err := o.persistLocked(); err != nil {
				return 0, 0, 0, err
			}
		}
	default:
		return 0, 0, 0, fmt.Errorf("%w: have (%d,%d) requested (%d,%d)",
			ErrOwnerSequenceRollback, o.generation, o.epoch, generation, epoch)
	}

	return o.generation, o.epoch, o.seq, nil
}

// Snapshot returns the last seq value already handed out (NOT the reservation
// ceiling). Callers should treat this as advisory: a concurrent Stamp may
// already be consuming the next value.
func (o *OwnerSequence) Snapshot() (int64, int64, int64) {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.generation, o.epoch, o.seq
}

// reservedHighSnapshot exposes the persisted reservation ceiling for tests.
// Production code must not depend on this: the ceiling may move at any time.
func (o *OwnerSequence) reservedHighSnapshot() int64 {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.reservedHigh
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

// persistLocked writes (generation, epoch, reservedHigh) to bbolt. seq itself
// is NOT persisted: only the reservation ceiling is durable.
func (o *OwnerSequence) persistLocked() error {
	encoded := encodeOwnerSeqState(o.generation, o.epoch, o.reservedHigh)
	return o.db.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(ownerSeqBucket)
		if bucket == nil {
			return errors.New("owner sequence bucket missing")
		}
		return bucket.Put(ownerSeqStateKey, encoded)
	})
}

func encodeOwnerSeqState(generation, epoch, reservedHigh int64) []byte {
	buf := make([]byte, 24)
	binary.BigEndian.PutUint64(buf[0:8], uint64(generation))
	binary.BigEndian.PutUint64(buf[8:16], uint64(epoch))
	binary.BigEndian.PutUint64(buf[16:24], uint64(reservedHigh))
	return buf
}

func decodeOwnerSeqState(encoded []byte, generation, epoch, reservedHigh *int64) error {
	if len(encoded) != 24 {
		return fmt.Errorf("owner sequence state corrupted: len=%d", len(encoded))
	}
	*generation = int64(binary.BigEndian.Uint64(encoded[0:8]))
	*epoch = int64(binary.BigEndian.Uint64(encoded[8:16]))
	*reservedHigh = int64(binary.BigEndian.Uint64(encoded[16:24]))
	return nil
}
