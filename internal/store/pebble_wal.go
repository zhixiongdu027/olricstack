package store

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"time"

	"github.com/cockroachdb/pebble"
)

var (
	walDirtyPrefix      = []byte("dirty/")
	walDirtyUpperBound  = []byte("dirty0")
	walMetaSeqKey       = []byte("meta/wal_seq")
	walSyncWriteOptions = pebble.Sync
	errStopScan         = errors.New("stop wal scan")
)

type pebbleWAL struct {
	db        *pebble.DB
	queueSize int
	mu        sync.Mutex
	depth     int
	closed    bool
}

func openPebbleWAL(path string, queueSize int) (*pebbleWAL, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, fmt.Errorf("create wal dir: %w", err)
	}
	db, err := pebble.Open(path, &pebble.Options{})
	if err != nil {
		return nil, fmt.Errorf("open wal: %w", err)
	}
	w := &pebbleWAL{db: db, queueSize: queueSize}
	depth, err := w.countDirty()
	if err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("init wal: %w", err)
	}
	w.depth = depth
	return w, nil
}

func (w *pebbleWAL) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return nil
	}
	w.closed = true
	return w.db.Close()
}

func (w *pebbleWAL) Prepare(record EntryRecord) (dirtyEntry, int, error) {
	now := time.Now().UTC()
	record = record.Clone()
	if record.UpdatedAt.IsZero() {
		record.UpdatedAt = now
	}

	w.mu.Lock()
	defer w.mu.Unlock()
	if err := w.checkOpenLocked(); err != nil {
		return dirtyEntry{}, 0, err
	}

	entry := dirtyEntry{Record: record}
	batch := w.db.NewIndexedBatch()
	defer batch.Close()

	key := dirtyKey(record.Ref())
	current, ok, err := w.loadFrom(batch, key)
	if err != nil {
		return dirtyEntry{}, 0, err
	}
	if ok {
		if shouldPreserveDirtyRecord(current, record) {
			return dirtyEntry{Record: current.Clone()}, w.depth, nil
		}
	} else if w.depth >= w.queueSize {
		return dirtyEntry{}, 0, ErrQueueFull
	}

	seq, err := w.nextSeq(batch)
	if err != nil {
		return dirtyEntry{}, 0, err
	}
	entry.Record.WALSeq = seq
	if err := putRecord(batch, key, entry.Record); err != nil {
		return dirtyEntry{}, 0, err
	}
	if err := batch.Commit(walSyncWriteOptions); err != nil {
		return dirtyEntry{}, 0, err
	}
	if !ok {
		w.depth++
	}
	return dirtyEntry{Record: entry.Record.Clone()}, w.depth, nil
}

func (w *pebbleWAL) Commit(ref EntryRef, walSeq int64) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if err := w.checkOpenLocked(); err != nil {
		return 0, err
	}

	batch := w.db.NewIndexedBatch()
	defer batch.Close()

	key := dirtyKey(ref)
	record, ok, err := w.loadFrom(batch, key)
	if err != nil {
		return 0, err
	}
	if !ok || record.WALSeq != walSeq {
		return 0, ErrNotFound
	}
	record.WALState = WALStateCommitted
	record.FlushMySQL = true
	if err := putRecord(batch, key, record); err != nil {
		return 0, err
	}
	if err := batch.Commit(walSyncWriteOptions); err != nil {
		return 0, err
	}
	return w.depth, nil
}

func (w *pebbleWAL) Abort(ref EntryRef, walSeq int64) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if err := w.checkOpenLocked(); err != nil {
		return err
	}

	batch := w.db.NewIndexedBatch()
	defer batch.Close()

	key := dirtyKey(ref)
	record, ok, err := w.loadFrom(batch, key)
	if err != nil {
		return err
	}
	if !ok || record.WALSeq != walSeq {
		return nil
	}
	if record.WALState == WALStateCommitted {
		return errors.New("cannot abort committed wal record")
	}
	if err := batch.Delete(key, nil); err != nil {
		return err
	}
	if err := batch.Commit(walSyncWriteOptions); err != nil {
		return err
	}
	if w.depth > 0 {
		w.depth--
	}
	return nil
}

func (w *pebbleWAL) Load(ref EntryRef) (EntryRecord, bool, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if err := w.checkOpenLocked(); err != nil {
		return EntryRecord{}, false, err
	}
	record, ok, err := w.load(dirtyKey(ref))
	if err != nil {
		return EntryRecord{}, false, err
	}
	if !ok {
		return EntryRecord{}, false, nil
	}
	return record.Clone(), true, nil
}

func (w *pebbleWAL) Scan(limit int) ([]dirtyEntry, error) {
	return w.scanDirty(limit, func(EntryRecord) bool { return true })
}

func (w *pebbleWAL) ScanFlushable(limit int) ([]dirtyEntry, error) {
	return w.scanDirty(limit, isFlushable)
}

func (w *pebbleWAL) ScanFlushableRefs(refs []EntryRef) ([]dirtyEntry, error) {
	entries := make([]dirtyEntry, 0, len(refs))
	for _, ref := range refs {
		record, ok, err := w.Load(ref)
		if err != nil {
			return nil, err
		}
		if ok && isFlushable(record) {
			entries = append(entries, dirtyEntry{Record: record})
		}
	}
	return entries, nil
}

func (w *pebbleWAL) ScanFlushablePartition(dmap string, partitionID, partitionCount uint64) ([]dirtyEntry, error) {
	return w.scanDirty(0, func(record EntryRecord) bool {
		return record.DMap == dmap && record.HKey%partitionCount == partitionID && isFlushable(record)
	})
}

func (w *pebbleWAL) DeleteIfSeq(entries []dirtyEntry) error {
	if len(entries) == 0 {
		return nil
	}

	w.mu.Lock()
	defer w.mu.Unlock()
	if err := w.checkOpenLocked(); err != nil {
		return err
	}

	batch := w.db.NewIndexedBatch()
	defer batch.Close()

	var deleted int
	for _, flushed := range entries {
		key := dirtyKey(flushed.Record.Ref())
		current, ok, err := w.loadFrom(batch, key)
		if err != nil {
			return err
		}
		if ok && current.WALSeq == flushed.Record.WALSeq {
			if err := batch.Delete(key, nil); err != nil {
				return err
			}
			deleted++
		}
	}
	if err := batch.Commit(walSyncWriteOptions); err != nil {
		return err
	}
	w.depth -= deleted
	if w.depth < 0 {
		w.depth = 0
	}
	return nil
}

func (w *pebbleWAL) SweepPrepared() (int, error) {
	return w.deleteMatching(func(record EntryRecord) bool {
		return record.WALState == WALStatePrepared
	})
}

func (w *pebbleWAL) PreparedCount() (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if err := w.checkOpenLocked(); err != nil {
		return 0, err
	}
	var count int
	err := w.forEachDirty(func(_ []byte, record EntryRecord) error {
		if record.WALState == WALStatePrepared {
			count++
		}
		return nil
	})
	return count, err
}

func (w *pebbleWAL) PurgeBelowGeneration(minGeneration int64) (int, error) {
	return w.deleteMatching(func(record EntryRecord) bool {
		if record.WALState == WALStatePrepared {
			return false
		}
		return record.Generation > 0 && record.Generation < minGeneration
	})
}

func (w *pebbleWAL) Replay(ctx context.Context, f func(EntryRecord) error) ([]EntryRef, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if err := w.checkOpenLocked(); err != nil {
		return nil, err
	}
	expired := make([]EntryRef, 0)
	err := w.forEachDirty(func(_ []byte, record EntryRecord) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		record = record.Clone()
		if record.WALState == WALStatePrepared {
			return nil
		}
		if record.Tombstone {
			return nil
		}
		if isExpired(record.TTL, time.Now()) {
			expired = append(expired, record.Ref())
			return nil
		}
		return f(record)
	})
	if err != nil {
		return nil, err
	}
	return expired, nil
}

func (w *pebbleWAL) scanDirty(limit int, keep func(EntryRecord) bool) ([]dirtyEntry, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if err := w.checkOpenLocked(); err != nil {
		return nil, err
	}
	entries := make([]dirtyEntry, 0, limit)
	err := w.forEachDirty(func(_ []byte, record EntryRecord) error {
		if limit > 0 && len(entries) >= limit {
			return errStopScan
		}
		if keep(record) {
			entries = append(entries, dirtyEntry{Record: record.Clone()})
		}
		return nil
	})
	if errors.Is(err, errStopScan) {
		err = nil
	}
	if err != nil {
		return nil, err
	}
	return entries, nil
}

func (w *pebbleWAL) deleteMatching(match func(EntryRecord) bool) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if err := w.checkOpenLocked(); err != nil {
		return 0, err
	}

	keys := make([][]byte, 0)
	if err := w.forEachDirty(func(key []byte, record EntryRecord) error {
		if match(record) {
			keys = append(keys, append([]byte(nil), key...))
		}
		return nil
	}); err != nil {
		return 0, err
	}
	if len(keys) == 0 {
		return 0, nil
	}

	batch := w.db.NewIndexedBatch()
	defer batch.Close()
	for _, key := range keys {
		if err := batch.Delete(key, nil); err != nil {
			return 0, err
		}
	}
	if err := batch.Commit(walSyncWriteOptions); err != nil {
		return 0, err
	}
	w.depth -= len(keys)
	if w.depth < 0 {
		w.depth = 0
	}
	return len(keys), nil
}

func (w *pebbleWAL) countDirty() (int, error) {
	var count int
	err := w.forEachDirty(func(_ []byte, _ EntryRecord) error {
		count++
		return nil
	})
	return count, err
}

func (w *pebbleWAL) checkOpenLocked() error {
	if w.closed {
		return errors.New("wal is closed")
	}
	return nil
}

func (w *pebbleWAL) forEachDirty(f func([]byte, EntryRecord) error) error {
	iter, err := w.db.NewIter(&pebble.IterOptions{
		LowerBound: walDirtyPrefix,
		UpperBound: walDirtyUpperBound,
	})
	if err != nil {
		return err
	}
	defer iter.Close()

	for valid := iter.First(); valid; valid = iter.Next() {
		record, err := decodeRecord(iter.Value())
		if err != nil {
			return err
		}
		if err := f(iter.Key(), record); err != nil {
			return err
		}
	}
	return iter.Error()
}

func (w *pebbleWAL) load(key []byte) (EntryRecord, bool, error) {
	value, closer, err := w.db.Get(key)
	if errors.Is(err, pebble.ErrNotFound) {
		return EntryRecord{}, false, nil
	}
	if err != nil {
		return EntryRecord{}, false, err
	}
	defer closer.Close()

	record, err := decodeRecord(value)
	if err != nil {
		return EntryRecord{}, false, err
	}
	return record, true, nil
}

func (w *pebbleWAL) loadFrom(batch *pebble.Batch, key []byte) (EntryRecord, bool, error) {
	value, closer, err := batch.Get(key)
	if errors.Is(err, pebble.ErrNotFound) {
		return EntryRecord{}, false, nil
	}
	if err != nil {
		return EntryRecord{}, false, err
	}
	defer closer.Close()

	record, err := decodeRecord(value)
	if err != nil {
		return EntryRecord{}, false, err
	}
	return record, true, nil
}

func (w *pebbleWAL) nextSeq(batch *pebble.Batch) (int64, error) {
	var current uint64
	value, closer, err := batch.Get(walMetaSeqKey)
	if err != nil && !errors.Is(err, pebble.ErrNotFound) {
		return 0, err
	}
	if err == nil {
		if len(value) == 8 {
			current = binary.BigEndian.Uint64(value)
		}
		_ = closer.Close()
	}
	next := current + 1
	var encoded [8]byte
	binary.BigEndian.PutUint64(encoded[:], next)
	if err := batch.Set(walMetaSeqKey, encoded[:], nil); err != nil {
		return 0, err
	}
	return int64(next), nil
}

func putRecord(batch *pebble.Batch, key []byte, record EntryRecord) error {
	encoded, err := json.Marshal(record)
	if err != nil {
		return err
	}
	return batch.Set(key, encoded, nil)
}

func decodeRecord(encoded []byte) (EntryRecord, error) {
	var record EntryRecord
	if err := json.Unmarshal(encoded, &record); err != nil {
		return EntryRecord{}, err
	}
	return record.Clone(), nil
}

func dirtyKey(ref EntryRef) []byte {
	return []byte("dirty/" + walKey(ref))
}

func walKey(ref EntryRef) string {
	return ref.DMap + "\x00" + strconv.FormatUint(ref.HKey, 10)
}
