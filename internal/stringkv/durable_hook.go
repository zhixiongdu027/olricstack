package stringkv

import (
	"context"
	"errors"
	"fmt"
	"time"

	olricconfig "github.com/olric-data/olric/config"
	olricstorage "github.com/olric-data/olric/pkg/storage"
	"github.com/zhixiongdu/olricstack/internal/store"
)

type DurableHook struct {
	store store.CacheStore
	now   func() time.Time
}

func NewDurableHook(backing store.CacheStore) (*DurableHook, error) {
	if backing == nil {
		return nil, errors.New("backing store is nil")
	}
	return &DurableHook{
		store: backing,
		now:   time.Now,
	}, nil
}

func (h *DurableHook) BeforeSet(ctx context.Context, op olricconfig.DurableOperation) (olricconfig.DurableOperation, error) {
	if op.Entry == nil {
		return op, errors.New("durable set entry is nil")
	}
	record := h.record(op, op.Entry, "client_set", true)
	if committer, ok := h.store.(store.CommitStore); ok {
		prepared, err := committer.PrepareEntry(ctx, record)
		op.Version = prepared.Version
		return op, err
	}
	err := h.store.StoreEntry(ctx, record)
	return op, err
}

func (h *DurableHook) AfterSet(ctx context.Context, op olricconfig.DurableOperation) error {
	return h.commit(ctx, op)
}

func (h *DurableHook) BeforeDelete(ctx context.Context, op olricconfig.DurableOperation) (olricconfig.DurableOperation, error) {
	ref := store.EntryRef{DMap: op.DMap, Key: op.Key, HKey: op.HKey}
	if committer, ok := h.store.(store.CommitStore); ok {
		prepared, err := committer.PrepareDelete(ctx, ref)
		op.Version = prepared.Version
		return op, err
	}
	err := h.store.DeleteEntry(ctx, ref)
	return op, err
}

func (h *DurableHook) AfterDelete(ctx context.Context, op olricconfig.DurableOperation) error {
	return h.commit(ctx, op)
}

func (h *DurableHook) BeforeExpire(ctx context.Context, op olricconfig.DurableOperation) (olricconfig.DurableOperation, error) {
	if op.Entry == nil {
		return op, errors.New("durable expire entry is nil")
	}
	record := h.record(op, op.Entry, "client_expire", true)
	if committer, ok := h.store.(store.CommitStore); ok {
		prepared, err := committer.PrepareEntry(ctx, record)
		op.Version = prepared.Version
		return op, err
	}
	err := h.store.StoreEntry(ctx, record)
	return op, err
}

func (h *DurableHook) AfterExpire(ctx context.Context, op olricconfig.DurableOperation) error {
	return h.commit(ctx, op)
}

func (h *DurableHook) LoadOnMiss(ctx context.Context, op olricconfig.DurableOperation) (olricstorage.Entry, error) {
	record, err := h.store.LoadEntry(ctx, store.EntryRef{DMap: op.DMap, Key: op.Key, HKey: op.HKey})
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return nil, olricstorage.ErrKeyNotFound
		}
		return nil, fmt.Errorf("durable load miss: %w", err)
	}
	entry := op.Entry
	if entry == nil {
		return nil, errors.New("durable load miss entry template is nil")
	}
	entry.Decode(record.EncodedEntry)
	record.Origin = "mysql_refill"
	record.FlushMySQL = false
	if err := h.store.StoreEntry(ctx, record); err != nil {
		return nil, fmt.Errorf("record mysql refill: %w", err)
	}
	return entry, nil
}

func (h *DurableHook) record(op olricconfig.DurableOperation, entry olricstorage.Entry, origin string, flush bool) store.EntryRecord {
	now := h.now()
	return store.EntryRecord{
		DMap:         op.DMap,
		Key:          op.Key,
		HKey:         op.HKey,
		EncodedEntry: entry.Encode(),
		TTL:          entry.TTL(),
		Timestamp:    entry.Timestamp(),
		Origin:       origin,
		FlushMySQL:   flush,
		UpdatedAt:    now.UTC(),
	}
}

func (h *DurableHook) commit(ctx context.Context, op olricconfig.DurableOperation) error {
	if op.Version == 0 {
		return nil
	}
	committer, ok := h.store.(store.CommitStore)
	if !ok {
		return nil
	}
	return committer.CommitEntry(ctx, store.EntryRef{DMap: op.DMap, Key: op.Key, HKey: op.HKey}, op.Version)
}

var _ olricconfig.DurableHook = (*DurableHook)(nil)
