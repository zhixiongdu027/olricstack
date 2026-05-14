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

func (h *DurableHook) BeforeSet(ctx context.Context, op olricconfig.DurableOperation) error {
	if op.Entry == nil {
		return errors.New("durable set entry is nil")
	}
	return h.store.StoreEntry(ctx, h.record(op, op.Entry, "client_set", true))
}

func (h *DurableHook) BeforeDelete(ctx context.Context, op olricconfig.DurableOperation) error {
	return h.store.DeleteEntry(ctx, store.EntryRef{DMap: op.DMap, Key: op.Key, HKey: op.HKey})
}

func (h *DurableHook) BeforeExpire(ctx context.Context, op olricconfig.DurableOperation) error {
	if op.Entry == nil {
		return errors.New("durable expire entry is nil")
	}
	return h.store.StoreEntry(ctx, h.record(op, op.Entry, "client_expire", true))
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

var _ olricconfig.DurableHook = (*DurableHook)(nil)
