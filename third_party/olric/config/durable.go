package config

import (
	"context"
	"time"

	"github.com/olric-data/olric/pkg/storage"
)

type DurableOperation struct {
	DMap        string
	Key         string
	HKey        uint64
	PartitionID uint64
	Origin      string
	Entry       storage.Entry
	TTL         time.Duration
}

type DurableHook interface {
	BeforeSet(ctx context.Context, op DurableOperation) error
	BeforeDelete(ctx context.Context, op DurableOperation) error
	BeforeExpire(ctx context.Context, op DurableOperation) error
	LoadOnMiss(ctx context.Context, op DurableOperation) (storage.Entry, error)
}
