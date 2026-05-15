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
	Version     int64
	Entry       storage.Entry
	TTL         time.Duration
}

type DurableHook interface {
	BeforeSet(ctx context.Context, op DurableOperation) (DurableOperation, error)
	// AfterSet runs unconditionally after the owner-side mutation has been
	// attempted. mutationErr is non-nil iff the mutation (and any required
	// quorum/replication step) failed; the hook uses it to decide between
	// commit and abort of any prepared durable record.
	AfterSet(ctx context.Context, op DurableOperation, mutationErr error) error
	BeforeDelete(ctx context.Context, op DurableOperation) (DurableOperation, error)
	AfterDelete(ctx context.Context, op DurableOperation, mutationErr error) error
	BeforeExpire(ctx context.Context, op DurableOperation) (DurableOperation, error)
	AfterExpire(ctx context.Context, op DurableOperation, mutationErr error) error
	LoadOnMiss(ctx context.Context, op DurableOperation) (storage.Entry, error)
}
