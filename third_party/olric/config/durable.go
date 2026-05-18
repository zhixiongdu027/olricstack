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

	// Fence is populated by BeforeSet/BeforeDelete/BeforeExpire as part of
	// the prepared record stamp and read back by VerifyAfterLock to confirm
	// the fence is still current at the moment the fragment lock is held.
	// Callers that don't run an out-of-lock prepare may leave these zero.
	FenceGeneration int64
	FenceEpoch      int64
	FenceOwnerSeq   int64
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

	// VerifyAfterLock is called by the fork after acquiring the per-fragment
	// write lock but BEFORE performing the in-memory mutation or replica
	// quorum. It re-checks that the fence stamped by BeforeSet/BeforeDelete
	// is still valid: the lease must not have expired and leadership must
	// not have moved to a different PRIMARY. A non-nil return aborts the
	// prepared durable record (the fork still calls AfterSet/AfterDelete
	// with the returned error so the hook can run its abort cleanup).
	//
	// Hooks that do not perform out-of-lock preparation may return nil
	// unconditionally. The fork passes the op as returned by the matching
	// BeforeX call.
	VerifyAfterLock(ctx context.Context, op DurableOperation) error
}
