package stringkv

import (
	"errors"
	"fmt"
	"sync"
)

// MemoryFenceSequencer is the in-memory FenceSequencer used by the
// shm-oplog architecture. Persistence is unnecessary because writer_id is
// per-pod-instance (a fresh UUID at boot), making the (G, E, writer_id, S)
// tuple unique even if S restarts at 1 across crashes. MySQL's strict-lex
// upsert comparator absorbs the resulting holes.
type MemoryFenceSequencer struct {
	mu sync.Mutex

	generation int64
	epoch      int64
	seq        int64
}

func NewMemoryFenceSequencer() *MemoryFenceSequencer {
	return &MemoryFenceSequencer{}
}

// Stamp returns the next (g, e, s) under the requested fence window. A
// strictly newer fence resets seq to 1; the current fence increments seq;
// an older fence is rejected with ErrOwnerSequenceRollback.
func (s *MemoryFenceSequencer) Stamp(generation, epoch int64) (int64, int64, int64, error) {
	if generation <= 0 || epoch < 0 {
		return 0, 0, 0, fmt.Errorf("invalid fence generation=%d epoch=%d", generation, epoch)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	switch {
	case generation > s.generation || (generation == s.generation && epoch > s.epoch):
		s.generation = generation
		s.epoch = epoch
		s.seq = 1
	case generation == s.generation && epoch == s.epoch:
		s.seq++
	default:
		return 0, 0, 0, fmt.Errorf("%w: have (%d,%d) requested (%d,%d)",
			ErrOwnerSequenceRollback, s.generation, s.epoch, generation, epoch)
	}
	return s.generation, s.epoch, s.seq, nil
}

// ErrOwnerSequenceRollback is returned when Stamp receives a fence strictly
// older than the last seen one.
var ErrOwnerSequenceRollback = errors.New("owner sequence rollback rejected")
