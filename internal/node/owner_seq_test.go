package node

import (
	"errors"
	"path/filepath"
	"testing"
)

func TestOwnerSequenceStartsAtZeroAndAdvances(t *testing.T) {
	path := filepath.Join(t.TempDir(), "owner_seq.db")
	seq, err := NewOwnerSequence(path)
	if err != nil {
		t.Fatalf("new sequence: %v", err)
	}
	defer seq.Close()

	g, e, s := seq.Snapshot()
	if g != 0 || e != 0 || s != 0 {
		t.Fatalf("expected zero snapshot, got (%d,%d,%d)", g, e, s)
	}

	g, e, s, err = seq.Stamp(7, 3)
	if err != nil {
		t.Fatalf("first stamp: %v", err)
	}
	if g != 7 || e != 3 || s != 1 {
		t.Fatalf("expected (7,3,1), got (%d,%d,%d)", g, e, s)
	}

	g, e, s, err = seq.Stamp(7, 3)
	if err != nil {
		t.Fatalf("same fence stamp: %v", err)
	}
	if g != 7 || e != 3 || s != 2 {
		t.Fatalf("expected (7,3,2), got (%d,%d,%d)", g, e, s)
	}

	g, e, s, err = seq.Stamp(7, 4)
	if err != nil {
		t.Fatalf("epoch advance stamp: %v", err)
	}
	if g != 7 || e != 4 || s != 1 {
		t.Fatalf("expected (7,4,1) after epoch advance, got (%d,%d,%d)", g, e, s)
	}

	g, e, s, err = seq.Stamp(8, 0)
	if err != nil {
		t.Fatalf("generation advance stamp: %v", err)
	}
	if g != 8 || e != 0 || s != 1 {
		t.Fatalf("expected (8,0,1) after generation advance, got (%d,%d,%d)", g, e, s)
	}
}

func TestOwnerSequenceRejectsRollback(t *testing.T) {
	path := filepath.Join(t.TempDir(), "owner_seq.db")
	seq, err := NewOwnerSequence(path)
	if err != nil {
		t.Fatalf("new sequence: %v", err)
	}
	defer seq.Close()

	if _, _, _, err := seq.Stamp(10, 5); err != nil {
		t.Fatalf("seed stamp: %v", err)
	}

	if _, _, _, err := seq.Stamp(10, 4); !errors.Is(err, ErrOwnerSequenceRollback) {
		t.Fatalf("expected rollback rejection on epoch regress, got %v", err)
	}
	if _, _, _, err := seq.Stamp(9, 6); !errors.Is(err, ErrOwnerSequenceRollback) {
		t.Fatalf("expected rollback rejection on generation regress, got %v", err)
	}
}

func TestOwnerSequenceSurvivesRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "owner_seq.db")
	first, err := NewOwnerSequence(path)
	if err != nil {
		t.Fatalf("new first sequence: %v", err)
	}
	if _, _, _, err := first.Stamp(11, 2); err != nil {
		t.Fatalf("first stamp: %v", err)
	}
	if _, _, _, err := first.Stamp(11, 2); err != nil {
		t.Fatalf("second stamp: %v", err)
	}
	if err := first.Close(); err != nil {
		t.Fatalf("close first: %v", err)
	}

	second, err := NewOwnerSequence(path)
	if err != nil {
		t.Fatalf("reopen sequence: %v", err)
	}
	defer second.Close()

	g, e, s := second.Snapshot()
	if g != 11 || e != 2 || s != 2 {
		t.Fatalf("expected restart snapshot (11,2,2), got (%d,%d,%d)", g, e, s)
	}

	g, e, s, err = second.Stamp(11, 2)
	if err != nil {
		t.Fatalf("stamp after restart: %v", err)
	}
	if g != 11 || e != 2 || s != 3 {
		t.Fatalf("expected (11,2,3) post-restart, got (%d,%d,%d)", g, e, s)
	}
}

func TestOwnerSequenceCloseIdempotent(t *testing.T) {
	path := filepath.Join(t.TempDir(), "owner_seq.db")
	seq, err := NewOwnerSequence(path)
	if err != nil {
		t.Fatalf("new sequence: %v", err)
	}
	if err := seq.Close(); err != nil {
		t.Fatalf("first close: %v", err)
	}
	if err := seq.Close(); err != nil {
		t.Fatalf("second close: %v", err)
	}
	if _, _, _, err := seq.Stamp(1, 0); !errors.Is(err, ErrOwnerSequenceClosed) {
		t.Fatalf("expected closed rejection, got %v", err)
	}
}
