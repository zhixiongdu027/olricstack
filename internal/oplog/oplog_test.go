package oplog

import (
	"errors"
	"strings"
	"testing"
)

func TestRoundTripSet(t *testing.T) {
	t.Parallel()
	in := Entry{
		Op:           OpSet,
		DMap:         "users",
		Key:          "alice",
		HKey:         1234,
		EncodedEntry: []byte("encoded"),
		TTL:          900,
		Timestamp:    42,
		Generation:   7,
		Epoch:        3,
		OwnerSeq:     11,
		WriterID:     "node-A",
	}
	payload, err := Encode(in)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	out, err := Decode(payload)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if out.Op != in.Op || out.DMap != in.DMap || out.Key != in.Key || out.HKey != in.HKey {
		t.Fatalf("identity mismatch: %+v vs %+v", out, in)
	}
	if string(out.EncodedEntry) != string(in.EncodedEntry) {
		t.Fatalf("encoded entry mismatch")
	}
	if out.Generation != in.Generation || out.Epoch != in.Epoch || out.OwnerSeq != in.OwnerSeq {
		t.Fatalf("fence mismatch")
	}
	if out.Version != currentVersion {
		t.Fatalf("expected version stamped, got %d", out.Version)
	}
}

func TestDecodeRejectsUnknownVersion(t *testing.T) {
	t.Parallel()
	payload := []byte(`{"v":99,"op":"set","dmap":"u","g":1,"s":1,"w":"x"}`)
	if _, err := Decode(payload); err == nil || !strings.Contains(err.Error(), "version") {
		t.Fatalf("expected version error, got %v", err)
	}
}

func TestDecodeRejectsUnknownOp(t *testing.T) {
	t.Parallel()
	bad := Entry{Op: "noop", DMap: "u", Generation: 1, OwnerSeq: 1, WriterID: "x"}
	payload, err := Encode(bad)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	if _, err := Decode(payload); err == nil || !strings.Contains(err.Error(), "unknown op") {
		t.Fatalf("expected unknown op error, got %v", err)
	}
}

func TestDecodeRejectsMissingFence(t *testing.T) {
	t.Parallel()
	bad := Entry{Op: OpSet, DMap: "u", WriterID: "x"}
	payload, err := Encode(bad)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	_, err = Decode(payload)
	if err == nil || !strings.Contains(err.Error(), "fence") {
		t.Fatalf("expected fence error, got %v", err)
	}
}

func TestDecodeRejectsCorruption(t *testing.T) {
	t.Parallel()
	_, err := Decode([]byte("not-json"))
	if err == nil || !errors.Is(err, err) {
		t.Fatalf("expected decode error, got nil")
	}
}
