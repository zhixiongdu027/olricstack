package ring

import (
	"bytes"
	"errors"
	"fmt"
	"path/filepath"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
)

func newRing(t *testing.T, capacity uint64) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "ring.dat")
	if err := Create(path, capacity); err != nil {
		t.Fatalf("create ring: %v", err)
	}
	return path
}

func TestRoundTripSingleEntry(t *testing.T) {
	path := newRing(t, minCapacity)
	prod, err := OpenProducer(path)
	if err != nil {
		t.Fatalf("open producer: %v", err)
	}
	defer prod.Close()
	cons, err := OpenConsumer(path)
	if err != nil {
		t.Fatalf("open consumer: %v", err)
	}
	defer cons.Close()

	payload := []byte("hello dirty world")
	if err := prod.Append(payload); err != nil {
		t.Fatalf("append: %v", err)
	}
	got, ok, err := cons.Pop()
	if err != nil || !ok {
		t.Fatalf("pop: ok=%v err=%v", ok, err)
	}
	if !bytes.Equal(got, payload) {
		t.Fatalf("payload mismatch: got %q want %q", got, payload)
	}
	if _, ok, err := cons.Pop(); err != nil || ok {
		t.Fatalf("expected empty after one pop, got ok=%v err=%v", ok, err)
	}
}

func TestFifoOrder(t *testing.T) {
	path := newRing(t, minCapacity)
	prod, _ := OpenProducer(path)
	defer prod.Close()
	cons, _ := OpenConsumer(path)
	defer cons.Close()

	for i := 0; i < 50; i++ {
		if err := prod.Append([]byte(fmt.Sprintf("entry-%03d", i))); err != nil {
			t.Fatalf("append %d: %v", i, err)
		}
	}
	for i := 0; i < 50; i++ {
		got, ok, err := cons.Pop()
		if err != nil || !ok {
			t.Fatalf("pop %d: ok=%v err=%v", i, ok, err)
		}
		want := fmt.Sprintf("entry-%03d", i)
		if string(got) != want {
			t.Fatalf("entry %d: got %q want %q", i, got, want)
		}
	}
}

func TestWrapAround(t *testing.T) {
	path := newRing(t, minCapacity)
	prod, _ := OpenProducer(path)
	defer prod.Close()
	cons, _ := OpenConsumer(path)
	defer cons.Close()

	chunk := make([]byte, 600)
	for i := range chunk {
		chunk[i] = byte(i)
	}
	for cycle := 0; cycle < 5; cycle++ {
		for i := 0; i < 5; i++ {
			if err := prod.Append(chunk); err != nil {
				t.Fatalf("append cycle=%d i=%d: %v", cycle, i, err)
			}
		}
		for i := 0; i < 5; i++ {
			got, ok, err := cons.Pop()
			if err != nil || !ok {
				t.Fatalf("pop cycle=%d i=%d ok=%v err=%v", cycle, i, ok, err)
			}
			if !bytes.Equal(got, chunk) {
				t.Fatalf("payload mismatch cycle=%d i=%d", cycle, i)
			}
		}
	}
}

func TestFullErrors(t *testing.T) {
	path := newRing(t, minCapacity)
	prod, _ := OpenProducer(path)
	defer prod.Close()
	cons, _ := OpenConsumer(path)
	defer cons.Close()

	chunk := make([]byte, 1000)
	count := 0
	for {
		err := prod.Append(chunk)
		if err == nil {
			count++
			continue
		}
		if !errors.Is(err, ErrRingFull) {
			t.Fatalf("expected ErrRingFull after %d appends, got %v", count, err)
		}
		break
	}
	if count == 0 {
		t.Fatalf("ring rejected first append")
	}
	for i := 0; i < count; i++ {
		if _, ok, err := cons.Pop(); err != nil || !ok {
			t.Fatalf("drain pop %d ok=%v err=%v", i, ok, err)
		}
	}
	if err := prod.Append(chunk); err != nil {
		t.Fatalf("append after drain: %v", err)
	}
}

func TestPayloadTooLarge(t *testing.T) {
	path := newRing(t, minCapacity)
	prod, _ := OpenProducer(path)
	defer prod.Close()
	if err := prod.Append(make([]byte, minCapacity)); !errors.Is(err, ErrPayloadTooLarge) {
		t.Fatalf("expected ErrPayloadTooLarge, got %v", err)
	}
}

func TestReopenPreservesCursors(t *testing.T) {
	path := newRing(t, minCapacity)
	prod, _ := OpenProducer(path)
	if err := prod.Append([]byte("first")); err != nil {
		t.Fatalf("append: %v", err)
	}
	if err := prod.Append([]byte("second")); err != nil {
		t.Fatalf("append: %v", err)
	}
	prod.Close()

	cons, _ := OpenConsumer(path)
	got, ok, err := cons.Pop()
	if err != nil || !ok || string(got) != "first" {
		t.Fatalf("first pop: got=%q ok=%v err=%v", got, ok, err)
	}
	cons.Close()

	cons2, _ := OpenConsumer(path)
	defer cons2.Close()
	got, ok, err = cons2.Pop()
	if err != nil || !ok || string(got) != "second" {
		t.Fatalf("second pop after reopen: got=%q ok=%v err=%v", got, ok, err)
	}
}

func TestSPSCConcurrentAppendAndPop(t *testing.T) {
	path := newRing(t, minCapacity)
	prod, err := OpenProducer(path)
	if err != nil {
		t.Fatalf("open producer: %v", err)
	}
	defer prod.Close()
	cons, err := OpenConsumer(path)
	if err != nil {
		t.Fatalf("open consumer: %v", err)
	}
	defer cons.Close()

	const total = 2000
	payloads := make([][]byte, total)
	for i := 0; i < total; i++ {
		payloads[i] = []byte(fmt.Sprintf("entry-%04d", i))
	}

	var failed atomic.Bool
	errCh := make(chan error, 1)
	var received atomic.Int64
	var produced atomic.Int64
	var wg sync.WaitGroup
	recordErr := func(err error) {
		if err == nil {
			return
		}
		failed.Store(true)
		select {
		case errCh <- err:
		default:
		}
	}

	wg.Add(2)
	go func() {
		defer wg.Done()
		for i, payload := range payloads {
			if failed.Load() {
				return
			}
			for {
				if failed.Load() {
					return
				}
				if err := prod.Append(payload); err != nil {
					if errors.Is(err, ErrRingFull) {
						runtime.Gosched()
						continue
					}
					recordErr(fmt.Errorf("append %d: %w", i, err))
					return
				}
				produced.Add(1)
				break
			}
		}
	}()
	go func() {
		defer wg.Done()
		for {
			if failed.Load() {
				return
			}
			if int(received.Load()) >= total {
				return
			}
			got, ok, err := cons.Pop()
			if err != nil {
				recordErr(fmt.Errorf("pop: %w", err))
				return
			}
			if !ok {
				runtime.Gosched()
				continue
			}
			idx := int(received.Add(1)) - 1
			want := fmt.Sprintf("entry-%04d", idx)
			if string(got) != want {
				recordErr(fmt.Errorf("entry %d: got %q want %q", idx, got, want))
				return
			}
		}
	}()

	wg.Wait()
	select {
	case err := <-errCh:
		t.Fatal(err)
	default:
	}

	if n := int(produced.Load()); n != total {
		t.Fatalf("expected %d appends, got %d", total, n)
	}
	if n := int(received.Load()); n != total {
		t.Fatalf("expected %d pops, got %d", total, n)
	}
}
