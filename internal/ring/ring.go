// Package ring implements a single-producer / single-consumer mmap-backed
// byte ring used as the dirty-data IPC between olric-node and olric-sidecar.
//
// The ring is a fixed-size file mapped with MAP_SHARED so both containers see
// the same bytes. Producer and consumer cursors are 64-bit absolute counters
// stored in the file header; the in-ring byte position is `cursor % capacity`.
//
// The ring is *not* expected to survive pod reschedule — it lives in a tmpfs
// emptyDir{medium: Memory}. The contract with the application is "Append
// returning success means the dirty entry is durably visible to the
// consumer process within the lifetime of this Pod". MySQL durability is the
// consumer's job and is asynchronous.
//
// Concurrency: Append and Pop are NOT goroutine-safe. The caller must ensure
// at most one goroutine is appending and at most one goroutine is popping at
// any time. Cross-process producer/consumer is the supported topology.
package ring

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"os"
	"sync/atomic"
	"unsafe"

	"golang.org/x/sys/unix"
)

const (
	magic       uint32 = 0x4F524E47 // "ORNG"
	version     uint32 = 1
	headerSize         = 64
	skipMarker  uint32 = 0xFFFFFFFF
	maxPayload  uint32 = 0xFFFFFFFE
	minCapacity uint64 = 4096
)

var crcTable = crc32.MakeTable(crc32.Castagnoli)

// Errors returned by Producer / Consumer.
var (
	ErrPayloadTooLarge = errors.New("ring: payload exceeds ring capacity")
	ErrRingFull        = errors.New("ring: full")
	ErrCorrupted       = errors.New("ring: corrupted entry")
)

// Create initialises a new ring file. Existing content is truncated.
// `capacity` is the data-region size; the file occupies `headerSize + capacity`
// bytes on disk. Capacity is rounded up to a multiple of 8.
func Create(path string, capacity uint64) error {
	if capacity < minCapacity {
		return fmt.Errorf("ring: capacity %d below minimum %d", capacity, minCapacity)
	}
	if capacity%8 != 0 {
		capacity += 8 - capacity%8
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0o600)
	if err != nil {
		return err
	}
	defer f.Close()
	total := int64(headerSize) + int64(capacity)
	if err := f.Truncate(total); err != nil {
		return err
	}
	data, err := unix.Mmap(int(f.Fd()), 0, int(total), unix.PROT_READ|unix.PROT_WRITE, unix.MAP_SHARED)
	if err != nil {
		return fmt.Errorf("ring: mmap: %w", err)
	}
	defer unix.Munmap(data)
	binary.LittleEndian.PutUint32(data[0:4], magic)
	binary.LittleEndian.PutUint32(data[4:8], version)
	binary.LittleEndian.PutUint64(data[8:16], capacity)
	binary.LittleEndian.PutUint64(data[16:24], 0)
	binary.LittleEndian.PutUint64(data[24:32], 0)
	return nil
}

// mapping holds an open mmap of a ring file.
type mapping struct {
	file     *os.File
	data     []byte
	capacity uint64
	headPtr  *uint64
	tailPtr  *uint64
}

func (m *mapping) close() error {
	if m == nil {
		return nil
	}
	var firstErr error
	if m.data != nil {
		if err := unix.Munmap(m.data); err != nil {
			firstErr = err
		}
		m.data = nil
	}
	if m.file != nil {
		if err := m.file.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
		m.file = nil
	}
	return firstErr
}

func openMapping(path string) (*mapping, error) {
	f, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		return nil, err
	}
	info, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, err
	}
	if info.Size() < int64(headerSize) {
		f.Close()
		return nil, fmt.Errorf("ring: file too small: %d", info.Size())
	}
	data, err := unix.Mmap(int(f.Fd()), 0, int(info.Size()), unix.PROT_READ|unix.PROT_WRITE, unix.MAP_SHARED)
	if err != nil {
		f.Close()
		return nil, fmt.Errorf("ring: mmap: %w", err)
	}
	gotMagic := binary.LittleEndian.Uint32(data[0:4])
	gotVersion := binary.LittleEndian.Uint32(data[4:8])
	capacity := binary.LittleEndian.Uint64(data[8:16])
	if gotMagic != magic {
		_ = unix.Munmap(data)
		_ = f.Close()
		return nil, fmt.Errorf("ring: bad magic %#x", gotMagic)
	}
	if gotVersion != version {
		_ = unix.Munmap(data)
		_ = f.Close()
		return nil, fmt.Errorf("ring: unsupported version %d", gotVersion)
	}
	if int64(headerSize)+int64(capacity) != info.Size() {
		_ = unix.Munmap(data)
		_ = f.Close()
		return nil, fmt.Errorf("ring: size mismatch header+capacity=%d file=%d", int64(headerSize)+int64(capacity), info.Size())
	}
	return &mapping{
		file:     f,
		data:     data,
		capacity: capacity,
		headPtr:  (*uint64)(unsafe.Pointer(&data[16])),
		tailPtr:  (*uint64)(unsafe.Pointer(&data[24])),
	}, nil
}

// Producer appends entries to a ring. Not goroutine-safe.
type Producer struct {
	m *mapping
}

// OpenProducer maps an existing ring file for writing.
func OpenProducer(path string) (*Producer, error) {
	m, err := openMapping(path)
	if err != nil {
		return nil, err
	}
	return &Producer{m: m}, nil
}

// Close unmaps the ring file. Pending Append calls must have returned.
func (p *Producer) Close() error {
	return p.m.close()
}

// Capacity returns the data region size in bytes.
func (p *Producer) Capacity() uint64 { return p.m.capacity }

// Pending reports the number of producer-written, consumer-unread bytes.
func (p *Producer) Pending() uint64 {
	return atomic.LoadUint64(p.m.tailPtr) - atomic.LoadUint64(p.m.headPtr)
}

// Append writes payload to the ring. Returns ErrRingFull if the consumer is
// behind, or ErrPayloadTooLarge if the payload cannot fit even in an empty
// ring.
func (p *Producer) Append(payload []byte) error {
	payloadLen := uint32(len(payload))
	if uint64(payloadLen) > uint64(maxPayload) {
		return ErrPayloadTooLarge
	}
	entrySize := alignUp(uint64(8) + uint64(payloadLen))
	capacity := p.m.capacity
	if entrySize > capacity {
		return ErrPayloadTooLarge
	}

	head := atomic.LoadUint64(p.m.headPtr)
	tail := atomic.LoadUint64(p.m.tailPtr)
	used := tail - head
	free := capacity - used

	posInRing := tail % capacity
	spaceToBoundary := capacity - posInRing
	needSkip := spaceToBoundary < entrySize
	needed := entrySize
	if needSkip {
		needed = spaceToBoundary + entrySize
	}
	if needed > free {
		return ErrRingFull
	}

	if needSkip {
		base := headerSize + int(posInRing)
		binary.LittleEndian.PutUint32(p.m.data[base:base+4], skipMarker)
		binary.LittleEndian.PutUint32(p.m.data[base+4:base+8], 0)
		tail += spaceToBoundary
		posInRing = 0
	}

	base := headerSize + int(posInRing)
	binary.LittleEndian.PutUint32(p.m.data[base:base+4], payloadLen)
	crc := crc32.Checksum(payload, crcTable)
	binary.LittleEndian.PutUint32(p.m.data[base+4:base+8], crc)
	copy(p.m.data[base+8:base+8+int(payloadLen)], payload)

	atomic.StoreUint64(p.m.tailPtr, tail+entrySize)
	return nil
}

// Consumer reads entries from a ring. Not goroutine-safe.
type Consumer struct {
	m *mapping
}

// OpenConsumer maps an existing ring file for reading.
func OpenConsumer(path string) (*Consumer, error) {
	m, err := openMapping(path)
	if err != nil {
		return nil, err
	}
	return &Consumer{m: m}, nil
}

// Close unmaps the ring file.
func (c *Consumer) Close() error { return c.m.close() }

// Capacity returns the data region size in bytes.
func (c *Consumer) Capacity() uint64 { return c.m.capacity }

// Pop returns the next available entry, or (nil, false, nil) if the ring is
// empty. Returned slice is freshly allocated and owned by the caller.
func (c *Consumer) Pop() ([]byte, bool, error) {
	for {
		tail := atomic.LoadUint64(c.m.tailPtr)
		head := atomic.LoadUint64(c.m.headPtr)
		if head == tail {
			return nil, false, nil
		}
		capacity := c.m.capacity
		posInRing := head % capacity
		base := headerSize + int(posInRing)
		rawLen := binary.LittleEndian.Uint32(c.m.data[base : base+4])

		if rawLen == skipMarker {
			spaceToBoundary := capacity - posInRing
			atomic.StoreUint64(c.m.headPtr, head+spaceToBoundary)
			continue
		}
		if uint64(rawLen) > uint64(maxPayload) {
			return nil, false, fmt.Errorf("%w: bogus length %#x at head=%d", ErrCorrupted, rawLen, head)
		}
		payloadLen := uint64(rawLen)
		entrySize := alignUp(uint64(8) + payloadLen)
		if entrySize > capacity-posInRing {
			return nil, false, fmt.Errorf("%w: entry crosses boundary at head=%d", ErrCorrupted, head)
		}
		crc := binary.LittleEndian.Uint32(c.m.data[base+4 : base+8])
		payload := make([]byte, payloadLen)
		copy(payload, c.m.data[base+8:base+8+int(payloadLen)])
		if got := crc32.Checksum(payload, crcTable); got != crc {
			return nil, false, fmt.Errorf("%w: crc mismatch at head=%d (got %#x want %#x)", ErrCorrupted, head, got, crc)
		}
		atomic.StoreUint64(c.m.headPtr, head+entrySize)
		return payload, true, nil
	}
}

// Pending reports the number of producer-written, consumer-unread bytes.
// Used by metrics and tests.
func (c *Consumer) Pending() uint64 {
	return atomic.LoadUint64(c.m.tailPtr) - atomic.LoadUint64(c.m.headPtr)
}

func alignUp(n uint64) uint64 {
	if n%8 == 0 {
		return n
	}
	return n + (8 - n%8)
}
