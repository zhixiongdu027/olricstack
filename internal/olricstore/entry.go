package olricstore

import (
	"encoding/binary"

	olricstorage "github.com/olric-data/olric/pkg/storage"
)

type Entry struct {
	key        string
	value      []byte
	ttl        int64
	timestamp  int64
	lastAccess int64
}

func (e *Entry) SetKey(key string) {
	e.key = key
}

func (e *Entry) Key() string {
	return e.key
}

func (e *Entry) SetValue(value []byte) {
	e.value = append([]byte(nil), value...)
}

func (e *Entry) Value() []byte {
	return append([]byte(nil), e.value...)
}

func (e *Entry) SetTTL(ttl int64) {
	e.ttl = ttl
}

func (e *Entry) TTL() int64 {
	return e.ttl
}

func (e *Entry) SetTimestamp(timestamp int64) {
	e.timestamp = timestamp
}

func (e *Entry) Timestamp() int64 {
	return e.timestamp
}

func (e *Entry) SetLastAccess(lastAccess int64) {
	e.lastAccess = lastAccess
}

func (e *Entry) LastAccess() int64 {
	return e.lastAccess
}

func (e *Entry) Encode() []byte {
	key := []byte(e.key)
	value := e.value
	buf := make([]byte, 1+len(key)+8+8+8+4+len(value))
	offset := 0
	buf[offset] = byte(len(key))
	offset++
	copy(buf[offset:], key)
	offset += len(key)
	binary.BigEndian.PutUint64(buf[offset:], uint64(e.ttl))
	offset += 8
	binary.BigEndian.PutUint64(buf[offset:], uint64(e.timestamp))
	offset += 8
	binary.BigEndian.PutUint64(buf[offset:], uint64(e.lastAccess))
	offset += 8
	binary.BigEndian.PutUint32(buf[offset:], uint32(len(value)))
	offset += 4
	copy(buf[offset:], value)
	return buf
}

func (e *Entry) Decode(buf []byte) {
	offset := 0
	keyLen := int(buf[offset])
	offset++
	e.key = string(buf[offset : offset+keyLen])
	offset += keyLen
	e.ttl = int64(binary.BigEndian.Uint64(buf[offset:]))
	offset += 8
	e.timestamp = int64(binary.BigEndian.Uint64(buf[offset:]))
	offset += 8
	e.lastAccess = int64(binary.BigEndian.Uint64(buf[offset:]))
	offset += 8
	valueLen := int(binary.BigEndian.Uint32(buf[offset:]))
	offset += 4
	e.value = append([]byte(nil), buf[offset:offset+valueLen]...)
}

var _ olricstorage.Entry = (*Entry)(nil)
