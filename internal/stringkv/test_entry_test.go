package stringkv

import (
	"encoding/json"
)

type testEntry struct {
	key        string
	value      []byte
	ttl        int64
	timestamp  int64
	lastAccess int64
}

func (e *testEntry) SetKey(key string) {
	e.key = key
}

func (e *testEntry) Key() string {
	return e.key
}

func (e *testEntry) SetValue(value []byte) {
	e.value = append([]byte(nil), value...)
}

func (e *testEntry) Value() []byte {
	return append([]byte(nil), e.value...)
}

func (e *testEntry) SetTTL(ttl int64) {
	e.ttl = ttl
}

func (e *testEntry) TTL() int64 {
	return e.ttl
}

func (e *testEntry) SetTimestamp(timestamp int64) {
	e.timestamp = timestamp
}

func (e *testEntry) Timestamp() int64 {
	return e.timestamp
}

func (e *testEntry) SetLastAccess(lastAccess int64) {
	e.lastAccess = lastAccess
}

func (e *testEntry) LastAccess() int64 {
	return e.lastAccess
}

func (e *testEntry) Encode() []byte {
	encoded, _ := json.Marshal(struct {
		Key        string `json:"key"`
		Value      []byte `json:"value"`
		TTL        int64  `json:"ttl"`
		Timestamp  int64  `json:"timestamp"`
		LastAccess int64  `json:"last_access"`
	}{
		Key:        e.key,
		Value:      e.value,
		TTL:        e.ttl,
		Timestamp:  e.timestamp,
		LastAccess: e.lastAccess,
	})
	return encoded
}

func (e *testEntry) Decode(buf []byte) {
	var decoded struct {
		Key        string `json:"key"`
		Value      []byte `json:"value"`
		TTL        int64  `json:"ttl"`
		Timestamp  int64  `json:"timestamp"`
		LastAccess int64  `json:"last_access"`
	}
	_ = json.Unmarshal(buf, &decoded)
	e.key = decoded.Key
	e.value = append([]byte(nil), decoded.Value...)
	e.ttl = decoded.TTL
	e.timestamp = decoded.Timestamp
	e.lastAccess = decoded.LastAccess
}
