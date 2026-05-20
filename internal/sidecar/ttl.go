package sidecar

import (
	"time"

	"github.com/zhixiongdu/olricstack/internal/store"
)

func recordExpired(rec store.EntryRecord) bool {
	return recordExpiredAt(rec, time.Now())
}

func recordExpiredAt(rec store.EntryRecord, now time.Time) bool {
	if rec.TTL <= 0 {
		return false
	}
	return now.UnixMilli() >= rec.TTL
}
