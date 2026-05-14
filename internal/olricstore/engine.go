package olricstore

import (
	"context"
	"errors"
	"log"
	"regexp"
	"sort"
	"sync"

	olricstorage "github.com/olric-data/olric/pkg/storage"
	"github.com/vmihailenco/msgpack/v5"
	"github.com/zhixiongdu/olricstack/internal/store"
)

type Engine struct {
	mu      sync.RWMutex
	entries map[uint64]olricstorage.Entry
	backing store.CacheStore
	logger  *log.Logger
}

func New(backing store.CacheStore) *Engine {
	return &Engine{
		entries: make(map[uint64]olricstorage.Entry),
		backing: backing,
	}
}

func (e *Engine) SetConfig(*olricstorage.Config) {}

func (e *Engine) SetLogger(logger *log.Logger) {
	e.logger = logger
}

func (e *Engine) Start() error {
	if e.entries == nil {
		e.entries = make(map[uint64]olricstorage.Entry)
	}
	if e.backing != nil {
		if replay, ok := e.backing.(store.ReplayStore); ok {
			if err := replay.Replay(context.Background(), func(record store.EntryRecord) error {
				entry := e.NewEntry()
				entry.Decode(record.EncodedEntry)
				e.mu.Lock()
				e.entries[record.HKey] = cloneEntry(entry)
				e.mu.Unlock()
				return nil
			}); err != nil {
				return err
			}
		}
		if starter, ok := e.backing.(store.Starter); ok {
			if err := starter.Start(context.Background()); err != nil {
				return err
			}
		}
	}
	return nil
}

func (e *Engine) NewEntry() olricstorage.Entry {
	return &Entry{}
}

func (e *Engine) Name() string {
	return "olricstack-mysql-wal"
}

func (e *Engine) Fork(*olricstorage.Config) (olricstorage.Engine, error) {
	return &Engine{
		entries: make(map[uint64]olricstorage.Entry),
		backing: e.backing,
		logger:  e.logger,
	}, nil
}

func (e *Engine) PutRaw(hkey uint64, raw []byte) error {
	entry := e.NewEntry()
	entry.Decode(raw)
	return e.Put(hkey, entry)
}

func (e *Engine) Put(hkey uint64, entry olricstorage.Entry) error {
	cloned := cloneEntry(entry)
	e.mu.Lock()
	e.entries[hkey] = cloned
	e.mu.Unlock()

	return nil
}

func (e *Engine) GetRaw(hkey uint64) ([]byte, error) {
	entry, err := e.Get(hkey)
	if err != nil {
		return nil, err
	}
	return append([]byte(nil), entry.Encode()...), nil
}

func (e *Engine) Get(hkey uint64) (olricstorage.Entry, error) {
	e.mu.RLock()
	entry, ok := e.entries[hkey]
	e.mu.RUnlock()
	if ok {
		return cloneEntry(entry), nil
	}
	return nil, olricstorage.ErrKeyNotFound
}

func (e *Engine) GetTTL(hkey uint64) (int64, error) {
	entry, err := e.Get(hkey)
	if err != nil {
		return 0, err
	}
	return entry.TTL(), nil
}

func (e *Engine) GetLastAccess(hkey uint64) (int64, error) {
	entry, err := e.Get(hkey)
	if err != nil {
		return 0, err
	}
	return entry.LastAccess(), nil
}

func (e *Engine) GetKey(hkey uint64) (string, error) {
	entry, err := e.Get(hkey)
	if err != nil {
		return "", err
	}
	return entry.Key(), nil
}

func (e *Engine) Delete(hkey uint64) error {
	e.mu.Lock()
	delete(e.entries, hkey)
	e.mu.Unlock()
	return nil
}

func (e *Engine) UpdateTTL(hkey uint64, entry olricstorage.Entry) error {
	e.mu.Lock()

	current, ok := e.entries[hkey]
	if !ok {
		e.mu.Unlock()
		return olricstorage.ErrKeyNotFound
	} else {
		e.mu.Unlock()
	}
	current.SetTTL(entry.TTL())
	current.SetTimestamp(entry.Timestamp())
	current.SetLastAccess(entry.LastAccess())
	cloned := cloneEntry(current)
	e.mu.Lock()
	e.entries[hkey] = cloned
	e.mu.Unlock()
	return nil
}

func (e *Engine) TransferIterator() olricstorage.TransferIterator {
	e.mu.RLock()
	defer e.mu.RUnlock()

	items := make([]transferItem, 0, len(e.entries))
	for hkey, entry := range e.entries {
		items = append(items, transferItem{HKey: hkey, Raw: append([]byte(nil), entry.Encode()...)})
	}
	sort.Slice(items, func(i, j int) bool {
		return items[i].HKey < items[j].HKey
	})
	return &transferIterator{engine: e, items: items}
}

func (e *Engine) Import(data []byte, f func(uint64, olricstorage.Entry) error) error {
	var items []transferItem
	if err := msgpack.Unmarshal(data, &items); err != nil {
		return err
	}
	for _, item := range items {
		entry := e.NewEntry()
		entry.Decode(item.Raw)
		if err := f(item.HKey, entry); err != nil {
			return err
		}
	}
	return nil
}

func (e *Engine) Stats() olricstorage.Stats {
	e.mu.RLock()
	defer e.mu.RUnlock()

	var inuse int
	for _, entry := range e.entries {
		inuse += len(entry.Key()) + len(entry.Value()) + 32
	}
	return olricstorage.Stats{
		Allocated: inuse,
		Inuse:     inuse,
		Length:    len(e.entries),
		NumTables: 1,
	}
}

func (e *Engine) Check(hkey uint64) bool {
	e.mu.RLock()
	defer e.mu.RUnlock()
	_, ok := e.entries[hkey]
	return ok
}

func (e *Engine) Range(f func(uint64, olricstorage.Entry) bool) {
	e.mu.RLock()
	items := make(map[uint64]olricstorage.Entry, len(e.entries))
	for hkey, entry := range e.entries {
		items[hkey] = cloneEntry(entry)
	}
	e.mu.RUnlock()

	for hkey, entry := range items {
		if !f(hkey, entry) {
			return
		}
	}
}

func (e *Engine) RangeHKey(f func(uint64) bool) {
	e.Range(func(hkey uint64, _ olricstorage.Entry) bool {
		return f(hkey)
	})
}

func (e *Engine) Scan(cursor uint64, count int, f func(olricstorage.Entry) bool) (uint64, error) {
	return e.scan(cursor, count, nil, f)
}

func (e *Engine) ScanRegexMatch(cursor uint64, match string, count int, f func(olricstorage.Entry) bool) (uint64, error) {
	re, err := regexp.Compile(match)
	if err != nil {
		return 0, err
	}
	return e.scan(cursor, count, re, f)
}

func (e *Engine) scan(cursor uint64, count int, re *regexp.Regexp, f func(olricstorage.Entry) bool) (uint64, error) {
	e.mu.RLock()
	hkeys := make([]uint64, 0, len(e.entries))
	for hkey := range e.entries {
		if hkey >= cursor {
			hkeys = append(hkeys, hkey)
		}
	}
	sort.Slice(hkeys, func(i, j int) bool { return hkeys[i] < hkeys[j] })
	entries := make([]olricstorage.Entry, 0, len(hkeys))
	for _, hkey := range hkeys {
		entries = append(entries, cloneEntry(e.entries[hkey]))
	}
	e.mu.RUnlock()

	if count <= 0 {
		count = len(entries)
	}
	var emitted int
	for i, entry := range entries {
		if re != nil && !re.MatchString(entry.Key()) {
			continue
		}
		if !f(entry) {
			return hkeys[i], nil
		}
		emitted++
		if emitted >= count {
			if i+1 < len(hkeys) {
				return hkeys[i+1], nil
			}
			return 0, nil
		}
	}
	return 0, nil
}

func (e *Engine) Compaction() (bool, error) {
	return false, nil
}

func (e *Engine) Close() error {
	return nil
}

func (e *Engine) Destroy() error {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.entries = make(map[uint64]olricstorage.Entry)
	return nil
}

type transferItem struct {
	HKey uint64 `msgpack:"hkey"`
	Raw  []byte `msgpack:"raw"`
}

type transferIterator struct {
	engine *Engine
	items  []transferItem
	next   bool
}

func (i *transferIterator) Next() bool {
	if i.next || len(i.items) == 0 {
		return false
	}
	i.next = true
	return true
}

func (i *transferIterator) Export() ([]byte, int, error) {
	if !i.next {
		return nil, 0, errors.New("transfer iterator is not positioned")
	}
	data, err := msgpack.Marshal(i.items)
	if err != nil {
		return nil, 0, err
	}
	return data, 0, nil
}

func (i *transferIterator) Drop(int) error {
	i.engine.mu.Lock()
	defer i.engine.mu.Unlock()
	for _, item := range i.items {
		delete(i.engine.entries, item.HKey)
	}
	return nil
}

func cloneEntry(entry olricstorage.Entry) olricstorage.Entry {
	cloned := &Entry{}
	cloned.SetKey(entry.Key())
	cloned.SetValue(append([]byte(nil), entry.Value()...))
	cloned.SetTTL(entry.TTL())
	cloned.SetTimestamp(entry.Timestamp())
	cloned.SetLastAccess(entry.LastAccess())
	return cloned
}

var _ olricstorage.Engine = (*Engine)(nil)
