package olricstore

import (
	"testing"

	olricstorage "github.com/olric-data/olric/pkg/storage"
)

func TestEnginePutGetRoundTrip(t *testing.T) {
	engine := New()
	entry := engine.NewEntry()
	entry.SetKey("key-a")
	entry.SetValue([]byte("value-a"))
	entry.SetTTL(10)
	entry.SetTimestamp(20)

	if err := engine.Put(42, entry); err != nil {
		t.Fatalf("put: %v", err)
	}
	got, err := engine.Get(42)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if string(got.Value()) != "value-a" {
		t.Fatalf("expected value-a, got %q", got.Value())
	}
}

func TestEngineGetReportsMissForUnknownKey(t *testing.T) {
	engine := New()
	if _, err := engine.Get(99); err != olricstorage.ErrKeyNotFound {
		t.Fatalf("expected ErrKeyNotFound, got %v", err)
	}
	if engine.Check(99) {
		t.Fatal("Check must return false for unknown hkey")
	}
}

func TestEnginePutRawRoundTrip(t *testing.T) {
	engine := New()
	entry := engine.NewEntry()
	entry.SetKey("raw-key")
	entry.SetValue([]byte("raw-value"))
	entry.SetTTL(10)
	entry.SetTimestamp(20)
	entry.SetLastAccess(30)

	if err := engine.PutRaw(7, entry.Encode()); err != nil {
		t.Fatalf("put raw: %v", err)
	}
	got, err := engine.Get(7)
	if err != nil {
		t.Fatalf("get raw: %v", err)
	}
	if got.Key() != "raw-key" || string(got.Value()) != "raw-value" || got.TTL() != 10 || got.Timestamp() != 20 || got.LastAccess() != 30 {
		t.Fatalf("unexpected entry: %#v", got)
	}
}

func TestEngineDeleteRemovesEntry(t *testing.T) {
	engine := New()
	entry := engine.NewEntry()
	entry.SetKey("delete-key")
	entry.SetValue([]byte("delete-value"))
	if err := engine.Put(9, entry); err != nil {
		t.Fatalf("put: %v", err)
	}
	if err := engine.Delete(9); err != nil {
		t.Fatalf("delete: %v", err)
	}
	if _, err := engine.Get(9); err != olricstorage.ErrKeyNotFound {
		t.Fatalf("expected miss after delete, got %v", err)
	}
}

func TestEngineUpdateTTLUpdatesExistingEntry(t *testing.T) {
	engine := New()
	entry := engine.NewEntry()
	entry.SetKey("ttl-key")
	entry.SetValue([]byte("ttl-value"))
	if err := engine.Put(10, entry); err != nil {
		t.Fatalf("put: %v", err)
	}
	update := engine.NewEntry()
	update.SetTTL(100)
	update.SetTimestamp(200)
	update.SetLastAccess(300)
	if err := engine.UpdateTTL(10, update); err != nil {
		t.Fatalf("update ttl: %v", err)
	}
	got, err := engine.Get(10)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if got.TTL() != 100 || got.Timestamp() != 200 || got.LastAccess() != 300 {
		t.Fatalf("unexpected ttl update: %#v", got)
	}
}

func TestEngineUpdateTTLMissReturnsNotFound(t *testing.T) {
	engine := New()
	update := engine.NewEntry()
	update.SetTTL(500)
	if err := engine.UpdateTTL(11, update); err != olricstorage.ErrKeyNotFound {
		t.Fatalf("expected ErrKeyNotFound, got %v", err)
	}
}

func TestEngineTransferIteratorExportImportDrop(t *testing.T) {
	source := New()
	entry := source.NewEntry()
	entry.SetKey("move-key")
	entry.SetValue([]byte("move-value"))
	if err := source.Put(9, entry); err != nil {
		t.Fatalf("put source: %v", err)
	}

	iterator := source.TransferIterator()
	if !iterator.Next() {
		t.Fatal("expected transfer iterator item")
	}
	payload, index, err := iterator.Export()
	if err != nil {
		t.Fatalf("export: %v", err)
	}

	target := New()
	if err := target.Import(payload, func(hkey uint64, entry olricstorage.Entry) error {
		return target.Put(hkey, entry)
	}); err != nil {
		t.Fatalf("import: %v", err)
	}
	got, err := target.Get(9)
	if err != nil {
		t.Fatalf("target get: %v", err)
	}
	if got.Key() != "move-key" || string(got.Value()) != "move-value" {
		t.Fatalf("unexpected moved entry: %#v", got)
	}

	if err := iterator.Drop(index); err != nil {
		t.Fatalf("drop: %v", err)
	}
	if _, err := source.Get(9); err != olricstorage.ErrKeyNotFound {
		t.Fatalf("expected source miss after drop, got %v", err)
	}
}
