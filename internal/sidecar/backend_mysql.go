package sidecar

import (
	"context"
	"errors"

	"github.com/zhixiongdu/olricstack/internal/store"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// MySQLBackend adapts a gorm.DB to the Backend interface. The flusher upserts
// records using a fence-aware OnConflict clause (strict lex
// `(generation, epoch, owner_seq, writer_id)`) so a delayed flush from an old
// owner provably loses to a newer-owner write.
type MySQLBackend struct {
	db        *gorm.DB
	batchSize int
}

func NewMySQLBackend(db *gorm.DB, batchSize int) (*MySQLBackend, error) {
	if db == nil {
		return nil, errors.New("gorm db is nil")
	}
	if err := db.AutoMigrate(&store.EntryRecord{}); err != nil {
		return nil, err
	}
	if batchSize <= 0 {
		batchSize = 256
	}
	return &MySQLBackend{db: db, batchSize: batchSize}, nil
}

func (b *MySQLBackend) UpsertEntries(ctx context.Context, records []store.EntryRecord) error {
	if len(records) == 0 {
		return nil
	}
	return b.db.WithContext(ctx).Clauses(b.upsertClause()).CreateInBatches(records, b.batchSize).Error
}

func (b *MySQLBackend) LoadFromMySQL(ctx context.Context, ref store.EntryRef) (store.EntryRecord, error) {
	var rec store.EntryRecord
	err := b.db.WithContext(ctx).First(&rec, "dmap = ? AND hkey = ?", ref.DMap, ref.HKey).Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return store.EntryRecord{}, store.ErrNotFound
		}
		return store.EntryRecord{}, err
	}
	if rec.Tombstone {
		return store.EntryRecord{}, store.ErrNotFound
	}
	return rec, nil
}

func (b *MySQLBackend) PurgeBelowGeneration(ctx context.Context, minGeneration int64) (int, error) {
	if minGeneration <= 0 {
		return 0, nil
	}
	res := b.db.WithContext(ctx).Where("generation < ?", minGeneration).Delete(&store.EntryRecord{})
	if res.Error != nil {
		return 0, res.Error
	}
	return int(res.RowsAffected), nil
}

func (b *MySQLBackend) Close(ctx context.Context) error {
	sqlDB, err := b.db.DB()
	if err != nil {
		return err
	}
	return sqlDB.Close()
}

// upsertClause encodes the fence-aware conflict-resolution rule used by the
// asynchronous flusher. Conflict resolution is a strict lex comparison of
// (generation, epoch, owner_seq, writer_id); writer_id is the tiebreaker
// that keeps the comparator total when (G, E, S) tie (e.g. two writers
// stamped the same triple under a balancer race).
func (b *MySQLBackend) upsertClause() clause.OnConflict {
	if b.db.Dialector.Name() == "mysql" {
		newer := "VALUES(generation) > generation OR " +
			"(VALUES(generation) = generation AND VALUES(epoch) > epoch) OR " +
			"(VALUES(generation) = generation AND VALUES(epoch) = epoch AND VALUES(owner_seq) > owner_seq) OR " +
			"(VALUES(generation) = generation AND VALUES(epoch) = epoch AND VALUES(owner_seq) = owner_seq AND VALUES(writer_id) > writer_id)"
		return clause.OnConflict{
			Columns: []clause.Column{{Name: "dmap"}, {Name: "hkey"}},
			DoUpdates: clause.Assignments(map[string]any{
				"key":           gorm.Expr("CASE WHEN " + newer + " THEN VALUES(`key`) ELSE `key` END"),
				"encoded_entry": gorm.Expr("CASE WHEN " + newer + " THEN VALUES(encoded_entry) ELSE encoded_entry END"),
				"ttl":           gorm.Expr("CASE WHEN " + newer + " THEN VALUES(ttl) ELSE ttl END"),
				"timestamp":     gorm.Expr("CASE WHEN " + newer + " THEN VALUES(timestamp) ELSE timestamp END"),
				"tombstone":     gorm.Expr("CASE WHEN " + newer + " THEN VALUES(tombstone) ELSE tombstone END"),
				"generation":    gorm.Expr("CASE WHEN " + newer + " THEN VALUES(generation) ELSE generation END"),
				"epoch":         gorm.Expr("CASE WHEN " + newer + " THEN VALUES(epoch) ELSE epoch END"),
				"owner_seq":     gorm.Expr("CASE WHEN " + newer + " THEN VALUES(owner_seq) ELSE owner_seq END"),
				"writer_id":     gorm.Expr("CASE WHEN " + newer + " THEN VALUES(writer_id) ELSE writer_id END"),
				"updated_at":    gorm.Expr("CASE WHEN " + newer + " THEN VALUES(updated_at) ELSE updated_at END"),
			}),
		}
	}
	newer := "excluded.generation > generation OR " +
		"(excluded.generation = generation AND excluded.epoch > epoch) OR " +
		"(excluded.generation = generation AND excluded.epoch = epoch AND excluded.owner_seq > owner_seq) OR " +
		"(excluded.generation = generation AND excluded.epoch = epoch AND excluded.owner_seq = owner_seq AND excluded.writer_id > writer_id)"
	return clause.OnConflict{
		Columns: []clause.Column{{Name: "dmap"}, {Name: "hkey"}},
		DoUpdates: clause.Assignments(map[string]any{
			"key":           gorm.Expr("CASE WHEN " + newer + " THEN excluded.key ELSE key END"),
			"encoded_entry": gorm.Expr("CASE WHEN " + newer + " THEN excluded.encoded_entry ELSE encoded_entry END"),
			"ttl":           gorm.Expr("CASE WHEN " + newer + " THEN excluded.ttl ELSE ttl END"),
			"timestamp":     gorm.Expr("CASE WHEN " + newer + " THEN excluded.timestamp ELSE timestamp END"),
			"tombstone":     gorm.Expr("CASE WHEN " + newer + " THEN excluded.tombstone ELSE tombstone END"),
			"generation":    gorm.Expr("CASE WHEN " + newer + " THEN excluded.generation ELSE generation END"),
			"epoch":         gorm.Expr("CASE WHEN " + newer + " THEN excluded.epoch ELSE epoch END"),
			"owner_seq":     gorm.Expr("CASE WHEN " + newer + " THEN excluded.owner_seq ELSE owner_seq END"),
			"writer_id":     gorm.Expr("CASE WHEN " + newer + " THEN excluded.writer_id ELSE writer_id END"),
			"updated_at":    gorm.Expr("CASE WHEN " + newer + " THEN excluded.updated_at ELSE updated_at END"),
		}),
	}
}

var _ Backend = (*MySQLBackend)(nil)
