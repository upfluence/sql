package sqltest

import (
	"testing"

	"github.com/upfluence/log/logtest"

	"github.com/upfluence/sql"
	"github.com/upfluence/sql/x/migration"
)

type MigrationMap map[string]string

func (mm MigrationMap) fetch(n string) ([]byte, error) {
	m, ok := mm[n]

	if !ok {
		return nil, migration.ErrNotExist
	}

	return []byte(m), nil
}

func (mm MigrationMap) Source(t testing.TB) migration.Source {
	var fs = make([]string, 0, len(mm))

	for f := range mm {
		fs = append(fs, f)
	}

	return migration.NewStaticSource(
		fs,
		migration.StaticFetcher(mm.fetch),
		logtest.WrapTestingLogger(t),
	)
}

func (mm MigrationMap) Migrator(t testing.TB, db sql.DB, opts ...migration.Option) migration.Migrator {
	return migration.NewMigrator(db, mm.Source(t), opts...)
}
