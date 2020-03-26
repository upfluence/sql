package upserter

import (
	"context"
	"testing"

	_ "github.com/mattn/go-sqlite3"
	"github.com/upfluence/log"

	"github.com/upfluence/sql"
	"github.com/upfluence/sql/sqltest"
	"github.com/upfluence/sql/x/migration"
	"github.com/upfluence/sql/x/sqlbuilder"
)

func buildMigrator(db sql.DB) migration.Migrator {
	return migration.NewMigrator(
		db,
		migration.NewStaticSource(
			[]string{"1_initial.up.sql", "1_initial.down.sql"},
			migration.StaticFetcher(
				func(n string) ([]byte, error) {
					switch n {
					case "1_initial.up.sql":
						return []byte("CREATE TABLE foo (x TEXT, y TEXT, z TEXT)"), nil
					case "1_initial.down.sql":
						return []byte("DROP TABLE foo"), nil
					default:
						return nil, migration.ErrNotExist
					}
				},
			),
			log.NewLogger(),
		),
	)
}

func assertResult(t *testing.T, res sql.Result, nn int64) {
	n, err := res.RowsAffected()

	if err != nil {
		t.Errorf("RowsAffected() = (_, %v) [ want nil ]", err)
	}

	if n != nn {
		t.Errorf("RowsAffected() = (%d, nil) [ want (%d, nil) ]", n, nn)
	}
}

func TestUpserter(t *testing.T) {
	sqltest.NewTestCase(
		sqltest.WithMigratorFunc(buildMigrator),
	).Run(t, func(t *testing.T, db sql.DB) {
		ctx := context.Background()
		u := Upserter{DB: db}
		e := u.PrepareUpsert(
			UpsertStatement{
				Table:       "foo",
				QueryValues: []sqlbuilder.Marker{sqlbuilder.Column("x")},
				SetValues: []sqlbuilder.Marker{
					sqlbuilder.Column("y"),
					sqlbuilder.Column("z"),
				},
			},
		)

		res, err := e.Exec(
			ctx,
			map[string]interface{}{"x": "foo", "y": "bar", "z": "buz"},
		)

		if err != nil {
			t.Fatalf("Exec() = %v [ want nil ]", err)
		}

		assertResult(t, res, 1)

		res, err = e.Exec(
			ctx,
			map[string]interface{}{"x": "foo", "y": "bar", "z": "buz"},
		)

		if err != nil {
			t.Fatalf("Exec() = %v [ want nil ]", err)
		}

		assertResult(t, res, 0)

		res, err = e.Exec(
			ctx,
			map[string]interface{}{"x": "foo", "y": "barz", "z": "buz"},
		)

		if err != nil {
			t.Fatalf("Exec() = %v [ want nil ]", err)
		}

		assertResult(t, res, 1)
	})
}
