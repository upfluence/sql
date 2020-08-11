package reader

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/upfluence/log"
	"github.com/upfluence/sql"
	"github.com/upfluence/sql/sqltest"
	"github.com/upfluence/sql/x/migration"
	"github.com/upfluence/sql/x/sqlbuilder"
)

func buildMigrator(ms map[string]string) func(sql.DB) migration.Migrator {
	return func(db sql.DB) migration.Migrator {
		var fs []string

		for f := range ms {
			fs = append(fs, f)
		}

		return migration.NewMigrator(
			db,
			migration.NewStaticSource(
				fs,
				migration.StaticFetcher(
					func(n string) ([]byte, error) {
						m, ok := ms[n]

						if !ok {
							return nil, migration.ErrNotExist
						}

						return []byte(m), nil
					},
				),
				log.NewLogger(),
			),
		)
	}
}

func assertReader(t *testing.T, r Reader, ids []int64) {
	cur, err := r.Read(
		context.Background(),
		[]sqlbuilder.Marker{sqlbuilder.Column("x")},
	)

	assert.NoError(t, err)

	var vs []int64

	err = sqlbuilder.ScrollCursor(cur, func(sc sqlbuilder.Scanner) error {
		var x int64

		if err := sc.Scan(map[string]interface{}{"x": &x}); err != nil {
			return err
		}

		vs = append(vs, x)

		return nil
	})

	assert.NoError(t, err)
	assert.Equal(t, ids, vs)
}

func TestReader(t *testing.T) {
	sqltest.NewTestCase(
		sqltest.WithMigratorFunc(
			buildMigrator(
				map[string]string{
					"1_initial.up.sqlite3":  "CREATE TABLE foo (x INTEGER PRIMARY KEY AUTOINCREMENT, y TEXT, z TEXT)",
					"1_initial.up.postgres": "CREATE TABLE foo (x SERIAL PRIMARY KEY, y TEXT, z TEXT)",
					"1_initial.down.sql":    "DROP TABLE foo",
				},
			),
		),
	).Run(t, func(t *testing.T, db sql.DB) {
		ctx := context.Background()
		rr := RootReader(db, "foo")

		_, err := db.Exec(ctx, "INSERT INTO foo(y, z) VALUES ($1, $2)", "foo", "buz")
		assert.NoError(t, err)

		_, err = db.Exec(ctx, "INSERT INTO foo(y, z) VALUES ($1, $2)", "biz", "buz")
		assert.NoError(t, err)

		assertReader(t, rr, []int64{1, 2})

		assertReader(
			t,
			rr.WithPredicateClauses(
				sqlbuilder.StaticEq(sqlbuilder.Column("y"), "foo"),
			),
			[]int64{1},
		)

		zr := rr.WithPredicateClauses(
			sqlbuilder.StaticEq(sqlbuilder.Column("z"), "buz"),
		)

		assertReader(t, zr, []int64{1, 2})

		or := zr.WithOrdering(
			sqlbuilder.OrderByClause{
				Field:     sqlbuilder.Column("x"),
				Direction: sqlbuilder.Desc,
			},
		)

		assertReader(t, or, []int64{2, 1})

		pr1 := zr.WithPagination(Pagination{Limit: 1, Offset: 1})
		assertReader(t, pr1, []int64{2})

		pr0 := pr1.WithPagination(Pagination{Limit: 1, Offset: 0})
		assertReader(t, pr0, []int64{1})

		pr2 := pr1.WithPagination(Pagination{Limit: 1, Offset: 2})
		assertReader(t, pr2, nil)
	})
}
