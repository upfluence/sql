package integration

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/upfluence/sql"
	"github.com/upfluence/sql/sqltest"
	"github.com/upfluence/sql/x/migration"
)

func TestConsistencyOption(t *testing.T) {
	sqltest.NewTestCase(
		sqltest.WithMigratorFunc(func(db sql.DB) migration.Migrator {
			return migration.NewMigrator(
				db,
				staticSource{
					up:   "CREATE TABLE foo(fiz TEXT)",
					down: "DROP TABLE foo",
				},
			)
		}),
	).Run(t, func(t *testing.T, db sql.DB) {
		var ctx = context.Background()

		_, err := db.Exec(ctx, "INSERT INTO foo (fiz) VALUES ($1)", "foobar", sql.StronglyConsistent)
		assert.NoError(t, err)

		cur, err := db.Query(ctx, "SELECT fiz FROM foo", sql.StronglyConsistent)
		assert.NoError(t, err)

		var res []string

		for cur.Next() {
			var fiz string

			assert.Nil(t, cur.Scan(&fiz))

			res = append(res, fiz)
		}

		assert.NoError(t, cur.Close())
		assert.Equal(t, []string{"foobar"}, res)
	})
}
