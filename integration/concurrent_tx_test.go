package integration

import (
	"context"
	"sync"
	"testing"

	_ "github.com/lib/pq"
	_ "github.com/mattn/go-sqlite3"
	"github.com/stretchr/testify/assert"

	"github.com/upfluence/sql"
	"github.com/upfluence/sql/sqltest"
	"github.com/upfluence/sql/x/migration"
)

func TestConcurrentTxQuery(t *testing.T) {
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
		var (
			ctx     = context.Background()
			tx, err = db.BeginTx(ctx, sql.TxOptions{})
			k       = 100

			wg sync.WaitGroup
		)

		assert.Nil(t, err)

		wg.Add(k)

		tx.Exec(ctx, "INSERT INTO foo(fiz) VALUES($1)", "bar")
		for i := 0; i < k; i++ {
			go func() {
				cur, err := tx.Query(ctx, "SELECT fiz FROM foo")

				assert.Nil(t, err)

				if err != nil {
					return
				}

				var res []string

				for cur.Next() {
					var fiz string

					assert.Nil(t, cur.Scan(&fiz))

					res = append(res, fiz)
				}

				assert.Nil(t, cur.Close())
				assert.Equal(t, res, []string{"bar"})
				wg.Done()
			}()
		}

		wg.Wait()
		assert.Nil(t, tx.Commit())
	})
}
