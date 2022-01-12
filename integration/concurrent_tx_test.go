package integration

import (
	"context"
	"sync"
	"testing"
	"time"

	_ "github.com/lib/pq"
	_ "github.com/mattn/go-sqlite3"
	"github.com/stretchr/testify/assert"

	"github.com/upfluence/sql"
	"github.com/upfluence/sql/backend/postgres"
	"github.com/upfluence/sql/sqltest"
	"github.com/upfluence/sql/x/migration"
)

func assertFoo(t *testing.T, q sql.Queryer, want []string) {
	cur, err := q.Query(context.Background(), "SELECT fiz FROM foo")

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
	assert.Equal(t, want, res)
}

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
			ctx = context.Background()
			k   = 100

			wg sync.WaitGroup
		)

		if postgres.IsPostgresDB(db) {
			_, err := db.Exec(ctx, "TRUNCATE foo")
			assert.NoError(t, err)
		}

		tx, err := db.BeginTx(ctx, sql.TxOptions{})
		assert.NoError(t, err)

		wg.Add(k)

		tx.Exec(ctx, "INSERT INTO foo(fiz) VALUES($1)", "bar")
		for i := 0; i < k; i++ {
			go func() {
				assertFoo(t, tx, []string{"bar"})
				wg.Done()
			}()
		}

		wg.Wait()
		assert.Nil(t, tx.Commit())
	})
}

func TestRetryConcurrentTxQuery(t *testing.T) {
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
			ctx = context.Background()
			k   = 2

			wg sync.WaitGroup
		)

		if postgres.IsPostgresDB(db) {
			_, err := db.Exec(ctx, "TRUNCATE foo")
			assert.NoError(t, err)
		}

		wg.Add(k)

		for i := 0; i < k; i++ {
			i := i

			go func() {
				defer wg.Done()
				err := sql.ExecuteTx(
					ctx,
					db,
					sql.TxOptions{Isolation: sql.LevelSerializable},
					func(q sql.Queryer) error {
						cur, err := q.Query(ctx, "SELECT fiz FROM foo")

						assert.Nil(t, err)

						if err != nil {
							t.Log(err)
							return err
						}

						for cur.Next() {
							var fiz string

							assert.Nil(t, cur.Scan(&fiz))
						}

						assert.Nil(t, cur.Close())

						time.Sleep(time.Duration(i) * 100 * time.Millisecond)

						_, err = q.Exec(ctx, "INSERT INTO foo(fiz) VALUES($1)", "bar")

						if err != nil {
							t.Log(err)
						}

						return err
					},
				)

				assert.NoError(t, err)
			}()
		}

		wg.Wait()
		assertFoo(t, db, []string{"bar", "bar"})
	})
}
