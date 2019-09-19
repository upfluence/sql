package integration

import (
	"context"
	"io"
	"io/ioutil"
	"strings"
	"sync"
	"testing"

	_ "github.com/lib/pq"
	_ "github.com/mattn/go-sqlite3"
	"github.com/stretchr/testify/assert"

	"github.com/upfluence/sql"
	"github.com/upfluence/sql/sqltest"
	"github.com/upfluence/sql/x/migration"
)

type staticSource struct {
	up, down string
}

func (ss staticSource) ID() uint {
	return 1
}

func (ss staticSource) Up(migration.Driver) (io.ReadCloser, error) {
	return ioutil.NopCloser(strings.NewReader(ss.up)), nil
}

func (ss staticSource) Down(migration.Driver) (io.ReadCloser, error) {
	return ioutil.NopCloser(strings.NewReader(ss.down)), nil
}

func (ss staticSource) Get(_ context.Context, v uint) (migration.Migration, error) {
	if v != 1 {
		return nil, migration.ErrNotExist
	}

	return ss, nil
}

func (ss staticSource) First(context.Context) (migration.Migration, error) {
	return ss, nil
}

func (ss staticSource) Next(context.Context, uint) (bool, uint, error) {
	return false, 0, nil
}

func (ss staticSource) Prev(context.Context, uint) (bool, uint, error) {
	return false, 0, nil
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
			ctx     = context.Background()
			tx, err = db.BeginTx(ctx)
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
