package integration

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/upfluence/sql"
	"github.com/upfluence/sql/sqltest"
	"github.com/upfluence/sql/x/migration"
)

func TestCanceling(t *testing.T) {
	sqltest.NewTestCase().Run(t, func(t *testing.T, db sql.DB) {
		ctx, done := context.WithTimeout(context.Background(), 10*time.Millisecond)
		defer done()

		var stmt string

		switch d := db.Driver(); d {
		case "postgres":
			stmt = "pg_sleep(1)"
		default:
			t.Skipf("driver not handled: %q", d)
		}

		err := db.QueryRow(ctx, "SELECT "+stmt).Scan()

		assert.ErrorIs(t, err, context.Canceled)
	})
}

func TestCanceled(t *testing.T) {
	sqltest.NewTestCase().Run(t, func(t *testing.T, db sql.DB) {
		ctx, done := context.WithCancel(context.Background())
		done()

		err := db.QueryRow(ctx, "SELECT 1").Scan()

		assert.ErrorIs(t, err, context.Canceled)
	})
}

func TestConstraintPrimaryKeyError(t *testing.T) {
	sqltest.NewTestCase(
		sqltest.WithMigratorFunc(func(db sql.DB) migration.Migrator {
			return migration.NewMigrator(
				db,
				staticSource{
					up:   "CREATE TABLE foo(fiz TEXT PRIMARY KEY)",
					down: "DROP TABLE foo",
				},
			)
		}),
	).Run(t, func(t *testing.T, db sql.DB) {
		ctx := context.Background()

		_, err := db.Exec(ctx, "INSERT INTO foo(fiz) VALUES ($1)", "buz")
		assert.Nil(t, err)

		_, err = db.Exec(ctx, "INSERT INTO foo(fiz) VALUES ($1)", "buz")

		cerr, ok := err.(sql.ConstraintError)

		assert.True(t, ok)

		assert.Equal(t, sql.PrimaryKey, cerr.Type)
		assert.Equal(
			t,
			map[string]string{
				"sqlite3":  "fiz",
				"postgres": "foo_pkey",
			}[db.Driver()],
			cerr.Constraint,
		)

		_, err = db.Exec(ctx, "INSERT INTO foo(fiz) VALUES ($1)", "bar")
		assert.Nil(t, err)
	})
}

func TestConstraintNotNullError(t *testing.T) {
	sqltest.NewTestCase(
		sqltest.WithMigratorFunc(func(db sql.DB) migration.Migrator {
			return migration.NewMigrator(
				db,
				staticSource{
					up:   "CREATE TABLE foo(fiz TEXT NOT NULL)",
					down: "DROP TABLE foo",
				},
			)
		}),
	).Run(t, func(t *testing.T, db sql.DB) {
		ctx := context.Background()

		_, err := db.Exec(ctx, "INSERT INTO foo(fiz) VALUES ($1)", nil)

		cerr, ok := err.(sql.ConstraintError)

		assert.True(t, ok)
		assert.Equal(t, sql.NotNull, cerr.Type)
		assert.Equal(t, "fiz", cerr.Constraint)

		_, err = db.Exec(ctx, "INSERT INTO foo(fiz) VALUES ($1)", "bar")
		assert.Nil(t, err)
	})
}

func TestConstraintUniqueError(t *testing.T) {
	sqltest.NewTestCase(
		sqltest.WithMigratorFunc(func(db sql.DB) migration.Migrator {
			return migration.NewMigrator(
				db,
				staticSource{
					up:   "CREATE TABLE foo(fiz TEXT UNIQUE)",
					down: "DROP TABLE foo",
				},
			)
		}),
	).Run(t, func(t *testing.T, db sql.DB) {
		ctx := context.Background()

		_, err := db.Exec(ctx, "INSERT INTO foo(fiz) VALUES ($1)", "buz")
		assert.Nil(t, err)

		_, err = db.Exec(ctx, "INSERT INTO foo(fiz) VALUES ($1)", "buz")

		cerr, ok := err.(sql.ConstraintError)

		assert.True(t, ok)
		assert.Equal(t, sql.Unique, cerr.Type)
		assert.Equal(
			t,
			map[string]string{
				"sqlite3":  "fiz",
				"postgres": "foo_fiz_key",
			}[db.Driver()],
			cerr.Constraint,
		)

		_, err = db.Exec(ctx, "INSERT INTO foo(fiz) VALUES ($1)", "bar")
		assert.Nil(t, err)
	})
}
