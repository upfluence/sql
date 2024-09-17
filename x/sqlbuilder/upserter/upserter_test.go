package upserter

import (
	"context"
	"database/sql/driver"
	"testing"

	_ "github.com/mattn/go-sqlite3"
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

func assertResultAffected(t *testing.T, res sql.Result, nn int64) {
	t.Helper()
	n, err := res.RowsAffected()

	if err != nil {
		t.Errorf("RowsAffected() = (_, %v) [ want nil ]", err)
	}

	if n != nn {
		t.Errorf("RowsAffected() = (%d, nil) [ want (%d, nil) ]", n, nn)
	}
}

func assertResultInsertedID(t *testing.T, res sql.Result, nn int64) {
	t.Helper()
	n, err := res.LastInsertId()

	if err != nil {
		t.Errorf("LastInsertId() = (_, %v) [ want nil ]", err)
	}

	if n != nn {
		t.Errorf("LastInsertId() = (%d, nil) [ want (%d, nil) ]", n, nn)
	}
}

func TestUpserterRegular(t *testing.T) {
	sqltest.NewTestCase(
		sqltest.WithMigratorFunc(
			buildMigrator(
				map[string]string{
					"1_initial.up.sql":   "CREATE TABLE foo (x TEXT, y TEXT, z TEXT)",
					"1_initial.down.sql": "DROP TABLE foo",
					"2_initial.up.sql":   "CREATE UNIQUE INDEX foo_pk ON foo(x)",
					"2_initial.down.sql": "DROP INDEX foo_pk",
				},
			),
		),
	).Run(t, func(t *testing.T, db sql.DB) {
		ctx := context.Background()
		u := Upserter{DB: db}

		for qc, result := range map[bool]int64{false: 0, true: 1} {
			_, err := db.Exec(ctx, "DELETE FROM foo")
			assert.NoError(t, err)

			e := u.PrepareUpsert(
				UpsertStatement{
					Table:       "foo",
					QueryValues: []sqlbuilder.Marker{sqlbuilder.Column("x")},
					SetValues: []sqlbuilder.Marker{
						sqlbuilder.Column("y"),
						sqlbuilder.Column("z"),
					},
					QueryConstrained: qc,
				},
			)

			res, err := e.Exec(
				ctx,
				map[string]interface{}{"x": "foo", "y": stringOverloaded("bar"), "z": "buz"},
			)

			if err != nil {
				t.Fatalf("Exec() = %v [ want nil ]", err)
			}

			assertResultAffected(t, res, 1)

			res, err = e.Exec(
				ctx,
				map[string]interface{}{"x": "foo", "y": stringOverloaded("bar"), "z": "buz"},
			)

			if err != nil {
				t.Fatalf("Exec() = %v [ want nil ]", err)
			}

			assertResultAffected(t, res, result)

			res, err = e.Exec(
				ctx,
				map[string]interface{}{"x": "foo", "y": "barz", "z": "buz"},
			)

			if err != nil {
				t.Fatalf("Exec() = %v [ want nil ]", err)
			}

			assertResultAffected(t, res, 1)
		}
	})
}

func TestUpserterReturning(t *testing.T) {
	sqltest.NewTestCase(
		sqltest.WithMigratorFunc(
			buildMigrator(
				map[string]string{
					"1_initial.up.sqlite3":  "CREATE TABLE foo (x INTEGER PRIMARY KEY AUTOINCREMENT, y TEXT, z TEXT)",
					"1_initial.up.postgres": "CREATE TABLE foo (x SERIAL PRIMARY KEY, y TEXT, z TEXT)",
					"1_initial.down.sql":    "DROP TABLE foo",
					"2_initial.up.sql":      "CREATE UNIQUE INDEX foo_pk ON foo(y)",
					"2_initial.down.sql":    "DROP INDEX foo_pk",
				},
			),
		),
	).Run(t, func(t *testing.T, db sql.DB) {
		ctx := context.Background()
		u := Upserter{DB: db}
		e := u.PrepareUpsert(
			UpsertStatement{
				Table:       "foo",
				QueryValues: []sqlbuilder.Marker{sqlbuilder.Column("y")},
				SetValues:   []sqlbuilder.Marker{sqlbuilder.Column("z")},
				Returning:   &sql.Returning{Field: "x"},
			},
		)

		res, err := e.Exec(
			ctx,
			map[string]interface{}{"y": "bar", "z": "buz"},
		)

		if err != nil {
			t.Fatalf("Exec() = %v [ want nil ]", err)
		}

		assertResultAffected(t, res, 1)
		assertResultInsertedID(t, res, 1)

		res, err = e.Exec(
			ctx,
			map[string]interface{}{"y": "bar", "z": "buz"},
		)

		if err != nil {
			t.Fatalf("Exec() = %v [ want nil ]", err)
		}

		assertResultAffected(t, res, 0)
		assertResultInsertedID(t, res, 1)

		res, err = e.Exec(
			ctx,
			map[string]interface{}{"y": "bar", "z": "biz"},
		)

		if err != nil {
			t.Fatalf("Exec() = %v [ want nil ]", err)
		}

		assertResultAffected(t, res, 1)
		assertResultInsertedID(t, res, 1)

		res, err = e.Exec(
			ctx,
			map[string]interface{}{"y": "barz", "z": "buz"},
		)

		if err != nil {
			t.Fatalf("Exec() = %v [ want nil ]", err)
		}

		assertResultAffected(t, res, 1)
		assertResultInsertedID(t, res, 2)
	})
}

func TestUpserterInsertValue(t *testing.T) {
	sqltest.NewTestCase(
		sqltest.WithMigratorFunc(
			buildMigrator(
				map[string]string{
					"1_initial.up.sql":   "CREATE TABLE foo (x TEXT, y TEXT, z TEXT)",
					"1_initial.down.sql": "DROP TABLE foo",
					"2_initial.up.sql":   "CREATE UNIQUE INDEX foo_pk ON foo(x)",
					"2_initial.down.sql": "DROP INDEX foo_pk",
				},
			),
		),
	).Run(t, func(t *testing.T, db sql.DB) {
		ctx := context.Background()
		u := Upserter{DB: db}

		for _, qc := range []bool{false, true} {
			_, err := db.Exec(ctx, "DELETE FROM foo")
			assert.NoError(t, err)

			e := u.PrepareUpsert(
				UpsertStatement{
					Table:            "foo",
					QueryValues:      []sqlbuilder.Marker{sqlbuilder.Column("x")},
					SetValues:        []sqlbuilder.Marker{sqlbuilder.Column("z")},
					InsertValues:     []sqlbuilder.Marker{sqlbuilder.Column("y")},
					QueryConstrained: qc,
				},
			)

			_, err = e.Exec(
				ctx,
				map[string]interface{}{"x": "foo", "y": "bar", "z": "buz"},
			)

			if err != nil {
				t.Fatalf("Exec() = %v [ want nil ]", err)
			}

			_, err = e.Exec(
				ctx,
				map[string]interface{}{"x": "foo", "y": "far", "z": "fuz"},
			)

			if err != nil {
				t.Fatalf("Exec() = %v [ want nil ]", err)
			}

			var y, z string

			if err := db.QueryRow(ctx, "SELECT y, z FROM foo WHERE x = $1", "foo").Scan(
				&y,
				&z,
			); err != nil {
				t.Fatalf("QueryRow() = _, %v [ want nil ]", err)
			}

			if y != "bar" {
				t.Errorf("y = %q  [ want \"bar\" ]", y)
			}

			if z != "fuz" {
				t.Errorf("z = %q  [ want \"fuz\" ]", z)
			}
		}
	})
}

func TestUpserterUpdateOnly(t *testing.T) {
	sqltest.NewTestCase(
		sqltest.WithMigratorFunc(
			buildMigrator(
				map[string]string{
					"1_initial.up.sql":   "CREATE TABLE foo (x TEXT, y TEXT, z TEXT)",
					"1_initial.down.sql": "DROP TABLE foo",
				},
			),
		),
	).Run(t, func(t *testing.T, db sql.DB) {
		ctx := context.Background()
		u := Upserter{DB: db}

		for _, qc := range []bool{false, true} {
			_, err := db.Exec(ctx, "DELETE FROM foo")
			assert.NoError(t, err)

			e := u.PrepareUpsert(
				UpsertStatement{
					Table:            "foo",
					QueryValues:      []sqlbuilder.Marker{sqlbuilder.Column("x")},
					SetValues:        []sqlbuilder.Marker{sqlbuilder.Column("z")},
					InsertValues:     []sqlbuilder.Marker{sqlbuilder.Column("y")},
					Mode:             Update,
					QueryConstrained: qc,
				},
			)

			res, err := e.Exec(
				ctx,
				map[string]interface{}{"x": "foo", "y": "bar", "z": "buz"},
			)

			if err != nil {
				t.Fatalf("Exec() = %v [ want nil ]", err)
			}

			assertResultAffected(t, res, 0)

			if err := db.QueryRow(ctx, "SELECT y, z FROM foo WHERE x = $1", "foo").Scan(
				nil,
				nil,
			); err != sql.ErrNoRows {
				t.Fatalf("QueryRow() = _, %v [ want sql.ErrNoRows ]", err)
			}
		}
	})
}

func TestUpserterInsertOnly(t *testing.T) {
	sqltest.NewTestCase(
		sqltest.WithMigratorFunc(
			buildMigrator(
				map[string]string{
					"1_initial.up.sql":   "CREATE TABLE foo (x TEXT, y TEXT, z TEXT)",
					"1_initial.down.sql": "DROP TABLE foo",
					"2_initial.up.sql":   "CREATE UNIQUE INDEX foo_pk ON foo(x)",
					"2_initial.down.sql": "DROP INDEX foo_pk",
				},
			),
		),
	).Run(t, func(t *testing.T, db sql.DB) {
		ctx := context.Background()
		u := Upserter{DB: db}

		for _, qc := range []bool{false, true} {
			_, err := db.Exec(ctx, "DELETE FROM foo")
			assert.NoError(t, err)
			e := u.PrepareUpsert(
				UpsertStatement{
					Table:            "foo",
					QueryValues:      []sqlbuilder.Marker{sqlbuilder.Column("x")},
					SetValues:        []sqlbuilder.Marker{sqlbuilder.Column("z")},
					InsertValues:     []sqlbuilder.Marker{sqlbuilder.Column("y")},
					Mode:             Insert,
					QueryConstrained: qc,
				},
			)

			res, err := e.Exec(
				ctx,
				map[string]interface{}{"x": "foo", "y": "bar", "z": "buz"},
			)

			if err != nil {
				t.Fatalf("Exec() = %v [ want nil ]", err)
			}

			assertResultAffected(t, res, 1)

			res, err = e.Exec(
				ctx,
				map[string]interface{}{"x": "foo", "y": "far", "z": "fuz"},
			)

			if err != nil {
				t.Fatalf("Exec() = %v [ want nil ]", err)
			}

			assertResultAffected(t, res, 0)

			var y, z string

			if err := db.QueryRow(ctx, "SELECT y, z FROM foo WHERE x = $1", "foo").Scan(
				&y,
				&z,
			); err != nil {
				t.Fatalf("QueryRow() = _, %v [ want nil ]", err)
			}

			if y != "bar" {
				t.Errorf("y = %q  [ want \"bar\" ]", y)
			}

			if z != "buz" {
				t.Errorf("z = %q  [ want \"buz\" ]", z)
			}
		}
	})
}

func TestUpserterOnlyQueryValues(t *testing.T) {
	sqltest.NewTestCase(
		sqltest.WithMigratorFunc(
			buildMigrator(
				map[string]string{
					"1_initial.up.sql":   "CREATE TABLE foo (x TEXT)",
					"1_initial.down.sql": "DROP TABLE foo",
					"2_initial.up.sql":   "CREATE UNIQUE INDEX foo_pk ON foo(x)",
					"2_initial.down.sql": "DROP INDEX foo_pk",
				},
			),
		),
	).Run(t, func(t *testing.T, db sql.DB) {
		ctx := context.Background()
		u := Upserter{DB: db}
		e := u.PrepareUpsert(
			UpsertStatement{
				Table:       "foo",
				QueryValues: []sqlbuilder.Marker{sqlbuilder.Column("x")},
			},
		)

		res, err := e.Exec(
			ctx,
			map[string]interface{}{"x": "foo"},
		)

		if err != nil {
			t.Fatalf("Exec() = %v [ want nil ]", err)
		}

		assertResultAffected(t, res, 1)

		res, err = e.Exec(
			ctx,
			map[string]interface{}{"x": "foo"},
		)

		if err != nil {
			t.Fatalf("Exec() = %v [ want nil ]", err)
		}

		assertResultAffected(t, res, 0)
	})
}

type stringOverloaded string

func (s stringOverloaded) Value() (driver.Value, error) { return []byte(s), nil }

func TestInTxUpserterPristine(t *testing.T) {
	sqltest.NewTestCase(
		sqltest.WithMigratorFunc(
			buildMigrator(
				map[string]string{
					"1_initial.up.sql":   "CREATE TABLE foo (x TEXT)",
					"1_initial.down.sql": "DROP TABLE foo",
					"2_initial.up.sql":   "CREATE UNIQUE INDEX foo_pk ON foo(x)",
					"2_initial.down.sql": "DROP INDEX foo_pk",
				},
			),
		),
	).Run(t, func(t *testing.T, db sql.DB) {
		ctx := context.Background()
		e := InTxUpserter(
			db,
			Statement{
				Table:       "foo",
				QueryValues: []sqlbuilder.Marker{sqlbuilder.Column("x")},
			},
		)

		res, err := e.Exec(
			ctx,
			map[string]interface{}{"x": "foo"},
		)

		if err != nil {
			t.Fatalf("Exec() = %v [ want nil ]", err)
		}

		assertResultAffected(t, res, 1)

		res, err = e.Exec(
			ctx,
			map[string]interface{}{"x": "foo"},
		)

		if err != nil {
			t.Fatalf("Exec() = %v [ want nil ]", err)
		}

		assertResultAffected(t, res, 0)
	})
}
