package upserter

import (
	"context"
	"database/sql/driver"
	"testing"

	_ "github.com/mattn/go-sqlite3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/upfluence/errors"
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
				map[string]any{"x": "foo", "y": nilDBValue{}, "z": "buz"},
			)

			if err != nil {
				t.Fatalf("Exec() = %v [ want nil ]", err)
			}

			assertResultAffected(t, res, 1)

			res, err = e.Exec(
				ctx,
				map[string]any{"x": "foo", "y": stringDBValue("bar"), "z": "buz"},
			)

			if err != nil {
				t.Fatalf("Exec() = %v [ want nil ]", err)
			}

			assertResultAffected(t, res, 1)

			res, err = e.Exec(
				ctx,
				map[string]any{"x": "foo", "y": stringDBValue("bar"), "z": "buz"},
			)

			if err != nil {
				t.Fatalf("Exec() = %v [ want nil ]", err)
			}

			assertResultAffected(t, res, result)

			res, err = e.Exec(
				ctx,
				map[string]any{"x": "foo", "y": "barz", "z": "buz"},
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
			map[string]any{"y": "bar", "z": "buz"},
		)

		if err != nil {
			t.Fatalf("Exec() = %v [ want nil ]", err)
		}

		assertResultAffected(t, res, 1)
		assertResultInsertedID(t, res, 1)

		res, err = e.Exec(
			ctx,
			map[string]any{"y": "bar", "z": "buz"},
		)

		if err != nil {
			t.Fatalf("Exec() = %v [ want nil ]", err)
		}

		assertResultAffected(t, res, 0)
		assertResultInsertedID(t, res, 1)

		res, err = e.Exec(
			ctx,
			map[string]any{"y": "bar", "z": "biz"},
		)

		if err != nil {
			t.Fatalf("Exec() = %v [ want nil ]", err)
		}

		assertResultAffected(t, res, 1)
		assertResultInsertedID(t, res, 1)

		res, err = e.Exec(
			ctx,
			map[string]any{"y": "barz", "z": "buz"},
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
				map[string]any{"x": "foo", "y": "bar", "z": "buz"},
			)

			if err != nil {
				t.Fatalf("Exec() = %v [ want nil ]", err)
			}

			_, err = e.Exec(
				ctx,
				map[string]any{"x": "foo", "y": "far", "z": "fuz"},
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

func TestUpserterPredicateClause(t *testing.T) {
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

		for _, tt := range []struct {
			name string

			haveQueryConstrained bool
			haveExisting         bool
			havePredicate        sqlbuilder.PredicateClause

			wantAffected int64
			wantZ        string
		}{
			{
				name:          "no existing row, failing predicate still inserts",
				havePredicate: sqlbuilder.StaticEq(sqlbuilder.ColumnWithTable("y", "foo", "y"), "nope"),
				wantAffected:  1,
				wantZ:         "new",
			},
			{
				name:                 "no existing row, failing predicate still inserts, query constrained",
				haveQueryConstrained: true,
				havePredicate:        sqlbuilder.StaticEq(sqlbuilder.ColumnWithTable("y", "foo", "y"), "nope"),
				wantAffected:         1,
				wantZ:                "new",
			},
			{
				name:          "existing row, valid predicate updates",
				haveExisting:  true,
				havePredicate: sqlbuilder.StaticEq(sqlbuilder.ColumnWithTable("y", "foo", "y"), "bar"),
				wantAffected:  1,
				wantZ:         "new",
			},
			{
				name:                 "existing row, valid predicate updates, query constrained",
				haveQueryConstrained: true,
				haveExisting:         true,
				havePredicate:        sqlbuilder.StaticEq(sqlbuilder.ColumnWithTable("y", "foo", "y"), "bar"),
				wantAffected:         1,
				wantZ:                "new",
			},
			{
				name:          "existing row, failing predicate does not update",
				haveExisting:  true,
				havePredicate: sqlbuilder.StaticEq(sqlbuilder.ColumnWithTable("y", "foo", "y"), "nope"),
				wantAffected:  0,
				wantZ:         "old",
			},
			{
				name:                 "existing row, failing predicate does not update, query constrained",
				haveQueryConstrained: true,
				haveExisting:         true,
				havePredicate:        sqlbuilder.StaticEq(sqlbuilder.ColumnWithTable("y", "foo", "y"), "nope"),
				wantAffected:         0,
				wantZ:                "old",
			},
		} {
			t.Run(tt.name, func(t *testing.T) {
				_, err := db.Exec(ctx, "DELETE FROM foo")
				require.NoError(t, err)

				if tt.haveExisting {
					_, err := db.Exec(
						ctx,
						"INSERT INTO foo(x, y, z) VALUES ($1, $2, $3)",
						"foo",
						"bar",
						"old",
					)
					require.NoError(t, err)
				}

				res, err := u.PrepareUpsert(
					UpsertStatement{
						Table:            "foo",
						QueryValues:      []sqlbuilder.Marker{sqlbuilder.Column("x")},
						SetValues:        []sqlbuilder.Marker{sqlbuilder.Column("z")},
						InsertValues:     []sqlbuilder.Marker{sqlbuilder.Column("y")},
						PredicateClause:  tt.havePredicate,
						QueryConstrained: tt.haveQueryConstrained,
					},
				).Exec(
					ctx,
					map[string]any{"x": "foo", "y": "bar", "z": "new"},
				)

				require.NoError(t, err)
				assertResultAffected(t, res, tt.wantAffected)

				var z string

				require.NoError(
					t,
					db.QueryRow(ctx, "SELECT z FROM foo WHERE x = $1", "foo").Scan(&z),
				)
				assert.Equal(t, tt.wantZ, z)
			})
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
				map[string]any{"x": "foo", "y": "bar", "z": "buz"},
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
				map[string]any{"x": "foo", "y": "bar", "z": "buz"},
			)

			if err != nil {
				t.Fatalf("Exec() = %v [ want nil ]", err)
			}

			assertResultAffected(t, res, 1)

			res, err = e.Exec(
				ctx,
				map[string]any{"x": "foo", "y": "far", "z": "fuz"},
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
			map[string]any{"x": "foo"},
		)

		if err != nil {
			t.Fatalf("Exec() = %v [ want nil ]", err)
		}

		assertResultAffected(t, res, 1)

		res, err = e.Exec(
			ctx,
			map[string]any{"x": "foo"},
		)

		if err != nil {
			t.Fatalf("Exec() = %v [ want nil ]", err)
		}

		assertResultAffected(t, res, 0)
	})
}

type nilDBValue struct{}

func (nilDBValue) Value() (driver.Value, error) { return nil, nil }

type stringDBValue string

func (s stringDBValue) Value() (driver.Value, error) { return []byte(s), nil }

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
			map[string]any{"x": "foo"},
		)

		if err != nil {
			t.Fatalf("Exec() = %v [ want nil ]", err)
		}

		assertResultAffected(t, res, 1)

		res, err = e.Exec(
			ctx,
			map[string]any{"x": "foo"},
		)

		if err != nil {
			t.Fatalf("Exec() = %v [ want nil ]", err)
		}

		assertResultAffected(t, res, 0)
	})
}

func readFoo(t *testing.T, db sql.DB) map[string][2]string {
	t.Helper()

	cur, err := db.Query(context.Background(), "SELECT x, y, z FROM foo")

	require.NoError(t, err)

	res := make(map[string][2]string)

	require.NoError(
		t,
		sql.ScrollCursor(
			cur,
			func(sc sql.Scanner) error {
				var x, y, z string

				if err := sc.Scan(&x, &y, &z); err != nil {
					return errors.WithStack(err)
				}

				res[x] = [2]string{y, z}

				return nil
			},
		),
	)

	return res
}

func TestUpserterMultiExec(t *testing.T) {
	var (
		x = sqlbuilder.Column("x")
		y = sqlbuilder.Column("y")
		z = sqlbuilder.Column("z")
	)

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

		for _, tt := range []struct {
			name string

			haveSetup string
			haveStmt  Statement
			haveRows  [][]map[string]any

			wantAffected []int64
			wantRows     map[string][2]string
		}{
			{
				name: "upsert updates every row with its own values",
				haveStmt: Statement{
					Table:            "foo",
					QueryValues:      []sqlbuilder.Marker{x},
					SetValues:        []sqlbuilder.Marker{y},
					InsertValues:     []sqlbuilder.Marker{z},
					QueryConstrained: true,
				},
				haveRows: [][]map[string]any{
					{
						{"x": "a", "y": "1", "z": "1"},
						{"x": "b", "y": "2", "z": "2"},
					},
					{
						{"x": "b", "y": "3", "z": "9"},
						{"x": "c", "y": "4", "z": "4"},
					},
				},
				wantAffected: []int64{2, 2},
				wantRows: map[string][2]string{
					"a": {"1", "1"},
					"b": {"3", "2"},
					"c": {"4", "4"},
				},
			},
			{
				name: "upsert without set values only inserts",
				haveStmt: Statement{
					Table:            "foo",
					QueryValues:      []sqlbuilder.Marker{x},
					InsertValues:     []sqlbuilder.Marker{y, z},
					QueryConstrained: true,
				},
				haveRows: [][]map[string]any{
					{
						{"x": "a", "y": "1", "z": "1"},
						{"x": "b", "y": "2", "z": "2"},
					},
					{
						{"x": "b", "y": "3", "z": "3"},
						{"x": "c", "y": "4", "z": "4"},
					},
				},
				wantAffected: []int64{2, 1},
				wantRows: map[string][2]string{
					"a": {"1", "1"},
					"b": {"2", "2"},
					"c": {"4", "4"},
				},
			},
			{
				name: "without the key constraint upsert writes only what changes",
				haveStmt: Statement{
					Table:        "foo",
					QueryValues:  []sqlbuilder.Marker{x},
					SetValues:    []sqlbuilder.Marker{y},
					InsertValues: []sqlbuilder.Marker{z},
				},
				haveRows: [][]map[string]any{
					{
						{"x": "a", "y": "1", "z": "1"},
						{"x": "b", "y": "2", "z": "2"},
					},
					{
						{"x": "b", "y": "3", "z": "9"},
						{"x": "c", "y": "4", "z": "4"},
						{"x": "a", "y": "1", "z": "1"},
					},
				},
				wantAffected: []int64{2, 2},
				wantRows: map[string][2]string{
					"a": {"1", "1"},
					"b": {"3", "2"},
					"c": {"4", "4"},
				},
			},
			{
				name: "without the key constraint insert skips the existing rows",
				haveStmt: Statement{
					Table:       "foo",
					QueryValues: []sqlbuilder.Marker{x},
					SetValues:   []sqlbuilder.Marker{y, z},
					Mode:        Insert,
				},
				haveRows: [][]map[string]any{
					{
						{"x": "a", "y": "1", "z": "1"},
						{"x": "b", "y": "2", "z": "2"},
					},
					{
						{"x": "b", "y": "3", "z": "3"},
						{"x": "c", "y": "4", "z": "4"},
					},
				},
				wantAffected: []int64{2, 1},
				wantRows: map[string][2]string{
					"a": {"1", "1"},
					"b": {"2", "2"},
					"c": {"4", "4"},
				},
			},
			{
				name:      "without the key constraint update skips the missing rows",
				haveSetup: "INSERT INTO foo(x, y, z) VALUES ('a', '1', '1'), ('b', '2', '2')",
				haveStmt: Statement{
					Table:       "foo",
					QueryValues: []sqlbuilder.Marker{x},
					SetValues:   []sqlbuilder.Marker{y},
					Mode:        Update,
				},
				haveRows: [][]map[string]any{
					{
						{"x": "a", "y": "5"},
						{"x": "c", "y": "6"},
					},
				},
				wantAffected: []int64{1},
				wantRows: map[string][2]string{
					"a": {"5", "1"},
					"b": {"2", "2"},
				},
			},
			{
				name: "insert skips the existing rows",
				haveStmt: Statement{
					Table:            "foo",
					QueryValues:      []sqlbuilder.Marker{x},
					SetValues:        []sqlbuilder.Marker{y, z},
					Mode:             Insert,
					QueryConstrained: true,
				},
				haveRows: [][]map[string]any{
					{
						{"x": "a", "y": "1", "z": "1"},
						{"x": "b", "y": "2", "z": "2"},
					},
					{
						{"x": "b", "y": "3", "z": "3"},
						{"x": "c", "y": "4", "z": "4"},
					},
				},
				wantAffected: []int64{2, 1},
				wantRows: map[string][2]string{
					"a": {"1", "1"},
					"b": {"2", "2"},
					"c": {"4", "4"},
				},
			},
		} {
			t.Run(tt.name, func(t *testing.T) {
				_, err := db.Exec(ctx, "DELETE FROM foo")

				require.NoError(t, err)

				if tt.haveSetup != "" {
					_, err := db.Exec(ctx, tt.haveSetup)

					require.NoError(t, err)
				}

				me, ok := u.PrepareUpsert(tt.haveStmt).(MultiExecer)

				require.True(t, ok)

				for i, rows := range tt.haveRows {
					res, err := me.MultiExec(ctx, rows)

					require.NoError(t, err)
					assertResultAffected(t, res, tt.wantAffected[i])
				}

				assert.Equal(t, tt.wantRows, readFoo(t, db))
			})
		}
	})
}

func TestUpserterMultiExecNotSupported(t *testing.T) {
	var (
		u = Upserter{}

		x = []sqlbuilder.Marker{sqlbuilder.Column("x")}
		y = []sqlbuilder.Marker{sqlbuilder.Column("y")}
	)

	for _, tt := range []struct {
		name string

		haveStmt Statement
	}{
		{
			name: "update",
			haveStmt: Statement{
				Table:            "foo",
				QueryValues:      x,
				SetValues:        y,
				Mode:             Update,
				QueryConstrained: true,
			},
		},
		{
			name: "not query constrained with returning",
			haveStmt: Statement{
				Table:       "foo",
				QueryValues: x,
				SetValues:   y,
				Returning:   &sql.Returning{Field: "id"},
			},
		},
		{
			name: "upsert with returning",
			haveStmt: Statement{
				Table:            "foo",
				QueryValues:      x,
				SetValues:        y,
				Returning:        &sql.Returning{Field: "id"},
				QueryConstrained: true,
			},
		},
		{
			name: "upsert with predicate",
			haveStmt: Statement{
				Table:            "foo",
				QueryValues:      x,
				SetValues:        y,
				PredicateClause:  sqlbuilder.StaticEq(sqlbuilder.ColumnWithTable("y", "foo", "y"), "bar"),
				QueryConstrained: true,
			},
		},
		{
			name: "insert with returning",
			haveStmt: Statement{
				Table:            "foo",
				QueryValues:      x,
				SetValues:        y,
				Mode:             Insert,
				Returning:        &sql.Returning{Field: "id"},
				QueryConstrained: true,
			},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			_, ok := u.PrepareUpsert(tt.haveStmt).(MultiExecer)

			assert.False(t, ok)
		})
	}
}
