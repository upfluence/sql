package integration

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/upfluence/sql"
	"github.com/upfluence/sql/sqltest"
	"github.com/upfluence/sql/x/migration"
	"github.com/upfluence/sql/x/sqlbuilder"
)

const (
	fizColumn      = "fiz"
	postgresDriver = "postgres"

	dropFooStmt = "DROP TABLE foo"
)

func fooWithIDMigrator(db sql.DB) migration.Migrator {
	up := "CREATE TABLE foo(id INTEGER PRIMARY KEY, fiz TEXT UNIQUE)"

	if db.Driver() == postgresDriver {
		up = "CREATE TABLE foo(id SERIAL PRIMARY KEY, fiz TEXT UNIQUE)"
	}

	return migration.NewMigrator(db, staticSource{up: up, down: dropFooStmt})
}

func TestMultiQueryReturning(t *testing.T) {
	sqltest.NewTestCase(
		sqltest.WithMigratorFunc(fooWithIDMigrator),
	).Run(t, func(t *testing.T, db sql.DB) {
		var (
			ctx       = context.Background()
			fizMarker = sqlbuilder.Column(fizColumn)
		)

		qb := sqlbuilder.QueryBuilder{Queryer: db}

		ie := qb.PrepareInsert(
			sqlbuilder.InsertStatement{
				Table:  "foo",
				Fields: []sqlbuilder.Marker{fizMarker},
				OnConfict: &sqlbuilder.OnConflictClause{
					Action: sqlbuilder.Nothing,
				},
				Returnings: []*sql.Returning{{Field: "id"}, {Field: fizColumn}},
			},
		)

		for _, tt := range []struct {
			name     string
			haveFizs []string
			wantFizs []string
		}{
			{
				name:     "multiple rows",
				haveFizs: []string{"a", "b", "c"},
				wantFizs: []string{"a", "b", "c"},
			},
			{
				name:     "single row",
				haveFizs: []string{"d"},
				wantFizs: []string{"d"},
			},
			{
				name:     "partially skipped rows",
				haveFizs: []string{"a", "e"},
				wantFizs: []string{"e"},
			},
			{
				name:     "all rows skipped",
				haveFizs: []string{"a", "b"},
			},
		} {
			t.Run(tt.name, func(t *testing.T) {
				vvs := make([]map[string]any, len(tt.haveFizs))

				for i, fiz := range tt.haveFizs {
					vvs[i] = map[string]any{fizMarker.Binding(): fiz}
				}

				cur, err := ie.MultiQuery(ctx, vvs, nil)

				require.NoError(t, err)

				var gotFizs []string

				require.NoError(
					t,
					sqlbuilder.ScrollCursor(
						cur,
						func(sc sqlbuilder.Scanner) error {
							var (
								id  int64
								fiz string
							)

							if err := sc.Scan(
								map[string]any{"id": &id, fizColumn: &fiz},
							); err != nil {
								return err
							}

							gotFizs = append(gotFizs, fiz)

							return nil
						},
					),
				)

				assert.Equal(t, tt.wantFizs, gotFizs)
			})
		}
	})
}

func TestMultiQueryConstraintError(t *testing.T) {
	sqltest.NewTestCase(
		sqltest.WithMigratorFunc(func(db sql.DB) migration.Migrator {
			return migration.NewMigrator(
				db,
				staticSource{
					up:   "CREATE TABLE foo(fiz TEXT UNIQUE)",
					down: dropFooStmt,
				},
			)
		}),
	).Run(t, func(t *testing.T, db sql.DB) {
		ctx := context.Background()
		qb := sqlbuilder.QueryBuilder{Queryer: db}

		cur, err := qb.PrepareInsert(
			sqlbuilder.InsertStatement{
				Table:      "foo",
				Fields:     []sqlbuilder.Marker{sqlbuilder.Column(fizColumn)},
				Returnings: []*sql.Returning{{Field: fizColumn}},
			},
		).MultiQuery(
			ctx,
			[]map[string]any{{fizColumn: "a"}, {fizColumn: "a"}},
			nil,
		)

		if err == nil {
			err = sqlbuilder.ScrollCursor(
				cur,
				func(sqlbuilder.Scanner) error { return nil },
			)
		}

		var cerr sql.ConstraintError

		require.ErrorAs(t, err, &cerr)
		assert.Equal(t, sql.Unique, cerr.Type)
	})
}
