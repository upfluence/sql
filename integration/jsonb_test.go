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

const jsonbAddressKey = "address"

func jsonbMigrator(db sql.DB) migration.Migrator {
	up := "CREATE TABLE jsonb_rows(id INTEGER PRIMARY KEY, tags TEXT, data TEXT)"

	if db.Driver() == postgresDriver {
		up = "CREATE TABLE jsonb_rows(id INTEGER PRIMARY KEY, tags JSONB, data JSONB)"
	}

	return migration.NewMigrator(
		db,
		staticSource{up: up, down: "DROP TABLE jsonb_rows"},
	)
}

func TestJSONBPredicateClause(t *testing.T) {
	sqltest.NewTestCase(
		sqltest.WithMigratorFunc(jsonbMigrator),
	).Run(t, func(t *testing.T, db sql.DB) {
		ctx := context.Background()

		for _, row := range []struct {
			id         int64
			tags, data any
		}{
			{
				id:   1,
				tags: `["a", "b"]`,
				data: `{"status": "active", "tier": 2, "address": {"country": "FR"}, "labels": ["x", "y"]}`,
			},
			{
				id:   2,
				tags: `[1, 2.5]`,
				data: `{"status": "inactive", "tier": 2.0, "address": {"country": "US"}}`,
			},
			{
				id:   3,
				tags: `["1", true]`,
				data: `{"status": "active", "tier": "2", "labels": "x"}`,
			},
			{id: 4, tags: `{"a": "a"}`, data: `["status"]`},
			{id: 5, tags: `[]`, data: `{}`},
			{id: 6},
			{
				id:   7,
				tags: `[["a"]]`,
				data: `{"address": {"country": "FR", "city": "Paris"}, "labels": ["y", "x", "z"], "flags": true}`,
			},
		} {
			_, err := db.Exec(
				ctx,
				"INSERT INTO jsonb_rows(id, tags, data) VALUES ($1, $2, $3)",
				row.id,
				row.tags,
				row.data,
			)

			require.NoError(t, err)
		}

		var (
			tags = sqlbuilder.Column("tags")
			data = sqlbuilder.Column("data")
		)

		for _, tt := range []struct {
			name       string
			haveClause sqlbuilder.PredicateClause
			want       []int64
		}{
			{
				name:       "contains any string",
				haveClause: sqlbuilder.StaticJSONBArrayContainsAny(tags, []string{"a"}),
				want:       []int64{1},
			},
			{
				name:       "contains any number",
				haveClause: sqlbuilder.StaticJSONBArrayContainsAny(tags, []int{1}),
				want:       []int64{2},
			},
			{
				name:       "contains any mixed",
				haveClause: sqlbuilder.StaticJSONBArrayContainsAny(tags, []any{2.5, "b"}),
				want:       []int64{1, 2},
			},
			{
				name:       "contains any of nothing",
				haveClause: sqlbuilder.StaticJSONBArrayContainsAny(tags, []string{}),
			},
			{
				name:       "contains all",
				haveClause: sqlbuilder.StaticJSONBArrayContainsAll(tags, []string{"b", "a"}),
				want:       []int64{1},
			},
			{
				name:       "contains all duplicated",
				haveClause: sqlbuilder.StaticJSONBArrayContainsAll(tags, []string{"a", "a"}),
				want:       []int64{1},
			},
			{
				name:       "contains all numbers",
				haveClause: sqlbuilder.StaticJSONBArrayContainsAll(tags, []float64{1, 2.5}),
				want:       []int64{2},
			},
			{
				name:       "contains all numeric string",
				haveClause: sqlbuilder.StaticJSONBArrayContainsAll(tags, []string{"1"}),
				want:       []int64{3},
			},
			{
				name:       "contains all of nothing",
				haveClause: sqlbuilder.StaticJSONBArrayContainsAll(tags, []string{}),
				want:       []int64{1, 2, 3, 5, 7},
			},
			{
				name: "object contains string",
				haveClause: sqlbuilder.StaticJSONBObjectContains(
					data,
					map[string]any{"status": "active"},
				),
				want: []int64{1, 3},
			},
			{
				name: "object contains number",
				haveClause: sqlbuilder.StaticJSONBObjectContains(
					data,
					map[string]any{"tier": 2},
				),
				want: []int64{1, 2},
			},
			{
				name: "object contains nested object",
				haveClause: sqlbuilder.StaticJSONBObjectContains(
					data,
					map[string]any{jsonbAddressKey: map[string]any{"country": "FR"}},
				),
				want: []int64{1, 7},
			},
			{
				name: "object contains empty object",
				haveClause: sqlbuilder.StaticJSONBObjectContains(
					data,
					map[string]any{jsonbAddressKey: map[string]any{}},
				),
				want: []int64{1, 2, 7},
			},
			{
				name: "object contains array",
				haveClause: sqlbuilder.StaticJSONBObjectContains(
					data,
					map[string]any{"labels": []string{"x"}},
				),
				want: []int64{1, 7},
			},
			{
				name:       "object contains nothing",
				haveClause: sqlbuilder.StaticJSONBObjectContains(data, map[string]any{}),
				want:       []int64{1, 2, 3, 5, 7},
			},
			{
				name: "object contains many",
				haveClause: sqlbuilder.StaticJSONBObjectContains(
					data,
					map[string]any{
						"status":        "active",
						jsonbAddressKey: map[string]any{"country": "FR"},
						"labels":        []string{"y", "x"},
					},
				),
				want: []int64{1},
			},
		} {
			t.Run(tt.name, func(t *testing.T) {
				got, err := selectJSONBRowIDs(ctx, db, tt.haveClause)

				require.NoError(t, err)
				assert.Equal(t, tt.want, got)
			})
		}

		t.Run("in a transaction", func(t *testing.T) {
			var got []int64

			err := sql.ExecuteTx(
				ctx,
				db,
				sql.TxOptions{},
				func(q sql.Queryer) error {
					var err error

					got, err = selectJSONBRowIDs(
						ctx,
						q,
						sqlbuilder.StaticJSONBArrayContainsAll(tags, []string{"a"}),
					)

					return err
				},
			)

			require.NoError(t, err)
			assert.Equal(t, []int64{1}, got)
		})
	})
}

func selectJSONBRowIDs(ctx context.Context, q sql.Queryer, pc sqlbuilder.PredicateClause) ([]int64, error) {
	qb := sqlbuilder.QueryBuilder{Queryer: q}

	cur, err := qb.PrepareSelect(
		sqlbuilder.SelectStatement{
			Table:          "jsonb_rows",
			SelectClauses:  []sqlbuilder.Marker{sqlbuilder.Column("id")},
			WhereClause:    pc,
			OrderByClauses: []sqlbuilder.OrderByClause{{Field: sqlbuilder.Column("id")}},
		},
	).Query(ctx, nil)

	if err != nil {
		return nil, err
	}

	var ids []int64

	err = sqlbuilder.ScrollCursor(
		cur,
		func(sc sqlbuilder.Scanner) error {
			var id int64

			if err := sc.Scan(map[string]any{"id": &id}); err != nil {
				return err
			}

			ids = append(ids, id)

			return nil
		},
	)

	return ids, err
}
