package sqlbuilder

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/upfluence/errors/errtest"

	"github.com/upfluence/sql/backend/static"
)

const postgresDriver = "postgres"

func TestJSONBPredicateClause(t *testing.T) {
	var (
		tags = Column("tags")
		data = Column("data")
	)

	for _, tt := range []struct {
		name       string
		haveDriver string
		haveClause PredicateClause
		want       string
		wantArgs   []any
		wantErr    errtest.ErrorAssertion
	}{
		{
			name:       "postgres contains any",
			haveDriver: postgresDriver,
			haveClause: StaticJSONBArrayContainsAny(tags, []any{"a", 1}),
			want:       "(tags @> $1::jsonb OR tags @> $2::jsonb)",
			wantArgs:   []any{`["a"]`, `[1]`},
			wantErr:    errtest.NoError(),
		},
		{
			name:       "unknown driver contains any",
			haveClause: StaticJSONBArrayContainsAny(tags, []string{"z"}),
			want:       "(tags @> $1::jsonb)",
			wantArgs:   []any{`["z"]`},
			wantErr:    errtest.NoError(),
		},
		{
			name:       "postgres contains any of nothing",
			haveDriver: postgresDriver,
			haveClause: StaticJSONBArrayContainsAny(tags, []string{}),
			want:       "1=0",
			wantErr:    errtest.NoError(),
		},
		{
			name:       "postgres contains all",
			haveDriver: postgresDriver,
			haveClause: StaticJSONBArrayContainsAll(tags, []any{"a", 1.5}),
			want:       "tags @> $1::jsonb",
			wantArgs:   []any{`["a",1.5]`},
			wantErr:    errtest.NoError(),
		},
		{
			name:       "postgres contains all of nil",
			haveDriver: postgresDriver,
			haveClause: StaticJSONBArrayContainsAll(tags, []string(nil)),
			want:       "tags @> $1::jsonb",
			wantArgs:   []any{`[]`},
			wantErr:    errtest.NoError(),
		},
		{
			name:       "postgres object contains",
			haveDriver: postgresDriver,
			haveClause: StaticJSONBObjectContains(
				data,
				map[string]any{
					"a":      map[string]any{"b": "x"},
					"n":      1,
					"labels": []string{"t"},
				},
			),
			want:     "data @> $1::jsonb",
			wantArgs: []any{`{"a":{"b":"x"},"labels":["t"],"n":1}`},
			wantErr:  errtest.NoError(),
		},
		{
			name:       "sqlite contains any",
			haveDriver: sqlite3Driver,
			haveClause: StaticJSONBArrayContainsAny(tags, []string{"a", "b"}),
			want: "(json_type(tags) = 'array' AND EXISTS (SELECT 1 FROM " +
				"json_each(tags) AS jsonb_elem, json_each($1) AS jsonb_want " +
				"WHERE " + sqliteJSONBElementMatch + "))",
			wantArgs: []any{`["a","b"]`},
			wantErr:  errtest.NoError(),
		},
		{
			name:       "sqlite contains any of nothing",
			haveDriver: sqlite3Driver,
			haveClause: StaticJSONBArrayContainsAny(tags, []string{}),
			want:       "1=0",
			wantErr:    errtest.NoError(),
		},
		{
			name:       "sqlite contains all",
			haveDriver: sqlite3Driver,
			haveClause: StaticJSONBArrayContainsAll(tags, []int{1, 2}),
			want: "(json_type(tags) = 'array' AND (SELECT count(DISTINCT " +
				"jsonb_want.key) FROM json_each(tags) AS jsonb_elem, " +
				"json_each($1) AS jsonb_want WHERE " + sqliteJSONBElementMatch +
				") = 2)",
			wantArgs: []any{`[1,2]`},
			wantErr:  errtest.NoError(),
		},
		{
			name:       "sqlite object contains",
			haveDriver: sqlite3Driver,
			haveClause: StaticJSONBObjectContains(
				data,
				map[string]any{
					"a":      map[string]any{"b": "x"},
					"e":      map[string]any{},
					"n":      1.5,
					"i":      2,
					"labels": []string{"t"},
				},
			),
			want: "(json_type(data) = 'object' AND " +
				"json_type(data, $1) IN ('integer', 'real', 'text') AND " +
				"json_extract(data, $2) = $3 AND " +
				"json_type(data, $4) = 'object' AND " +
				"json_type(data, $5) IN ('integer', 'real', 'text') AND " +
				"json_extract(data, $6) = $7 AND " +
				"json_type(data, $8) = 'array' AND (SELECT count(DISTINCT " +
				"jsonb_want.key) FROM json_each(data, $9) AS jsonb_elem, " +
				"json_each($10) AS jsonb_want WHERE " + sqliteJSONBElementMatch +
				") = 1 AND " +
				"json_type(data, $11) IN ('integer', 'real', 'text') AND " +
				"json_extract(data, $12) = $13)",
			wantArgs: []any{
				`$."a"."b"`, `$."a"."b"`, "x",
				`$."e"`,
				`$."i"`, `$."i"`, int64(2),
				`$."labels"`, `$."labels"`, `["t"]`,
				`$."n"`, `$."n"`, 1.5,
			},
			wantErr: errtest.NoError(),
		},
		{
			name:       "sqlite object contains nothing",
			haveDriver: sqlite3Driver,
			haveClause: StaticJSONBObjectContains(data, struct{}{}),
			want:       "(json_type(data) = 'object')",
			wantErr:    errtest.NoError(),
		},
		{
			name:       "boolean element",
			haveDriver: postgresDriver,
			haveClause: StaticJSONBArrayContainsAny(tags, []any{"a", true}),
			wantErr:    errtest.ErrorCause(errInvalidJSONBValue),
		},
		{
			name:       "null element",
			haveDriver: sqlite3Driver,
			haveClause: StaticJSONBArrayContainsAll(tags, []any{nil}),
			wantErr:    errtest.ErrorCause(errInvalidJSONBValue),
		},
		{
			name:       "array of objects",
			haveDriver: postgresDriver,
			haveClause: StaticJSONBArrayContainsAll(tags, []any{map[string]any{}}),
			wantErr:    errtest.ErrorCause(errInvalidJSONBValue),
		},
		{
			name:       "not an array",
			haveDriver: sqlite3Driver,
			haveClause: StaticJSONBArrayContainsAny(tags, "a"),
			wantErr:    errtest.ErrorCause(errInvalidJSONBValue),
		},
		{
			name:       "object holding an array of objects",
			haveDriver: postgresDriver,
			haveClause: StaticJSONBObjectContains(
				data,
				map[string]any{"items": []any{map[string]any{"sku": "x"}}},
			),
			wantErr: errtest.ErrorCause(errInvalidJSONBValue),
		},
		{
			name:       "object holding a boolean",
			haveDriver: sqlite3Driver,
			haveClause: StaticJSONBObjectContains(
				data,
				map[string]any{"a": map[string]any{"b": false}},
			),
			wantErr: errtest.ErrorCause(errInvalidJSONBValue),
		},
		{
			name:       "not an object",
			haveDriver: postgresDriver,
			haveClause: StaticJSONBObjectContains(data, []string{"a"}),
			wantErr:    errtest.ErrorCause(errInvalidJSONBValue),
		},
		{
			name:       "sqlite object key holding a double quote",
			haveDriver: sqlite3Driver,
			haveClause: StaticJSONBObjectContains(data, map[string]any{`a"b`: 1}),
			wantErr:    errtest.ErrorCause(errInvalidJSONBKey),
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			w := queryWriter{driver: tt.haveDriver}

			err := tt.haveClause.WriteTo(&w, nil)

			tt.wantErr.Assert(t, err)
			assert.Equal(t, tt.want, w.String())
			assert.Equal(t, tt.wantArgs, w.vs)
		})
	}
}

type driverQueryer struct {
	*static.Queryer

	driver string
}

func (dq driverQueryer) Driver() string { return dq.driver }

func TestJSONBPredicateClauseDriver(t *testing.T) {
	var (
		q static.Queryer

		tags = Column("tags")
		qb   = QueryBuilder{
			Queryer: driverQueryer{Queryer: &q, driver: sqlite3Driver},
		}
	)

	_, err := qb.PrepareDelete(
		DeleteStatement{
			Table:       "rows",
			WhereClause: JSONBArrayContainsAny(tags),
		},
	).Exec(context.Background(), map[string]any{tags.Binding(): []string{"c"}})

	require.NoError(t, err)
	require.Len(t, q.ExecQueries, 1)
	assert.Equal(
		t,
		static.Query{
			Query: "DELETE FROM rows WHERE (json_type(tags) = 'array' AND " +
				"EXISTS (SELECT 1 FROM json_each(tags) AS jsonb_elem, " +
				"json_each($1) AS jsonb_want WHERE " + sqliteJSONBElementMatch +
				"))",
			Args: []any{`["c"]`},
		},
		q.ExecQueries[0],
	)
}
