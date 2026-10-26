package sqlbuilder

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/upfluence/errors/errtest"

	"github.com/upfluence/sql/backend/static"
)

const (
	postgresDriver = "postgres"

	skuKey   = "sku"
	skuPath  = `$."sku"`
	idPath   = `$."id"`
	typePath = `$."type"`
)

type testBoundEntity struct {
	Type string `json:"type"`
	ID   int64  `json:"id"`
}

// sqliteBoundEntityMatch returns the condition matching an element of the
// entities column containing a testBoundEntity, its arguments being
// numbered from first.
func sqliteBoundEntityMatch(alias string, first int) string {
	return fmt.Sprintf(
		"EXISTS (SELECT 1 FROM json_each(entities) AS %[1]s WHERE "+
			"json_type((CASE WHEN %[1]s.type = 'object' THEN %[1]s.value END)) = 'object' AND "+
			"json_type((CASE WHEN %[1]s.type = 'object' THEN %[1]s.value END), $%[2]d) IN ('integer', 'real', 'text') AND "+
			"json_extract((CASE WHEN %[1]s.type = 'object' THEN %[1]s.value END), $%[3]d) = $%[4]d AND "+
			"json_type((CASE WHEN %[1]s.type = 'object' THEN %[1]s.value END), $%[5]d) IN ('integer', 'real', 'text') AND "+
			"json_extract((CASE WHEN %[1]s.type = 'object' THEN %[1]s.value END), $%[6]d) = $%[7]d)",
		alias,
		first,
		first+1,
		first+2,
		first+3,
		first+4,
		first+5,
	)
}

func sqliteBoundEntityArgs(e testBoundEntity) []any {
	return []any{idPath, idPath, e.ID, typePath, typePath, e.Type}
}

func TestJSONBPredicateClause(t *testing.T) {
	var (
		tags     = Column("tags")
		data     = Column("data")
		entities = Column("entities")

		campaign     = testBoundEntity{Type: "campaign", ID: 12}
		contribution = testBoundEntity{Type: "contribution", ID: 14}
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
			name:       "postgres contains any object",
			haveDriver: postgresDriver,
			haveClause: StaticJSONBArrayContainsAny(
				entities,
				[]testBoundEntity{campaign, contribution},
			),
			want: "(entities @> $1::jsonb OR entities @> $2::jsonb)",
			wantArgs: []any{
				`[{"id":12,"type":"campaign"}]`,
				`[{"id":14,"type":"contribution"}]`,
			},
			wantErr: errtest.NoError(),
		},
		{
			name:       "sqlite contains any object",
			haveDriver: sqlite3Driver,
			haveClause: StaticJSONBArrayContainsAny(
				entities,
				[]testBoundEntity{campaign, contribution},
			),
			want: "(json_type(entities) = 'array' AND (" +
				sqliteBoundEntityMatch("jsonb_item_1", 1) + " OR " +
				sqliteBoundEntityMatch("jsonb_item_2", 7) + "))",
			wantArgs: append(
				sqliteBoundEntityArgs(campaign),
				sqliteBoundEntityArgs(contribution)...,
			),
			wantErr: errtest.NoError(),
		},
		{
			name:       "sqlite contains all of scalars, objects and arrays",
			haveDriver: sqlite3Driver,
			haveClause: StaticJSONBArrayContainsAll(
				tags,
				[]any{"a", map[string]any{skuKey: "x"}, []any{1}},
			),
			want: "(json_type(tags) = 'array' AND (SELECT count(DISTINCT " +
				"jsonb_want.key) FROM json_each(tags) AS jsonb_elem, " +
				"json_each($1) AS jsonb_want WHERE " + sqliteJSONBElementMatch +
				") = 1 AND EXISTS (SELECT 1 FROM json_each(tags) AS jsonb_item_1 " +
				"WHERE json_type((CASE WHEN jsonb_item_1.type = 'object' THEN jsonb_item_1.value END)) = 'object' AND " +
				"json_type((CASE WHEN jsonb_item_1.type = 'object' THEN jsonb_item_1.value END), $2) IN ('integer', 'real', 'text') " +
				"AND json_extract((CASE WHEN jsonb_item_1.type = 'object' THEN jsonb_item_1.value END), $3) = $4) AND EXISTS " +
				"(SELECT 1 FROM json_each(tags) AS jsonb_item_2 WHERE " +
				"json_type((CASE WHEN jsonb_item_2.type = 'array' THEN jsonb_item_2.value END)) = 'array' AND (SELECT " +
				"count(DISTINCT jsonb_want.key) FROM json_each((CASE WHEN jsonb_item_2.type = 'array' THEN jsonb_item_2.value END)) " +
				"AS jsonb_elem, json_each($5) AS jsonb_want WHERE " +
				sqliteJSONBElementMatch + ") = 1))",
			wantArgs: []any{`["a"]`, skuPath, skuPath, "x", `[1]`},
			wantErr:  errtest.NoError(),
		},
		{
			name:       "postgres object holding an array of objects",
			haveDriver: postgresDriver,
			haveClause: StaticJSONBObjectContains(
				data,
				map[string]any{"items": []any{map[string]any{skuKey: "x"}}},
			),
			want:     "data @> $1::jsonb",
			wantArgs: []any{`{"items":[{"sku":"x"}]}`},
			wantErr:  errtest.NoError(),
		},
		{
			name:       "sqlite object holding an array of objects",
			haveDriver: sqlite3Driver,
			haveClause: StaticJSONBObjectContains(
				data,
				map[string]any{"items": []any{map[string]any{skuKey: "x"}}},
			),
			want: "(json_type(data) = 'object' AND json_type(data, $1) = 'array' " +
				"AND EXISTS (SELECT 1 FROM json_each(data, $2) AS jsonb_item_1 " +
				"WHERE json_type((CASE WHEN jsonb_item_1.type = 'object' THEN jsonb_item_1.value END)) = 'object' AND " +
				"json_type((CASE WHEN jsonb_item_1.type = 'object' THEN jsonb_item_1.value END), $3) IN ('integer', 'real', 'text') " +
				"AND json_extract((CASE WHEN jsonb_item_1.type = 'object' THEN jsonb_item_1.value END), $4) = $5))",
			wantArgs: []any{`$."items"`, `$."items"`, skuPath, skuPath, "x"},
			wantErr:  errtest.NoError(),
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
			name:       "object holding a boolean in an array",
			haveDriver: postgresDriver,
			haveClause: StaticJSONBArrayContainsAny(
				entities,
				[]any{map[string]any{"active": true}},
			),
			wantErr: errtest.ErrorCause(errInvalidJSONBValue),
		},
		{
			name:       "not an array",
			haveDriver: sqlite3Driver,
			haveClause: StaticJSONBArrayContainsAny(tags, "a"),
			wantErr:    errtest.ErrorCause(errInvalidJSONBValue),
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
