package sqlbuilder

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/upfluence/sql/backend/static"
)

func TestSelectStatement(t *testing.T) {
	var (
		db = static.DB{
			Queryer: static.Queryer{
				QueryRowScanner: static.Scanner{
					Args: []static.ScanArg{static.Int64Arg(12)},
				},
			},
		}

		qb  = QueryBuilder{Queryer: &db}
		ctx = context.Background()

		res int64
	)

	err := qb.PrepareSelect(
		SelectStatement{
			Table:         "foo",
			SelectClauses: []Marker{Column("foo")},
			WhereClause:   In(Column("bar")),
		},
	).QueryRow(ctx, map[string]interface{}{"bar": []int{1, 2, 3, 4}}).Scan(
		map[string]interface{}{"foo": &res},
	)

	assert.Nil(t, err)
	assert.Equal(t, int64(12), res)

	assert.Equal(t, 1, len(db.QueryRowQueries))

	q := db.QueryRowQueries[0]
	assert.Equal(t, "SELECT foo FROM foo WHERE bar IN ($1, $2, $3, $4)", q.Query)
	assert.Equal(t, []interface{}{1, 2, 3, 4}, q.Args)
}

func TestSelectQuery(t *testing.T) {
	for _, tt := range []struct {
		name string

		ss SelectStatement
		vs map[string]interface{}

		stmt string
		args []interface{}
		err  error
	}{
		{
			name: "join",
			ss: SelectStatement{
				Table:         "foo",
				SelectClauses: []Marker{Column("biz"), Column("buz")},
				JoinClauses: []JoinClause{
					JoinClause{
						Table: "bar",
						Type:  InnerJoin,
						WhereClause: EqMarkers(
							ColumnWithTable("", "bar", "zzz"),
							Column("biz"),
						),
					},
				},
			},
			stmt: "SELECT biz, buz FROM foo INNER JOIN bar ON \"bar\".\"zzz\" = biz",
		},
		{
			name: "group by",
			ss: SelectStatement{
				Table:         "foo",
				SelectClauses: []Marker{Column("biz"), SQLExpression("count", "COUNT(*)")},
				GroupByClause: []Marker{Column("biz")},
				HavingClause:  PlainSQLPredicate("COUNT(*) > 2"),
			},
			stmt: "SELECT biz, COUNT(*) FROM foo GROUP BY biz HAVING COUNT(*) > 2",
		},
		{
			name: "group by multiple",
			ss: SelectStatement{
				Table:         "foo",
				SelectClauses: []Marker{Column("biz"), SQLExpression("count", "COUNT(*)")},
				GroupByClause: []Marker{Column("biz"), Column("bar")},
				HavingClause:  PlainSQLPredicate("COUNT(*) > 2"),
			},
			stmt: "SELECT biz, COUNT(*) FROM foo GROUP BY biz, bar HAVING COUNT(*) > 2",
		},
		{
			name: "and predicate",
			ss: SelectStatement{
				Table:         "foo",
				SelectClauses: []Marker{Column("bar")},
				WhereClause:   And(Lte(Column("foo")), Eq(Column("biz"))),
			},
			vs:   map[string]interface{}{"foo": 1, "biz": 2},
			stmt: "SELECT bar FROM foo WHERE (foo <= $1) AND (biz = $2)",
			args: []interface{}{1, 2},
		},
		{
			name: "empty and",
			ss: SelectStatement{
				Table:         "foo",
				SelectClauses: []Marker{Column("bar")},
				WhereClause:   And(),
			},
			vs:   map[string]interface{}{"foo": 1, "biz": 2},
			stmt: "SELECT bar FROM foo",
		},
		{
			name: "and with nil",
			ss: SelectStatement{
				Table:         "foo",
				SelectClauses: []Marker{Column("bar")},
				WhereClause:   And(nil, nil, PlainSQLPredicate("foo IS NULL")),
			},
			vs:   map[string]interface{}{"foo": 1, "biz": 2},
			stmt: "SELECT bar FROM foo WHERE foo IS NULL",
		},
		{
			name: "and flatten",
			ss: SelectStatement{
				Table:         "foo",
				SelectClauses: []Marker{Column("bar")},
				WhereClause: And(
					And(Eq(Column("foo")), PlainSQLPredicate("foo IS NULL")),
					Eq(Column("biz")),
				),
			},
			vs:   map[string]interface{}{"foo": 1, "biz": 2},
			stmt: "SELECT bar FROM foo WHERE (foo = $1) AND (foo IS NULL) AND (biz = $2)",
			args: []interface{}{1, 2},
		},
		{
			name: "empty in",
			ss: SelectStatement{
				Table:         "foo",
				SelectClauses: []Marker{Column("bar")},
				WhereClause:   In(Column("bar")),
			},
			vs:   map[string]interface{}{"bar": []int64{}},
			stmt: "SELECT bar FROM foo WHERE 1=0",
		},
		{
			name: "static in",
			ss: SelectStatement{
				Table:         "foo",
				SelectClauses: []Marker{Column("bar")},
				WhereClause:   StaticIn(Column("bar"), []int{1, 2, 3}),
			},
			stmt: "SELECT bar FROM foo WHERE bar IN ($1, $2, $3)",
			args: []interface{}{1, 2, 3},
		},
		{
			name: "limit & offset",
			ss: SelectStatement{
				Table:         "foo",
				SelectClauses: []Marker{Column("bar")},
				Limit:         NullableInt{Int: 5, Valid: true},
				Offset:        NullableInt{Int: 1, Valid: true},
			},
			stmt: "SELECT bar FROM foo LIMIT 5 OFFSET 1",
		},
		{
			name: "static eq",
			ss: SelectStatement{
				Table:         "foo",
				SelectClauses: []Marker{Column("bar")},
				WhereClause:   StaticEq(Column("bar"), "buz"),
			},
			stmt: "SELECT bar FROM foo WHERE bar = $1",
			args: []interface{}{"buz"},
		},
		{
			name: "static like",
			ss: SelectStatement{
				Table:         "foo",
				SelectClauses: []Marker{Column("bar")},
				WhereClause:   StaticLike(Column("bar"), "buz"),
			},
			stmt: "SELECT bar FROM foo WHERE bar LIKE $1",
			args: []interface{}{"buz"},
		},
		{
			name: "order by",
			ss: SelectStatement{
				Table:          "foo",
				SelectClauses:  []Marker{Column("bar")},
				WhereClause:    StaticEq(Column("bar"), "buz"),
				OrderByClauses: []OrderByClause{OrderByClause{Field: Column("bar")}},
			},
			stmt: "SELECT bar FROM foo WHERE bar = $1 ORDER BY bar",
			args: []interface{}{"buz"},
		},
		{
			name: "order by multi",
			ss: SelectStatement{
				Table:         "foo",
				SelectClauses: []Marker{Column("bar")},
				WhereClause:   StaticEq(Column("bar"), "buz"),
				OrderByClauses: []OrderByClause{
					OrderByClause{Field: Column("bar")},
					OrderByClause{Field: Column("buz"), Direction: Desc},
				},
			},
			stmt: "SELECT bar FROM foo WHERE bar = $1 ORDER BY bar, buz DESC",
			args: []interface{}{"buz"},
		},
		{
			name: "error no marker",
			ss: SelectStatement{
				Table:       "foo",
				WhereClause: In(Column("bar")),
			},
			vs:  map[string]interface{}{"bar": []int64{}},
			err: errNoMarkers,
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			stmt, args, _, err := tt.ss.Clone().buildQuery(tt.vs)

			assert.Equal(t, tt.err, err)
			assert.Equal(t, tt.stmt, stmt)
			assert.Equal(t, tt.args, args)
		})
	}
}
