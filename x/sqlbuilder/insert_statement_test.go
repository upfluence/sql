package sqlbuilder

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/upfluence/sql"
)

func TestInsertQuery(t *testing.T) {
	for _, tt := range []struct {
		name string

		is InsertStatement
		vs map[string]any

		stmt string
		args []any
		err  error
	}{
		{
			name: "insert",
			is: InsertStatement{
				Table:  "foo",
				Fields: []Marker{Column("biz"), Column("buz")},
			},
			vs:   map[string]any{"buz": 1, "biz": 2},
			stmt: "INSERT INTO foo(biz, buz) VALUES ($1, $2)",
			args: []any{2, 1},
		},
		{
			name: "error no marker",
			is:   InsertStatement{Table: "foo"},
			vs:   map[string]any{"bar": []int64{}},
			err:  errNoMarkers,
		},
		{
			name: "error missing key",
			is: InsertStatement{
				Table:  "foo",
				Fields: []Marker{Column("biz"), Column("buz")},
			},
			vs:  map[string]any{"buz": 1},
			err: ErrMissingKey{Key: "biz"},
		},
		{
			name: "with returning key",
			is: InsertStatement{
				Table:     "foo",
				Fields:    []Marker{Column("buz")},
				Returning: &sql.Returning{Field: "bar"},
			},
			vs:   map[string]any{"buz": 1},
			stmt: "INSERT INTO foo(buz) VALUES ($1)",
			args: []any{1, &sql.Returning{Field: "bar"}},
		},
		{
			name: "with on conflict nothing",
			is: InsertStatement{
				Table:  "foo",
				Fields: []Marker{Column("buz")},
				OnConfict: &OnConflictClause{
					Action: Nothing,
				},
			},
			vs:   map[string]any{"buz": 1},
			stmt: "INSERT INTO foo(buz) VALUES ($1) ON CONFLICT DO NOTHING",
			args: []any{1},
		},
		{
			name: "with on conflict update",
			is: InsertStatement{
				Table:  "foo",
				Fields: []Marker{Column("buz")},
				OnConfict: &OnConflictClause{
					Target: &OnConflictTarget{
						Fields: []Marker{Column("buz")},
					},
					Action: Update{
						Column("bar"),
					},
				},
			},
			vs:   map[string]any{"buz": 1, "bar": 2},
			stmt: "INSERT INTO foo(buz) VALUES ($1) ON CONFLICT (buz) DO UPDATE SET bar = $2",
			args: []any{1, 2},
		},
		{
			name: "with on conflict update excluded",
			is: InsertStatement{
				Table:  "foo",
				Fields: []Marker{Column("buz"), Column("bar"), ColumnWithTable("fiz", "foo", "fiz")},
				OnConfict: &OnConflictClause{
					Target: &OnConflictTarget{
						Fields: []Marker{Column("buz")},
					},
					Action: Update{
						Excluded(Column("bar")),
						Excluded(ColumnWithTable("fiz", "foo", "fiz")),
					},
				},
			},
			vs:   map[string]any{"buz": 1, "bar": 2, "fiz": 3},
			stmt: "INSERT INTO foo(buz, bar, fiz) VALUES ($1, $2, $3) ON CONFLICT (buz) DO UPDATE SET bar = EXCLUDED.bar, fiz = EXCLUDED.fiz",
			args: []any{1, 2, 3},
		},
		{
			name: "with on conflict update and where",
			is: InsertStatement{
				Table:  "foo",
				Fields: []Marker{Column("buz")},
				OnConfict: &OnConflictClause{
					Target: &OnConflictTarget{
						Fields: []Marker{Column("buz")},
					},
					Action: Update{
						Column("bar"),
					},
					Where: StaticEq(Column("biz"), "fiz"),
				},
			},
			vs:   map[string]any{"buz": 1, "bar": 2},
			stmt: "INSERT INTO foo(buz) VALUES ($1) ON CONFLICT (buz) DO UPDATE SET bar = $2 WHERE biz = $3",
			args: []any{1, 2, "fiz"},
		},
		{
			name: "with on conflict nothing ignores where",
			is: InsertStatement{
				Table:  "foo",
				Fields: []Marker{Column("buz")},
				OnConfict: &OnConflictClause{
					Action: Nothing,
					Where:  StaticEq(Column("biz"), "fiz"),
				},
			},
			vs:   map[string]any{"buz": 1},
			stmt: "INSERT INTO foo(buz) VALUES ($1) ON CONFLICT DO NOTHING",
			args: []any{1},
		},
		{
			name: "with returning + isQuery",
			is: InsertStatement{
				Table:     "foo",
				Fields:    []Marker{Column("buz")},
				Returning: &sql.Returning{Field: "buz"},
				isQuery:   true,
			},
			vs:   map[string]any{"buz": 1},
			stmt: "INSERT INTO foo(buz) VALUES ($1) RETURNING buz",
			args: []any{1},
		},
		{
			name: "with single returnings",
			is: InsertStatement{
				Table:      "foo",
				Fields:     []Marker{Column("buz")},
				Returnings: []*sql.Returning{{Field: "bar"}},
			},
			vs:   map[string]any{"buz": 1},
			stmt: "INSERT INTO foo(buz) VALUES ($1)",
			args: []any{1, &sql.Returning{Field: "bar"}},
		},
		{
			name: "with returnings ",
			is: InsertStatement{
				Table:  "foo",
				Fields: []Marker{Column("buz")},
				Returnings: []*sql.Returning{
					{Field: "buz"},
					{Field: "bar"},
				},
			},
			vs:   map[string]any{"buz": 1},
			stmt: "INSERT INTO foo(buz) VALUES ($1) RETURNING buz, bar",
			args: []any{1},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			stmt, args, err := tt.is.Clone().buildQuery(tt.vs)

			assert.Equal(t, tt.err, err)
			assert.Equal(t, tt.stmt, stmt)
			assert.Equal(t, tt.args, args)
		})
	}
}

func TestInsertMultiQuery(t *testing.T) {
	for _, tt := range []struct {
		name string

		haveStmt InsertStatement
		haveVVs  []map[string]any

		wantStmt string
		wantArgs []any
		wantErr  error
	}{
		{
			name: "multiple rows",
			haveStmt: InsertStatement{
				Table:  "foo",
				Fields: []Marker{Column("biz"), Column("buz")},
			},
			haveVVs: []map[string]any{
				{"biz": 1, "buz": 2},
				{"biz": 3, "buz": 4},
			},
			wantStmt: "INSERT INTO foo(biz, buz) VALUES ($1, $2), ($3, $4)",
			wantArgs: []any{1, 2, 3, 4},
		},
		{
			name: "multiple rows with returning",
			haveStmt: InsertStatement{
				Table:     "foo",
				Fields:    []Marker{Column("biz")},
				Returning: &sql.Returning{Field: "id"},
			},
			haveVVs: []map[string]any{
				{"biz": 1},
				{"biz": 2},
			},
			wantStmt: "INSERT INTO foo(biz) VALUES ($1), ($2)",
			wantArgs: []any{1, 2, &sql.Returning{Field: "id"}},
		},
		{
			name: "missing key in a row",
			haveStmt: InsertStatement{
				Table:  "foo",
				Fields: []Marker{Column("biz")},
			},
			haveVVs: []map[string]any{
				{"biz": 1},
				{"buz": 2},
			},
			wantErr: ErrMissingKey{Key: "biz"},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			stmt, args, err := tt.haveStmt.buildQueries(tt.haveVVs, nil)

			assert.Equal(t, tt.wantErr, err)
			assert.Equal(t, tt.wantStmt, stmt)
			assert.Equal(t, tt.wantArgs, args)
		})
	}
}
