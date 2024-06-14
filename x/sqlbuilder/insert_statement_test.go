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
		vs map[string]interface{}

		stmt string
		args []interface{}
		err  error
	}{
		{
			name: "insert",
			is: InsertStatement{
				Table:  "foo",
				Fields: []Marker{Column("biz"), Column("buz")},
			},
			vs:   map[string]interface{}{"buz": 1, "biz": 2},
			stmt: "INSERT INTO foo(biz, buz) VALUES ($1, $2)",
			args: []interface{}{2, 1},
		},
		{
			name: "error no marker",
			is:   InsertStatement{Table: "foo"},
			vs:   map[string]interface{}{"bar": []int64{}},
			err:  errNoMarkers,
		},
		{
			name: "error missing key",
			is: InsertStatement{
				Table:  "foo",
				Fields: []Marker{Column("biz"), Column("buz")},
			},
			vs:  map[string]interface{}{"buz": 1},
			err: ErrMissingKey{Key: "biz"},
		},
		{
			name: "with returning key",
			is: InsertStatement{
				Table:     "foo",
				Fields:    []Marker{Column("buz")},
				Returning: &sql.Returning{Field: "bar"},
			},
			vs:   map[string]interface{}{"buz": 1},
			stmt: "INSERT INTO foo(buz) VALUES ($1)",
			args: []interface{}{1, &sql.Returning{Field: "bar"}},
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
			vs:   map[string]interface{}{"buz": 1},
			stmt: "INSERT INTO foo(buz) VALUES ($1) ON CONFLICT DO NOTHING",
			args: []interface{}{1},
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
			vs:   map[string]interface{}{"buz": 1, "bar": 2},
			stmt: "INSERT INTO foo(buz) VALUES ($1) ON CONFLICT (buz) DO UPDATE SET bar = $2",
			args: []interface{}{1, 2},
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
