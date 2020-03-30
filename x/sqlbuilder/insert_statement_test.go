package sqlbuilder

import (
	"testing"

	"github.com/stretchr/testify/assert"
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
	} {
		t.Run(tt.name, func(t *testing.T) {
			stmt, args, err := tt.is.Clone().buildQuery(tt.vs)

			assert.Equal(t, tt.err, err)
			assert.Equal(t, tt.stmt, stmt)
			assert.Equal(t, tt.args, args)
		})
	}
}
