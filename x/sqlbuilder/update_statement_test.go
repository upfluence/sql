package sqlbuilder

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestUpdateQuery(t *testing.T) {
	for _, tt := range []struct {
		name string

		us UpdateStatement
		vs map[string]interface{}

		stmt string
		args []interface{}
		err  error
	}{
		{
			name: "update all",
			us: UpdateStatement{
				Table:  "foo",
				Fields: []Marker{Column("biz"), Column("buz")},
			},
			vs:   map[string]interface{}{"buz": 1, "biz": 2},
			stmt: "UPDATE foo SET biz = $1, buz = $2",
			args: []interface{}{2, 1},
		},
		{
			name: "update specific",
			us: UpdateStatement{
				Table:       "foo",
				Fields:      []Marker{Column("biz"), Column("buz")},
				WhereClause: Eq(Column("bar")),
			},
			vs:   map[string]interface{}{"buz": 1, "biz": 2, "bar": "foo"},
			stmt: "UPDATE foo SET biz = $1, buz = $2 WHERE bar = $3",
			args: []interface{}{2, 1, "foo"},
		},
		{
			name: "error no markers",
			us: UpdateStatement{
				Table:       "foo",
				WhereClause: Eq(Column("bar")),
			},
			vs:  map[string]interface{}{"buz": 1, "biz": 2, "bar": "foo"},
			err: errNoMarkers,
		},
		{
			name: "error missing key",
			us: UpdateStatement{
				Table:       "foo",
				Fields:      []Marker{Column("biz"), Column("buz")},
				WhereClause: Eq(Column("bar")),
			},
			vs:  map[string]interface{}{"buz": 1, "bar": "foo"},
			err: ErrMissingKey{"biz"},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			stmt, args, err := tt.us.Clone().buildQuery(tt.vs)

			assert.Equal(t, tt.err, err)
			assert.Equal(t, tt.stmt, stmt)
			assert.Equal(t, tt.args, args)
		})
	}
}
