package sqlbuilder

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestDeleteQuery(t *testing.T) {
	for _, tt := range []struct {
		name string

		ds DeleteStatement
		vs map[string]interface{}

		stmt string
		args []interface{}
		err  error
	}{
		{
			name: "delete all not allowed",
			ds:   DeleteStatement{Table: "foo"},
			err:  ErrMissingPredicate,
		},
		{
			name: "delete",
			ds: DeleteStatement{
				Table:       "foo",
				WhereClause: Eq(Column("biz")),
			},
			vs:   map[string]interface{}{"buz": 1, "biz": 2},
			stmt: "DELETE FROM foo WHERE biz = $1",
			args: []interface{}{2},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			stmt, args, err := tt.ds.Clone().buildQuery(tt.vs)

			assert.Equal(t, tt.stmt, stmt)
			assert.Equal(t, tt.args, args)
			assert.Equal(t, tt.err, err)
		})
	}
}
