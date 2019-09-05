package sqlite3

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/upfluence/sql/backend/static"
)

func TestQueryer(t *testing.T) {
	for _, tt := range []struct {
		in, out static.Query
		err     error
	}{
		{
			in:  static.Query{Query: "foo", Args: []interface{}{}},
			out: static.Query{Query: "foo", Args: []interface{}{}},
		},

		{
			in:  static.Query{Query: "$1, $2, $3", Args: []interface{}{1, 2, 3}},
			out: static.Query{Query: "?, ?, ?", Args: []interface{}{1, 2, 3}},
		},
		{
			in:  static.Query{Query: "$2, $1, $3", Args: []interface{}{1, 2, 3}},
			out: static.Query{Query: "?, ?, ?", Args: []interface{}{2, 1, 3}},
		},
		{
			in:  static.Query{Query: "$2, $1, $3, $4", Args: []interface{}{1, 2, 3}},
			err: ErrInvalidArgsNumber,
		},
		{
			in:  static.Query{Query: "$2, $1, $4", Args: []interface{}{1, 2, 3}},
			err: ErrInvalidArgsNumber,
		},
	} {
		t.Run(tt.in.Query, func(t *testing.T) {
			sq := static.Queryer{}
			q := queryer{q: &sq}
			_, err := q.Exec(context.Background(), tt.in.Query, tt.in.Args...)
			assert.Equal(t, tt.err, err)

			if err == nil {
				require.Equal(t, 1, len(sq.ExecQueries))
				rq := sq.ExecQueries[0]
				assert.Equal(t, tt.out.Query, rq.Query)
				assert.Equal(t, tt.out.Args, rq.Args)
			}
		})
	}
}
