package logger

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/upfluence/sql"
	"github.com/upfluence/sql/backend/static"
)

type emptyCursor struct {
	sql.Cursor
}

type emptyScanner struct {
	sql.Scanner
}

type logEvent struct {
	op   OpType
	qs   string
	args []interface{}
}

type mockLogger struct {
	event logEvent
}

func (ml *mockLogger) Log(op OpType, qs string, args []interface{}, _ time.Duration) {
	ml.event = logEvent{op: op, qs: qs, args: args}
}

func TestQueryer(t *testing.T) {
	var args = []interface{}{"bar", sql.StronglyConsistent}

	for _, tt := range []struct {
		op   OpType
		call func(*testing.T, sql.DB) error
		arg  func(static.DB) []static.Query
	}{
		{
			op: QueryRow,
			call: func(t *testing.T, db sql.DB) error {
				sc := db.QueryRow(context.Background(), "foo", args...)

				assert.Equal(t, &emptyScanner{}, sc)

				return nil
			},
			arg: func(db static.DB) []static.Query { return db.QueryRowQueries },
		},
		{
			op: Exec,
			call: func(t *testing.T, db sql.DB) error {
				res, err := db.Exec(context.Background(), "foo", args...)

				assert.Equal(t, sql.StaticResult(1), res)

				return err
			},
			arg: func(db static.DB) []static.Query { return db.ExecQueries },
		},
		{
			op: Query,
			call: func(t *testing.T, db sql.DB) error {
				cursor, err := db.Query(context.Background(), "foo", args...)

				assert.Equal(t, &emptyCursor{}, cursor)

				return err
			},
			arg: func(db static.DB) []static.Query { return db.QueryQueries },
		},
	} {
		t.Run(string(tt.op), func(t *testing.T) {
			var (
				db = &static.DB{
					Queryer: static.Queryer{
						QueryRowScanner: &emptyScanner{},
						QueryScanner:    &emptyCursor{},
						ExecResult:      sql.StaticResult(1),
					},
				}
				ml = &mockLogger{}
			)

			err := tt.call(t, NewFactory(ml).Wrap(db))
			assert.NoError(t, err)

			assert.Equal(
				t,
				logEvent{op: tt.op, qs: "foo", args: []interface{}{"bar"}},
				ml.event,
			)
			assert.Equal(t, []static.Query{{Query: "foo", Args: args}}, tt.arg(*db))
		})
	}
}
