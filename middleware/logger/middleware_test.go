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
		name string
		fn   func(t *testing.T, db sql.DB, ml *mockLogger, sdb *static.DB)
		opts []func(sdb *static.DB)
	}{
		{
			name: "QueryRow",
			fn: func(t *testing.T, db sql.DB, ml *mockLogger, sdb *static.DB) {
				var scanner = db.QueryRow(context.Background(), "foo", args...)

				assert.Equal(t, sdb.QueryRowScanner, scanner)
				assert.Equal(
					t,
					logEvent{op: QueryRow, qs: "foo", args: []interface{}{"bar"}},
					ml.event,
				)
				assert.Equal(
					t,
					[]static.Query{{Query: "foo", Args: args}},
					sdb.QueryRowQueries,
				)
			},
			opts: []func(sdb *static.DB){
				func(sdb *static.DB) { sdb.QueryRowScanner = &emptyScanner{} },
			},
		},
		{
			name: "Exec",
			fn: func(t *testing.T, db sql.DB, ml *mockLogger, sdb *static.DB) {
				var res, err = db.Exec(context.Background(), "foo", args...)

				assert.NoError(t, err)
				assert.Equal(t, sdb.ExecResult, res)
				assert.Equal(
					t,
					logEvent{op: Exec, qs: "foo", args: []interface{}{"bar"}},
					ml.event,
				)
				assert.Equal(
					t,
					[]static.Query{{Query: "foo", Args: args}},
					sdb.ExecQueries,
				)
			},
			opts: []func(sdb *static.DB){
				func(sdb *static.DB) { sdb.ExecResult = sql.StaticResult(1) },
			},
		},
		{
			name: "Query",
			fn: func(t *testing.T, db sql.DB, ml *mockLogger, sdb *static.DB) {
				var cursor, err = db.Query(context.Background(), "foo", args...)

				assert.NoError(t, err)
				assert.Equal(t, sdb.QueryScanner, cursor)
				assert.Equal(
					t,
					logEvent{op: Query, qs: "foo", args: []interface{}{"bar"}},
					ml.event,
				)
				assert.Equal(
					t,
					[]static.Query{{Query: "foo", Args: args}},
					sdb.QueryQueries,
				)
			},
			opts: []func(sdb *static.DB){
				func(sdb *static.DB) { sdb.QueryScanner = &emptyCursor{} },
			},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			executeTest(t, tt.fn, tt.opts...)
		})
	}
}

func executeTest(t *testing.T, fn func(t *testing.T, db sql.DB, ml *mockLogger, sdb *static.DB), opts ...func(*static.DB)) {
	var (
		db = &static.DB{}
		ml = &mockLogger{}
	)

	for _, opt := range opts {
		opt(db)
	}

	fn(t, NewFactory(ml).Wrap(db), ml, db)
}
