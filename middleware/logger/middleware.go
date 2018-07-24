package logger

import (
	"context"
	"fmt"
	"time"

	"github.com/upfluence/log"
	"github.com/upfluence/log/record"

	"github.com/upfluence/sql"
)

type OpType string

const (
	Exec     OpType = "Exec"
	QueryRow OpType = "QueryRow"
	Query    OpType = "Query"
)

type Logger interface {
	Log(OpType, string, []interface{}, time.Duration)
}

type simplifiedLogger struct {
	level  record.Level
	logger log.Logger
}

type durationField struct {
	d time.Duration
}

func (d *durationField) GetKey() string   { return "duration" }
func (d *durationField) GetValue() string { return fmt.Sprintf("%v", d.d) }

type dynamicField struct {
	name  string
	value interface{}
}

func (d *dynamicField) GetKey() string   { return d.name }
func (d *dynamicField) GetValue() string { return fmt.Sprintf("%v", d.value) }

func (l *simplifiedLogger) Log(_ OpType, q string, vs []interface{}, d time.Duration) {
	var logger = l.logger.WithField(&durationField{d})

	for i, v := range vs {
		logger = logger.WithField(&dynamicField{name: fmt.Sprintf("$%d", i+1), value: v})
	}

	logger.Log(l.level, q)
}

func NewFactory(l Logger) sql.MiddlewareFactory {
	return &factory{l: l}
}

func NewLevelFactory(l log.Logger, lvl record.Level) sql.MiddlewareFactory {
	return NewFactory(&simplifiedLogger{logger: l, level: lvl})
}

func NewDebugFactory(l log.Logger) sql.MiddlewareFactory {
	return NewLevelFactory(l, record.Debug)
}

type factory struct {
	l Logger
}

func (f *factory) Wrap(d sql.DB) sql.DB {
	return &db{DB: d, l: f.l}
}

type db struct {
	sql.DB
	l Logger
}

func (d *db) logRequest(t OpType, t0 time.Time, q string, vs []interface{}) {
	d.l.Log(t, q, vs, time.Since(t0))
}

func (d *db) Exec(ctx context.Context, q string, vs ...interface{}) (sql.Result, error) {
	var t0 = time.Now()

	defer d.logRequest(Exec, t0, q, vs)

	return d.DB.Exec(ctx, q, vs...)
}

func (d *db) QueryRow(ctx context.Context, q string, vs ...interface{}) sql.Scanner {
	var t0 = time.Now()

	defer d.logRequest(QueryRow, t0, q, vs)

	return d.DB.QueryRow(ctx, q, vs...)
}

func (d *db) Query(ctx context.Context, q string, vs ...interface{}) (sql.Cursor, error) {
	var t0 = time.Now()

	defer d.logRequest(QueryRow, t0, q, vs)

	return d.DB.Query(ctx, q, vs...)
}
