// Package stats instruments SQL database operations.
package stats

import (
	"context"

	"github.com/upfluence/stats"

	"github.com/upfluence/sql"
)

// Config configures each SQL operation instrument.
type Config struct {
	ExecOptions     []stats.InstrumentOption
	QueryOptions    []stats.InstrumentOption
	QueryRowOptions []stats.InstrumentOption
	BeginTxOptions  []stats.InstrumentOption
	CommitOptions   []stats.InstrumentOption
	RollbackOptions []stats.InstrumentOption

	TrackRowsAffected bool
	TrackRowsScanned  bool
}

type instruments struct {
	exec     stats.Instrument
	query    stats.Instrument
	queryRow stats.Instrument
	beginTx  stats.Instrument
	commit   stats.Instrument
	rollback stats.Instrument

	rowsAffected stats.Counter
	rowsScanned  stats.Counter
}

type factory struct {
	instruments instruments
}

// NewFactory creates a SQL middleware factory using scope.
func NewFactory(scope stats.Scope, cfg Config) sql.MiddlewareFactory {
	rowsAffected := stats.NoopCounter

	if cfg.TrackRowsAffected {
		rowsAffected = scope.Counter("rows_affected_total")
	}

	rowsScanned := stats.NoopCounter

	if cfg.TrackRowsScanned {
		rowsScanned = scope.Counter("rows_scanned_total")
	}

	return &factory{
		instruments: instruments{
			exec:     stats.NewInstrument(scope, "exec", cfg.ExecOptions...),
			query:    stats.NewInstrument(scope, "query", cfg.QueryOptions...),
			queryRow: stats.NewInstrument(scope, "query_row", cfg.QueryRowOptions...),
			beginTx:  stats.NewInstrument(scope, "begin_tx", cfg.BeginTxOptions...),
			commit:   stats.NewInstrument(scope, "commit", cfg.CommitOptions...),
			rollback: stats.NewInstrument(scope, "rollback", cfg.RollbackOptions...),

			rowsAffected: rowsAffected,
			rowsScanned:  rowsScanned,
		},
	}
}

func (f *factory) Wrap(rawDB sql.DB) sql.DB {
	return &db{
		queryer: queryer{Queryer: rawDB, instruments: f.instruments},
		db:      rawDB,
	}
}

type db struct {
	queryer

	db sql.DB
}

func (d *db) Driver() string {
	return d.db.Driver()
}

func (d *db) BeginTx(ctx context.Context, opts sql.TxOptions) (sql.Tx, error) {
	operation := d.instruments.beginTx.Begin()
	rawTx, err := d.db.BeginTx(ctx, opts)
	operation.Finish(err)

	if err != nil {
		return nil, err //nolint:wrapcheck
	}

	return &tx{
		queryer: queryer{Queryer: rawTx, instruments: d.instruments},
		tx:      rawTx,
	}, nil
}
