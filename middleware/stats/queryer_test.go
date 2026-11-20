package stats

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/upfluence/errors"
	statslib "github.com/upfluence/stats"

	"github.com/upfluence/sql"
	"github.com/upfluence/sql/backend/static"
)

var errQuery = errors.New("query")

func TestExec(t *testing.T) {
	for _, tt := range []struct {
		name       string
		haveErr    error
		wantStatus string
		wantRows   int64
	}{
		{name: successCase, wantStatus: success, wantRows: 1},
		{name: failureCase, haveErr: errQuery, wantStatus: failed},
	} {
		t.Run(tt.name, func(t *testing.T) {
			rawDB := &static.DB{
				Queryer: static.Queryer{
					ExecResult: sql.StaticResult(42),
					ExecErr:    tt.haveErr,
				},
			}
			db, collector := wrap(rawDB)

			res, err := db.Exec(context.Background(), queryString, 42)

			assert.Equal(t, tt.haveErr, err)
			assert.Equal(t, sql.StaticResult(42), res)
			assert.Equal(t, []static.Query{{Query: queryString, Args: []any{42}}}, rawDB.ExecQueries)
			assert.Equal(t, counters(tt.wantRows, 0, metric("exec_total", tt.wantStatus)), collector.Get().Counters)
		})
	}
}

func TestExecRowsAffected(t *testing.T) {
	for _, tt := range []struct {
		name       string
		haveResult sql.Result
		wantRows   int64
	}{
		{
			name:       "multiple rows",
			haveResult: &static.StaticResult{RowsAffectedRes: 42},
			wantRows:   42,
		},
		{
			name:       "unsupported",
			haveResult: &static.StaticResult{RowsAffectedErr: errQuery},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			rawDB := &static.DB{Queryer: static.Queryer{ExecResult: tt.haveResult}}
			db, collector := wrap(rawDB)

			res, err := db.Exec(context.Background(), queryString)

			require.NoError(t, err)
			assert.Equal(t, tt.haveResult, res)
			assert.Equal(t, counters(tt.wantRows, 0, metric("exec_total", success)), collector.Get().Counters)
		})
	}
}

func TestExecWithoutRowsAffectedTracking(t *testing.T) {
	rawDB := &static.DB{Queryer: static.Queryer{ExecResult: sql.StaticResult(42)}}
	cfg := testConfig()
	cfg.TrackRowsAffected = false
	collector := statslib.NewStaticCollector()
	db := NewFactory(statslib.RootScope(collector), cfg).Wrap(rawDB)

	res, err := db.Exec(context.Background(), queryString)

	require.NoError(t, err)
	assert.Equal(t, sql.StaticResult(42), res)
	assert.Equal(
		t,
		[]statslib.Int64Snapshot{
			{Name: "exec_total", Labels: map[string]string{statusLabel: success}, Value: 1},
			{Name: "rows_scanned_total", Labels: map[string]string{}, Value: 0},
		},
		collector.Get().Counters,
	)
}

func TestQueryRow(t *testing.T) {
	for _, tt := range []struct {
		name       string
		haveErr    error
		wantStatus string
	}{
		{name: successCase, wantStatus: success},
		{name: failureCase, haveErr: errQuery, wantStatus: failed},
	} {
		t.Run(tt.name, func(t *testing.T) {
			rawDB := &static.DB{
				Queryer: static.Queryer{
					QueryRowScanner: static.Scanner{Err: tt.haveErr},
				},
			}
			db, collector := wrap(rawDB)
			scanner := db.QueryRow(context.Background(), queryString, 42)

			assert.Equal(t, counters(0, 0), collector.Get().Counters)

			err := scanner.Scan()

			assert.Equal(t, tt.haveErr, err)
			assert.Equal(t, []static.Query{{Query: queryString, Args: []any{42}}}, rawDB.QueryRowQueries)
			assert.Equal(t, counters(0, 0, metric("query_row_total", tt.wantStatus)), collector.Get().Counters)
		})
	}
}

func TestQueryImmediateError(t *testing.T) {
	rawDB := &static.DB{Queryer: static.Queryer{QueryErr: errQuery}}
	db, collector := wrap(rawDB)

	cursor, err := db.Query(context.Background(), "query")

	assert.Equal(t, errQuery, err)
	assert.Nil(t, cursor)
	assert.Equal(t, counters(0, 0, metric("query_total", failed)), collector.Get().Counters)
}
