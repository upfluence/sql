package stats

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/upfluence/sql"
	"github.com/upfluence/sql/backend/static"
)

func TestBeginTx(t *testing.T) {
	for _, tt := range []struct {
		name       string
		haveErr    error
		wantStatus string
		assertTx   func(*testing.T, *static.Tx, sql.Tx)
	}{
		{
			name:       successCase,
			wantStatus: success,
			assertTx: func(t *testing.T, rawTx *static.Tx, tx sql.Tx) {
				assert.NotEqual(t, rawTx, tx)
			},
		},
		{
			name:       failureCase,
			haveErr:    errQuery,
			wantStatus: failed,
			assertTx: func(t *testing.T, _ *static.Tx, tx sql.Tx) {
				assert.Nil(t, tx)
			},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			rawTx := &static.Tx{}
			rawDB := &static.DB{Tx: rawTx, TxErr: tt.haveErr}
			db, collector := wrap(rawDB)

			tx, err := db.BeginTx(context.Background(), sql.TxOptions{})

			assert.Equal(t, tt.haveErr, err)
			tt.assertTx(t, rawTx, tx)
			assert.Equal(t, counters(0, 0, metric("begin_tx_total", tt.wantStatus)), collector.Get().Counters)
		})
	}
}

func TestTxCompletion(t *testing.T) {
	for _, tt := range []struct {
		name       string
		haveCall   func(sql.Tx) error
		haveTx     *static.Tx
		wantMetric string
		wantErr    error
	}{
		{
			name:       "commit",
			haveCall:   sql.Tx.Commit,
			haveTx:     &static.Tx{},
			wantMetric: "commit_total",
		},
		{
			name:       "rollback error",
			haveCall:   sql.Tx.Rollback,
			haveTx:     &static.Tx{RollbackErr: errQuery},
			wantMetric: "rollback_total",
			wantErr:    errQuery,
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			db, collector := wrap(&static.DB{Tx: tt.haveTx})
			tx, err := db.BeginTx(context.Background(), sql.TxOptions{})

			require.NoError(t, err)
			assert.Equal(t, tt.wantErr, tt.haveCall(tx))
			assert.Equal(
				t,
				counters(0, 0,
					metric("begin_tx_total", success),
					metric(tt.wantMetric, defaultFormatter(tt.wantErr)),
				),
				collector.Get().Counters,
			)
		})
	}
}

func TestTxExec(t *testing.T) {
	rawTx := &static.Tx{Queryer: static.Queryer{ExecResult: sql.StaticResult(42)}}
	db, collector := wrap(&static.DB{Tx: rawTx})
	tx, err := db.BeginTx(context.Background(), sql.TxOptions{})

	require.NoError(t, err)

	res, err := tx.Exec(context.Background(), queryString, 42)

	require.NoError(t, err)
	assert.Equal(t, sql.StaticResult(42), res)
	assert.Equal(t, []static.Query{{Query: queryString, Args: []any{42}}}, rawTx.ExecQueries)
	assert.Equal(
		t,
		counters(1, 0,
			metric("begin_tx_total", success),
			metric("exec_total", success),
		),
		collector.Get().Counters,
	)
}

func defaultFormatter(err error) string {
	if err == nil {
		return success
	}

	return failed
}
