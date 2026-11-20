package stats

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	statslib "github.com/upfluence/stats"

	"github.com/upfluence/sql/backend/static"
)

func TestCursorExhaustion(t *testing.T) {
	rawCursor := &static.MultipleCursor{}
	rawDB := &static.DB{Queryer: static.Queryer{QueryScanner: rawCursor}}
	db, collector := wrap(rawDB)

	cursor, err := db.Query(context.Background(), "query")

	require.NoError(t, err)
	assert.Equal(t, counters(0, 0), collector.Get().Counters)
	assert.False(t, cursor.Next())
	assert.Equal(t, counters(0, 0, metric("query_total", success)), collector.Get().Counters)
}

func TestCursorIterationError(t *testing.T) {
	rawCursor := &static.MultipleCursor{ReturnedErr: errQuery}
	rawDB := &static.DB{Queryer: static.Queryer{QueryScanner: rawCursor}}
	db, collector := wrap(rawDB)

	cursor, err := db.Query(context.Background(), "query")

	require.NoError(t, err)
	assert.False(t, cursor.Next())
	assert.Equal(t, errQuery, cursor.Err())
	assert.Equal(t, counters(0, 0, metric("query_total", failed)), collector.Get().Counters)
}

func TestCursorScanError(t *testing.T) {
	rawCursor := &static.MultipleCursor{Scanners: []static.Scanner{{Err: errQuery}}}
	rawDB := &static.DB{Queryer: static.Queryer{QueryScanner: rawCursor}}
	db, collector := wrap(rawDB)

	cursor, err := db.Query(context.Background(), "query")

	require.NoError(t, err)
	assert.True(t, cursor.Next())
	assert.Equal(t, errQuery, cursor.Scan())
	assert.Equal(t, counters(0, 0, metric("query_total", failed)), collector.Get().Counters)
}

func TestCursorClose(t *testing.T) {
	for _, tt := range []struct {
		name       string
		haveErr    error
		wantStatus string
	}{
		{name: successCase, wantStatus: success},
		{name: failureCase, haveErr: errQuery, wantStatus: failed},
	} {
		t.Run(tt.name, func(t *testing.T) {
			rawCursor := &static.MultipleCursor{CloseErr: tt.haveErr}
			rawDB := &static.DB{Queryer: static.Queryer{QueryScanner: rawCursor}}
			db, collector := wrap(rawDB)

			cursor, err := db.Query(context.Background(), "query")

			require.NoError(t, err)
			assert.Equal(t, tt.haveErr, cursor.Close())
			assert.Equal(t, counters(0, 0, metric("query_total", tt.wantStatus)), collector.Get().Counters)
		})
	}
}

func TestCursorFinishesOnce(t *testing.T) {
	rawCursor := &static.MultipleCursor{}
	rawDB := &static.DB{Queryer: static.Queryer{QueryScanner: rawCursor}}
	db, collector := wrap(rawDB)

	cursor, err := db.Query(context.Background(), "query")

	require.NoError(t, err)
	assert.False(t, cursor.Next())
	assert.NoError(t, cursor.Err())
	assert.NoError(t, cursor.Close())
	assert.Equal(t, counters(0, 0, metric("query_total", success)), collector.Get().Counters)
}

func TestCursorRowsScanned(t *testing.T) {
	rawCursor := &static.MultipleCursor{
		Scanners: []static.Scanner{{}, {}},
	}
	rawDB := &static.DB{Queryer: static.Queryer{QueryScanner: rawCursor}}
	db, collector := wrap(rawDB)

	cursor, err := db.Query(context.Background(), queryString)

	require.NoError(t, err)

	for cursor.Next() {
		require.NoError(t, cursor.Scan())
	}

	assert.Equal(t, counters(0, 2, metric("query_total", success)), collector.Get().Counters)
}

func TestCursorWithoutRowsScannedTracking(t *testing.T) {
	rawCursor := &static.MultipleCursor{Scanners: []static.Scanner{{}}}
	rawDB := &static.DB{Queryer: static.Queryer{QueryScanner: rawCursor}}
	cfg := testConfig()
	cfg.TrackRowsScanned = false
	collector := statslib.NewStaticCollector()
	db := NewFactory(statslib.RootScope(collector), cfg).Wrap(rawDB)

	cursor, err := db.Query(context.Background(), queryString)

	require.NoError(t, err)
	require.True(t, cursor.Next())
	require.NoError(t, cursor.Scan())
	assert.False(t, cursor.Next())
	assert.Equal(
		t,
		[]statslib.Int64Snapshot{
			{Name: "query_total", Labels: map[string]string{statusLabel: success}, Value: 1},
			{Name: "rows_affected_total", Labels: map[string]string{}, Value: 0},
		},
		collector.Get().Counters,
	)
}
