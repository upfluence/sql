package stats

import (
	"context"
	stdsql "database/sql"
	"testing"
	"time"

	_ "github.com/lib/pq"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	statslib "github.com/upfluence/stats"
)

func snapshot(name string, value int64) statslib.Int64Snapshot {
	return statslib.Int64Snapshot{
		Name:   name,
		Labels: map[string]string{},
		Value:  value,
	}
}

func poolGauges(maxOpen, open, inUse, idle int64) []statslib.Int64Snapshot {
	return []statslib.Int64Snapshot{
		snapshot("pool_idle_connections", idle),
		snapshot("pool_in_use_connections", inUse),
		snapshot("pool_max_open_connections", maxOpen),
		snapshot("pool_open_connections", open),
	}
}

func TestPoolMetricsReport(t *testing.T) {
	for _, tt := range []struct {
		name         string
		haveSamples  []stdsql.DBStats
		wantGauges   []statslib.Int64Snapshot
		wantCounters []statslib.Int64Snapshot
	}{
		{
			name: "single sample",
			haveSamples: []stdsql.DBStats{
				{
					MaxOpenConnections: 8,
					OpenConnections:    3,
					InUse:              2,
					Idle:               1,
					WaitCount:          4,
					WaitDuration:       1500 * time.Microsecond,
					MaxIdleClosed:      5,
					MaxIdleTimeClosed:  6,
					MaxLifetimeClosed:  7,
				},
			},
			wantGauges: poolGauges(8, 3, 2, 1),
			wantCounters: []statslib.Int64Snapshot{
				snapshot("pool_max_idle_closed_total", 5),
				snapshot("pool_max_idle_time_closed_total", 6),
				snapshot("pool_max_lifetime_closed_total", 7),
				snapshot("pool_wait_millisecond_total", 1),
				snapshot("pool_wait_total", 4),
			},
		},
		{
			name: "cumulative values are not double counted",
			haveSamples: []stdsql.DBStats{
				{
					MaxOpenConnections: 8,
					OpenConnections:    3,
					InUse:              2,
					Idle:               1,
					WaitCount:          4,
					WaitDuration:       1500 * time.Microsecond,
					MaxIdleClosed:      5,
					MaxIdleTimeClosed:  6,
					MaxLifetimeClosed:  7,
				},
				{
					MaxOpenConnections: 8,
					OpenConnections:    1,
					InUse:              0,
					Idle:               1,
					WaitCount:          10,
					WaitDuration:       2600 * time.Microsecond,
					MaxIdleClosed:      5,
					MaxIdleTimeClosed:  8,
					MaxLifetimeClosed:  9,
				},
			},
			wantGauges: poolGauges(8, 1, 0, 1),
			wantCounters: []statslib.Int64Snapshot{
				snapshot("pool_max_idle_closed_total", 5),
				snapshot("pool_max_idle_time_closed_total", 8),
				snapshot("pool_max_lifetime_closed_total", 9),
				snapshot("pool_wait_millisecond_total", 2),
				snapshot("pool_wait_total", 10),
			},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			var (
				prev stdsql.DBStats

				c  = statslib.NewStaticCollector()
				pm = newPoolMetrics(statslib.RootScope(c))
			)

			for _, cur := range tt.haveSamples {
				pm.report(cur, prev)
				prev = cur
			}

			got := c.Get()

			assert.Equal(t, tt.wantGauges, got.Gauges)
			assert.Equal(t, tt.wantCounters, got.Counters)
		})
	}
}

func TestPoolReporter(t *testing.T) {
	db, err := stdsql.Open("postgres", "foobar")

	require.NoError(t, err)

	db.SetMaxOpenConns(8)

	var (
		c  = statslib.NewStaticCollector()
		pr = NewPoolReporter(statslib.RootScope(c), db)

		ctx, cancel = context.WithCancel(context.Background())
	)

	assert.Equal(t, poolGauges(0, 0, 0, 0), c.Get().Gauges)

	cancel()

	require.ErrorIs(t, pr.Run(ctx, time.Hour), context.Canceled)
	assert.Equal(t, poolGauges(8, 0, 0, 0), c.Get().Gauges)
}
