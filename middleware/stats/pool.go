package stats

import (
	"context"
	stdsql "database/sql"
	"time"

	"github.com/upfluence/stats"
)

type poolMetrics struct {
	maxOpen stats.Gauge
	open    stats.Gauge
	inUse   stats.Gauge
	idle    stats.Gauge

	wait              stats.Counter
	waitDuration      stats.Counter
	maxIdleClosed     stats.Counter
	maxIdleTimeClosed stats.Counter
	maxLifetimeClosed stats.Counter
}

func newPoolMetrics(scope stats.Scope) poolMetrics {
	scope = scope.Scope("pool", nil)

	return poolMetrics{
		maxOpen: scope.Gauge("max_open_connections"),
		open:    scope.Gauge("open_connections"),
		inUse:   scope.Gauge("in_use_connections"),
		idle:    scope.Gauge("idle_connections"),

		wait:              scope.Counter("wait_total"),
		waitDuration:      scope.Counter("wait_millisecond_total"),
		maxIdleClosed:     scope.Counter("max_idle_closed_total"),
		maxIdleTimeClosed: scope.Counter("max_idle_time_closed_total"),
		maxLifetimeClosed: scope.Counter("max_lifetime_closed_total"),
	}
}

// report records cur, the cumulative values of database/sql are added as the
// difference with prev, the previous sample.
func (pm *poolMetrics) report(cur, prev stdsql.DBStats) {
	pm.maxOpen.Update(int64(cur.MaxOpenConnections))
	pm.open.Update(int64(cur.OpenConnections))
	pm.inUse.Update(int64(cur.InUse))
	pm.idle.Update(int64(cur.Idle))

	pm.wait.Add(cur.WaitCount - prev.WaitCount)
	pm.waitDuration.Add(
		cur.WaitDuration.Milliseconds() - prev.WaitDuration.Milliseconds(),
	)
	pm.maxIdleClosed.Add(cur.MaxIdleClosed - prev.MaxIdleClosed)
	pm.maxIdleTimeClosed.Add(cur.MaxIdleTimeClosed - prev.MaxIdleTimeClosed)
	pm.maxLifetimeClosed.Add(cur.MaxLifetimeClosed - prev.MaxLifetimeClosed)
}

// PoolReporter records the connection pool stats of a database.
type PoolReporter struct {
	db      *stdsql.DB
	metrics poolMetrics
}

// NewPoolReporter registers the pool metrics of db in scope.
func NewPoolReporter(scope stats.Scope, db *stdsql.DB) *PoolReporter {
	return &PoolReporter{db: db, metrics: newPoolMetrics(scope)}
}

// Run records the pool stats right away and then every interval, until ctx
// is done.
func (pr *PoolReporter) Run(ctx context.Context, interval time.Duration) error {
	var (
		prev stdsql.DBStats

		t = time.NewTicker(interval)
	)

	defer t.Stop()

	for {
		cur := pr.db.Stats()
		pr.metrics.report(cur, prev)
		prev = cur

		select {
		case <-ctx.Done():
			return ctx.Err() //nolint:wrapcheck
		case <-t.C:
		}
	}
}
