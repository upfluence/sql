package stats

import (
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"
	statslib "github.com/upfluence/stats"

	"github.com/upfluence/sql"
	"github.com/upfluence/sql/backend/static"
)

const (
	failureCase = "failure"
	failed      = "failed"
	queryString = "query"
	statusLabel = "status"
	success     = "success"
	successCase = "success"
)

var instrumentOptions = []statslib.InstrumentOption{
	statslib.DisableDurationTracking(),
	statslib.DisableStartedCounter(),
}

func testConfig() Config {
	return Config{
		ExecOptions:       instrumentOptions,
		QueryOptions:      instrumentOptions,
		QueryRowOptions:   instrumentOptions,
		BeginTxOptions:    instrumentOptions,
		CommitOptions:     instrumentOptions,
		RollbackOptions:   instrumentOptions,
		TrackRowsAffected: true,
		TrackRowsScanned:  true,
	}
}

func wrap(rawDB sql.DB) (sql.DB, *statslib.StaticCollector) {
	collector := statslib.NewStaticCollector()
	db := NewFactory(statslib.RootScope(collector), testConfig()).Wrap(rawDB)

	return db, collector
}

func metric(name, status string) statslib.Int64Snapshot {
	return statslib.Int64Snapshot{
		Name:   name,
		Labels: map[string]string{statusLabel: status},
		Value:  1,
	}
}

func counters(rowsAffected, rowsScanned int64, metrics ...statslib.Int64Snapshot) []statslib.Int64Snapshot {
	metrics = append(
		metrics,
		statslib.Int64Snapshot{Name: "rows_affected_total", Labels: map[string]string{}, Value: rowsAffected},
		statslib.Int64Snapshot{Name: "rows_scanned_total", Labels: map[string]string{}, Value: rowsScanned},
	)
	sort.Sort(statslib.Int64Snapshots(metrics))

	return metrics
}

func TestDBDriver(t *testing.T) {
	db, _ := wrap(&static.DB{})

	assert.Equal(t, "sqltest", db.Driver())
}
