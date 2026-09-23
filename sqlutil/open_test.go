package sqlutil

import (
	"database/sql"
	"testing"

	"github.com/lib/pq"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/upfluence/sql/backend/postgres"
)

func TestOpenPostgresDB(t *testing.T) {
	db, err := Open(WithMaster("postgres", "foobar"))

	if err != nil {
		t.Errorf("Open() = (_, %+v) wanted nil", err)
	}

	if !postgres.IsPostgresDB(db) {
		t.Errorf("invalid wrapping of the DB")
	}
}

func TestRegisterDriverWrapper(t *testing.T) {
	sql.Register("bizbuz", &pq.Driver{})
	RegisterDriverWrapper("bizbuz", postgres.NewDB)

	db, err := Open(WithMaster("bizbuz", "foobar"))

	if err != nil {
		t.Errorf("Open() = (_, %+v) wanted nil", err)
	}

	if !postgres.IsPostgresDB(db) {
		t.Errorf("invalid wrapping of the DB")
	}
}

func TestDatabaseOptionsOverrideDefaults(t *testing.T) {
	var stats sql.DBStats

	_, err := Open(
		WithMaster(
			"postgres",
			"foobar",
			WithMaxOpenConns(16),
			WithStdDBCallback(func(db *sql.DB) { stats = db.Stats() }),
		),
	)

	require.NoError(t, err)
	assert.Equal(t, 16, stats.MaxOpenConnections)
}
