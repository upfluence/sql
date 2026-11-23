package sqlutil

import (
	"context"
	stdsql "database/sql"
	"testing"

	"github.com/lib/pq"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/upfluence/sql"
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
	stdsql.Register("bizbuz", &pq.Driver{})
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
	var stats stdsql.DBStats

	_, err := Open(
		WithMaster(
			"postgres",
			"foobar",
			WithMaxOpenConns(16),
			WithStdDBCallback(func(db *stdsql.DB) { stats = db.Stats() }),
		),
	)

	require.NoError(t, err)
	assert.Equal(t, 16, stats.MaxOpenConnections)
}

type recordingDB struct {
	sql.DB

	queries []string
}

func (db *recordingDB) Query(_ context.Context, q string, _ ...any) (sql.Cursor, error) {
	db.queries = append(db.queries, q)

	return nil, nil
}

type recordingFactory struct {
	dbs []*recordingDB
}

func (rf *recordingFactory) Wrap(db sql.DB) sql.DB {
	rdb := &recordingDB{DB: db}
	rf.dbs = append(rf.dbs, rdb)

	return rdb
}

func TestDBMiddlewareWrapsEachDatabase(t *testing.T) {
	var master, slave recordingFactory

	db, err := Open(
		WithMaster("postgres", "master", WithDBMiddleware(&master)),
		WithSlave("postgres", "slave", WithDBMiddleware(&slave)),
	)

	require.NoError(t, err)

	for _, q := range []string{"SELECT 1", "UPDATE foo SET bar = 1"} {
		_, err := db.Query(context.Background(), q)

		require.NoError(t, err)
	}

	require.Len(t, master.dbs, 1)
	require.Len(t, slave.dbs, 1)
	assert.True(t, postgres.IsPostgresDB(master.dbs[0].DB))
	assert.True(t, postgres.IsPostgresDB(slave.dbs[0].DB))
	assert.Equal(t, []string{"UPDATE foo SET bar = 1"}, master.dbs[0].queries)
	assert.Equal(t, []string{"SELECT 1"}, slave.dbs[0].queries)
}
