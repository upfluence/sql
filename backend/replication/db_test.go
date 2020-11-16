package replication

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/upfluence/sql"
	"github.com/upfluence/sql/backend/static"
	"github.com/upfluence/sql/sqlparser"
)

type mockDB struct {
	static.DB

	Called bool
}

func (mdb *mockDB) Query(_ context.Context, q string, vs ...interface{}) (sql.Cursor, error) {
	mdb.Called = true

	return nil, nil
}

type mockParser map[string]sqlparser.StmtType

func (p mockParser) GetStatementType(q string) sqlparser.StmtType {
	return p[q]
}

func TestPickDB(t *testing.T) {
	tests := []struct {
		name   string
		query  string
		args   []interface{}
		parser sqlparser.SQLParser
		assert func(*testing.T, *db)
	}{
		{
			name:   "select",
			query:  "foo",
			parser: mockParser(map[string]sqlparser.StmtType{"foo": sqlparser.StmtSelect}),
			assert: func(t *testing.T, db *db) {
				assertDBCalled(t, db.DB, false)
				assertDBCalled(t, db.slave, true)
			},
		},
		{
			name:   "update",
			query:  "foo",
			parser: mockParser(map[string]sqlparser.StmtType{"foo": sqlparser.StmtUpdate}),
			assert: func(t *testing.T, db *db) {
				assertDBCalled(t, db.DB, true)
				assertDBCalled(t, db.slave, false)
			},
		},
		{
			name:   "strongly consistent",
			query:  "foo",
			args:   []interface{}{sql.StronglyConsistent},
			parser: mockParser(map[string]sqlparser.StmtType{"foo": sqlparser.StmtSelect}),
			assert: func(t *testing.T, db *db) {
				assertDBCalled(t, db.DB, true)
				assertDBCalled(t, db.slave, false)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var (
				db0, db1 mockDB

				db = &db{
					DB:     &db0,
					slave:  &db1,
					parser: tt.parser,
				}
			)

			db.Query(context.Background(), tt.query, tt.args...)

			tt.assert(t, db)
		})
	}
}

func assertDBCalled(t *testing.T, db sql.DB, v bool) {
	var mdb, ok = db.(*mockDB)

	if !ok {
		t.Fatal("invalid db type")
	}

	assert.Equal(t, v, mdb.Called)
}
