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

	called bool
}

func (mdb *mockDB) Query(_ context.Context, q string, vs ...interface{}) (sql.Cursor, error) {
	mdb.called = true

	return nil, nil
}

type mockParser map[string]sqlparser.StmtType

func (p mockParser) GetStatementType(q string) sqlparser.StmtType {
	return p[q]
}

func TestPickDB(t *testing.T) {
	tests := []struct {
		name       string
		args       []interface{}
		stype      sqlparser.StmtType
		wantMaster bool
	}{
		{
			name:       "select",
			stype:      sqlparser.StmtSelect,
			wantMaster: false,
		},
		{
			name:       "update",
			stype:      sqlparser.StmtUpdate,
			wantMaster: true,
		},
		{
			name:       "strongly consistent",
			args:       []interface{}{sql.StronglyConsistent},
			stype:      sqlparser.StmtSelect,
			wantMaster: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var (
				db0, db1 mockDB

				db = &db{
					DB:    &db0,
					slave: &db1,
					parser: mockParser(map[string]sqlparser.StmtType{
						"foo": tt.stype,
					}),
				}
			)

			db.Query(context.Background(), "foo", tt.args...)
			assert.Equal(t, tt.wantMaster, db0.called)
			assert.Equal(t, !tt.wantMaster, db1.called)
		})
	}
}
