package replication

import (
	"reflect"
	"testing"

	"github.com/upfluence/sql"
	"github.com/upfluence/sql/sqlparser"
	"github.com/upfluence/sql/sqltest"
)

type mockParser map[string]sqlparser.StmtType

func (p mockParser) GetStatementType(q string) sqlparser.StmtType {
	return p[q]
}

func Test_db_pickDB(t *testing.T) {
	var db0, db1 sqltest.StaticDB

	tests := []struct {
		name  string
		query string
		db    *db
		want  sql.DB
	}{
		{
			name:  "select",
			query: "foo",
			db: &db{
				DB:     &db0,
				slave:  &db1,
				parser: mockParser(map[string]sqlparser.StmtType{"foo": sqlparser.StmtSelect}),
			},
			want: &db1,
		},

		{
			name:  "update",
			query: "foo",
			db: &db{
				DB:     &db0,
				slave:  &db1,
				parser: mockParser(map[string]sqlparser.StmtType{"foo": sqlparser.StmtUpdate}),
			},
			want: &db0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.db.pickDB(tt.query); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("db.pickDB() = %v, want %v", got, tt.want)
			}
		})
	}
}
