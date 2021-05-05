// +build cgo

package sqlutil

import (
	"github.com/upfluence/sql"
	"github.com/upfluence/sql/backend/sqlite3"
	"github.com/upfluence/sql/sqlparser"
)

func init() {
	RegisterDriverWrapper("sqlite3", newSQLite3DB)
}

func newSQLite3DB(db sql.DB, _ sqlparser.SQLParser) sql.DB {
	return sqlite3.NewDB(db)
}
