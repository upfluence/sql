// +build cgo

package sqlutil

import (
	"testing"

	"github.com/upfluence/sql/backend/sqlite3"
)

func TestOpenSQLite3DB(t *testing.T) {
	db, err := Open(WithMaster("sqlite3", "foobar"))

	if err != nil {
		t.Errorf("Open() = (_, %+v) wanted nil", err)
	}

	if !sqlite3.IsSQLite3DB(db) {
		t.Errorf("invalid wrapping of the DB")
	}
}
