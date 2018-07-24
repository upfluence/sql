package roundrobin

import (
	"testing"

	"github.com/upfluence/sql"
	"github.com/upfluence/sql/sqltest"
)

func Test_db_nextDB(t *testing.T) {
	var (
		db0, db1, db2 sqltest.StaticDB

		db = NewDB(&db0, &db1, &db2).(*db)

		assertFn = func(d sql.DB) {
			if v := db.nextDB(); d != v {
				t.Errorf("Next DB is wrong: %v instead of: %v", v, d)
			}
		}
	)

	assertFn(&db0)
	assertFn(&db1)
	assertFn(&db2)

	assertFn(&db0)
	assertFn(&db1)
	assertFn(&db2)
}
