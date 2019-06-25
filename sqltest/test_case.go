package sqltest

import (
	"context"
	"fmt"
	"io/ioutil"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/upfluence/sql"
	"github.com/upfluence/sql/middleware/logger"
	"github.com/upfluence/sql/sqlutil"
	"github.com/upfluence/sql/x/migration"
)

type testLogger struct {
	testing.TB
}

func (tl testLogger) Log(ot logger.OpType, q string, vs []interface{}, d time.Duration) {
	var b strings.Builder

	fmt.Fprintf(&b, "[OpType: %s] [Duration: %s] ", ot, d.String())

	for i, v := range vs {
		fmt.Fprintf(&b, "[$%d: %v] ", i, v)
	}

	b.WriteString(q)

	tl.TB.Log(b.String())
}

func buildPostgres(t testing.TB) (sqlutil.Option, func()) {
	dsn := os.Getenv("POSTGRES_URL")

	if dsn == "" {
		t.Log("No postgres DSN provided")

		return nil, nil
	}

	return sqlutil.WithMaster("postgres", dsn), func() {}
}

func buildSQLite(t testing.TB) (sqlutil.Option, func()) {
	tmpfile, err := ioutil.TempFile("", "example")

	if err != nil {
		t.Errorf("cant create tmp file: %v", err)
		return nil, nil
	}

	return sqlutil.WithMaster(
		"sqlite3",
		"file:"+tmpfile.Name()+"?cache=shared&mode=memory&_txlock=deferred",
	), func() { os.Remove(tmpfile.Name()) }
}

func defaultTestCaseFunc(t testing.TB) []dbCase {
	var cs []dbCase

	for n, fn := range map[string]func(testing.TB) (sqlutil.Option, func()){
		"postgres": buildPostgres,
		"sqlite3":  buildSQLite,
	} {
		opt, clean := fn(t)

		if opt == nil {
			continue
		}

		db, err := sqlutil.Open(
			opt,
			sqlutil.WithMiddleware(logger.NewFactory(testLogger{t})),
		)

		if err != nil {
			t.Errorf("cant build DB for %q", n)
			continue
		}

		cs = append(cs, dbCase{name: n, clean: clean, db: db})
	}

	return cs
}

type dbCase struct {
	db    sql.DB
	clean func()
	name  string
}

type TestCase struct {
	dbfn func(testing.TB) []dbCase

	mfns []func(sql.DB) migration.Migrator
}

type TestCaseOption func(*TestCase)

func WithMigratorFunc(fn func(sql.DB) migration.Migrator) TestCaseOption {
	return func(tc *TestCase) { tc.mfns = append(tc.mfns, fn) }
}

func NewTestCase(opts ...TestCaseOption) *TestCase {
	tc := TestCase{dbfn: defaultTestCaseFunc}

	for _, opt := range opts {
		opt(&tc)
	}

	return &tc
}

func (tc *TestCase) Run(t *testing.T, fn func(t *testing.T, db sql.DB)) {
	for _, dbc := range tc.dbfn(t) {
		db, clean := dbc.db, dbc.clean

		t.Run(dbc.name, func(t *testing.T) {
			for _, mfn := range tc.mfns {
				if err := mfn(db).Up(context.Background()); err != nil {
					t.Fatalf("can not proceed the migration up: %v", err.Error())
				}
			}

			fn(t, db)

			for _, mfn := range tc.mfns {
				if err := mfn(db).Down(context.Background()); err != nil {
					t.Fatalf("can not proceed the migration up: %v", err.Error())
				}
			}

			clean()
		})
	}
}
