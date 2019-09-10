package simple

import (
	"context"
	stdsql "database/sql"
	"sync"

	"github.com/upfluence/sql"
)

type db struct {
	*queryer

	db     *stdsql.DB
	driver string
}

func FromStdDB(stdDB *stdsql.DB) sql.DB {
	return &db{queryer: &queryer{stdDB}, db: stdDB}
}

func NewDB(driver, uri string) (sql.DB, error) {
	var plainDB, err = stdsql.Open(driver, uri)

	if err != nil {
		return nil, err
	}

	return &db{queryer: &queryer{plainDB}, db: plainDB, driver: driver}, nil
}

type tx struct {
	sync.Mutex

	q  *queryer
	tx *stdsql.Tx
}

func (tx *tx) Commit() error {
	tx.Lock()
	defer tx.Unlock()

	return tx.tx.Commit()
}

func (tx *tx) Rollback() error {
	tx.Lock()
	defer tx.Unlock()

	return tx.tx.Rollback()
}

func (tx *tx) Exec(ctx context.Context, qry string, vs ...interface{}) (sql.Result, error) {
	tx.Lock()
	defer tx.Unlock()

	return tx.q.ExecContext(ctx, qry, sql.StripReturningFields(vs)...)
}

func (tx *tx) QueryRow(ctx context.Context, qry string, vs ...interface{}) sql.Scanner {
	tx.Lock()
	defer tx.Unlock()

	return tx.q.QueryRowContext(ctx, qry, vs...)
}

func (tx *tx) Query(ctx context.Context, qry string, vs ...interface{}) (sql.Cursor, error) {
	tx.Lock()
	defer tx.Unlock()

	return tx.q.QueryContext(ctx, qry, vs...)
}

func (d *db) Driver() string { return d.driver }

func (d *db) BeginTx(ctx context.Context) (sql.Tx, error) {
	t, err := d.db.BeginTx(ctx, nil)

	if err != nil {
		return nil, err
	}

	return &tx{q: &queryer{t}, tx: t}, nil
}
