package simple

import (
	"context"
	stdsql "database/sql"

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
	*queryer

	tx *stdsql.Tx
}

func (t *tx) Commit() error   { return t.tx.Commit() }
func (t *tx) Rollback() error { return t.tx.Rollback() }

func (d *db) Driver() string { return d.driver }

func (d *db) BeginTx(ctx context.Context) (sql.Tx, error) {
	t, err := d.db.BeginTx(ctx, nil)

	if err != nil {
		return nil, err
	}

	return &tx{queryer: &queryer{t}, tx: t}, nil
}
