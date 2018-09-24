package roundrobin

import (
	"context"
	"sync"

	"github.com/upfluence/sql"
)

func NewDB(dbs ...sql.DB) sql.DB {
	switch len(dbs) {
	case 0:
		return nil
	case 1:
		return dbs[0]
	}

	return &db{dbs: dbs}
}

type db struct {
	dbs []sql.DB

	mu sync.Mutex
	i  int
}

func (d *db) nextDB() sql.DB {
	d.mu.Lock()
	defer d.mu.Unlock()

	db := d.dbs[d.i]
	d.i = (d.i + 1) % len(d.dbs)

	return db
}

func (d *db) Driver() string {
	var driver = d.dbs[0].Driver()

	for _, db := range d.dbs {
		if db.Driver() != driver {
			panic("uneven driver throughout the backends")
		}
	}

	return driver
}

func (d *db) BeginTx(ctx context.Context) (sql.Tx, error) {
	return d.nextDB().BeginTx(ctx)
}

func (d *db) Exec(ctx context.Context, q string, vs ...interface{}) (sql.Result, error) {
	return d.nextDB().Exec(ctx, q, vs...)
}

func (d *db) QueryRow(ctx context.Context, q string, vs ...interface{}) sql.Scanner {
	return d.nextDB().QueryRow(ctx, q, vs...)
}

func (d *db) Query(ctx context.Context, q string, vs ...interface{}) (sql.Cursor, error) {
	return d.nextDB().Query(ctx, q, vs...)
}
