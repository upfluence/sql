package roundrobin

import (
	"context"
	"sync"

	"github.com/upfluence/sql"
)

func NewDB(dbs ...sql.DB) sql.DB {
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

func (d *db) Exec(ctx context.Context, q string, vs ...interface{}) (sql.Result, error) {
	return d.nextDB().Exec(ctx, q, vs...)
}

func (d *db) QueryRow(ctx context.Context, q string, vs ...interface{}) sql.Scanner {
	return d.nextDB().QueryRow(ctx, q, vs...)
}

func (d *db) Query(ctx context.Context, q string, vs ...interface{}) (sql.Cursor, error) {
	return d.nextDB().Query(ctx, q, vs...)
}
