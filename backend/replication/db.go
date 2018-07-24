package replication

import (
	"context"

	"github.com/upfluence/sql"
	"github.com/upfluence/sql/sqlparser"
)

func NewDB(master sql.DB, slave sql.DB, parser sqlparser.SQLParser) sql.DB {
	return &db{master: master, slave: slave, parser: parser}
}

type db struct {
	master, slave sql.DB
	parser        sqlparser.SQLParser
}

func (d *db) pickDB(q string) sql.DB {
	if sqlparser.IsDML(d.parser.GetStatementType(q)) {
		return d.master
	}

	return d.slave
}

func (d *db) Exec(ctx context.Context, q string, vs ...interface{}) (sql.Result, error) {
	return d.pickDB(q).Exec(ctx, q, vs...)
}

func (d *db) QueryRow(ctx context.Context, q string, vs ...interface{}) sql.Scanner {
	return d.pickDB(q).QueryRow(ctx, q, vs...)
}

func (d *db) Query(ctx context.Context, q string, vs ...interface{}) (sql.Cursor, error) {
	return d.pickDB(q).Query(ctx, q, vs...)
}
