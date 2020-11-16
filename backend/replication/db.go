package replication

import (
	"context"

	"github.com/upfluence/sql"
	"github.com/upfluence/sql/sqlparser"
)

func NewDB(master sql.DB, slave sql.DB, parser sqlparser.SQLParser) sql.DB {
	return &db{DB: master, slave: slave, parser: parser}
}

type db struct {
	sql.DB

	slave  sql.DB
	parser sqlparser.SQLParser
}

func (d *db) pickDB(q string, vs []interface{}) sql.DB {
	if sqlparser.IsDML(d.parser.GetStatementType(q)) || forceMaster(vs) {
		return d.DB
	}

	return d.slave
}

func (d *db) QueryRow(ctx context.Context, q string, vs ...interface{}) sql.Scanner {
	return d.pickDB(q, vs).QueryRow(ctx, q, vs...)
}

func (d *db) Query(ctx context.Context, q string, vs ...interface{}) (sql.Cursor, error) {
	return d.pickDB(q, vs).Query(ctx, q, vs...)
}

func forceMaster(vs []interface{}) bool {
	for _, v := range vs {
		if c, ok := v.(sql.Consistency); ok && c == sql.StronglyConsistent {
			return true
		}
	}

	return false
}
