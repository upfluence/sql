package postgres

import (
	"context"
	"fmt"

	"github.com/upfluence/sql"
	"github.com/upfluence/sql/sqlparser"
)

type fakeResult int64

func (r fakeResult) LastInsertId() (int64, error) { return int64(r), nil }
func (fakeResult) RowsAffected() (int64, error)   { return 1, nil }

func NewDB(d sql.DB, parser sqlparser.SQLParser) sql.DB {
	return &db{DB: d, parser: parser}
}

type db struct {
	sql.DB

	parser sqlparser.SQLParser
}

func (d *db) Exec(ctx context.Context, q string, vs ...interface{}) (sql.Result, error) {
	if d.parser.GetStatementType(q) == sqlparser.StmtInsert {
		var (
			args []interface{}
			ret  *sql.Returning
		)

		for _, v := range vs {
			if r, ok := v.(*sql.Returning); ok {
				ret = r
			} else {
				args = append(args, v)
			}
		}

		if ret != nil {
			var id int64

			if err := d.DB.QueryRow(
				ctx,
				fmt.Sprintf("%s RETURNING %s", q, ret.Field),
				args...,
			).Scan(&id); err != nil {
				return nil, err
			}

			return fakeResult(id), nil
		}
	}

	return d.DB.Exec(ctx, q, vs...)
}
