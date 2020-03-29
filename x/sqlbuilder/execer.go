package sqlbuilder

import (
	"context"

	"github.com/upfluence/sql"
)

type Execer interface {
	Exec(context.Context, map[string]interface{}) (sql.Result, error)
}

type execer struct {
	qb   *QueryBuilder
	stmt statement
}

func (e execer) Exec(ctx context.Context, qvs map[string]interface{}) (sql.Result, error) {
	stmt, vs, err := e.stmt.buildQuery(qvs)

	if err != nil {
		return nil, err
	}

	return e.qb.Exec(ctx, stmt, vs...)
}
