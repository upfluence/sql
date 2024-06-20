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

type RetryExecer struct {
	Execer      Execer
	ShouldRetry func(error) bool
	RetryCount  int
}

func (re *RetryExecer) Exec(ctx context.Context, qvs map[string]interface{}) (sql.Result, error) {
	var i int

	for {
		i++
		res, err := re.Execer.Exec(ctx, qvs)

		if err == nil || !re.ShouldRetry(err) || i > re.RetryCount {
			return res, err
		}
	}
}
