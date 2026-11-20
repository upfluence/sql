package stats

import "github.com/upfluence/sql"

type tx struct {
	queryer

	tx sql.Tx
}

func (t *tx) Commit() error {
	operation := t.instruments.commit.Begin()
	err := t.tx.Commit()
	operation.Finish(err)

	return err //nolint:wrapcheck
}

func (t *tx) Rollback() error {
	operation := t.instruments.rollback.Begin()
	err := t.tx.Rollback()
	operation.Finish(err)

	return err //nolint:wrapcheck
}
