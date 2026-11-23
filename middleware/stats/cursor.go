package stats

import (
	"sync"

	"github.com/upfluence/stats"

	"github.com/upfluence/sql"
)

type cursor struct {
	sql.Cursor

	once        sync.Once
	operation   stats.Operation
	rowsScanned stats.Counter
}

func (c *cursor) finish(err error) {
	c.once.Do(func() { c.operation.Finish(err) })
}

func (c *cursor) Scan(vs ...any) error {
	err := c.Cursor.Scan(vs...)

	if err != nil {
		c.finish(err)
	} else {
		c.rowsScanned.Inc()
	}

	return err //nolint:wrapcheck
}

func (c *cursor) Next() bool {
	next := c.Cursor.Next()

	if !next {
		c.finish(c.Cursor.Err())
	}

	return next
}

func (c *cursor) Err() error {
	err := c.Cursor.Err()
	c.finish(err)

	return err //nolint:wrapcheck
}

func (c *cursor) Close() error {
	err := c.Cursor.Close()

	if err != nil {
		c.finish(err)

		return err //nolint:wrapcheck
	}

	c.finish(c.Cursor.Err())

	return nil
}
