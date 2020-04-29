package integration

import (
	"context"
	"io"
	"io/ioutil"
	"strings"

	"github.com/upfluence/sql/x/migration"
)

type staticSource struct {
	up, down string
}

func (ss staticSource) ID() uint {
	return 1
}

func (ss staticSource) Up(migration.Driver) (io.ReadCloser, error) {
	return ioutil.NopCloser(strings.NewReader(ss.up)), nil
}

func (ss staticSource) Down(migration.Driver) (io.ReadCloser, error) {
	return ioutil.NopCloser(strings.NewReader(ss.down)), nil
}

func (ss staticSource) Get(_ context.Context, v uint) (migration.Migration, error) {
	if v != 1 {
		return nil, migration.ErrNotExist
	}

	return ss, nil
}

func (ss staticSource) First(context.Context) (migration.Migration, error) {
	return ss, nil
}

func (ss staticSource) Next(context.Context, uint) (bool, uint, error) {
	return false, 0, nil
}

func (ss staticSource) Prev(context.Context, uint) (bool, uint, error) {
	return false, 0, nil
}
