//go:build go1.16

package migration

import (
	"io/fs"

	"github.com/upfluence/log"
)

func MustFSSource(fs fs.FS, logger log.Logger) Source {
	s, err := NewFSSource(fs, logger)

	if err != nil {
		panic(err)
	}

	return s
}

func NewFSSource(src fs.FS, logger log.Logger) (Source, error) {
	files, err := fs.ReadDir(src, ".")

	if err != nil {
		return nil, err
	}

	vs := make([]string, len(files))

	for i, f := range files {
		vs[i] = f.Name()
	}

	return NewStaticSource(
		vs,
		func(file string) ([]byte, error) {
			return fs.ReadFile(src, file)
		},
		logger,
	), nil
}
