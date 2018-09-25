package migration

import (
	"bytes"
	"context"
	"io"
	"io/ioutil"
	"sort"
	"strconv"
	"strings"

	"github.com/pkg/errors"
	"github.com/upfluence/log"
)

var ErrNotExist = errors.New("x/migration: This migration does not exist")

type Migration interface {
	ID() uint

	Up(Driver) (io.ReadCloser, error)
	Down(Driver) (io.ReadCloser, error)
}

type Source interface {
	Get(context.Context, uint) (Migration, error)

	First(context.Context) (Migration, error)
	Next(context.Context, uint) (bool, uint, error)
	Prev(context.Context, uint) (bool, uint, error)
}

type StaticFetcher func(string) ([]byte, error)

type migration struct {
	id   uint
	name string

	fetcher func(string) (io.ReadCloser, error)

	ups   map[string]string
	downs map[string]string
}

func (m *migration) ID() uint { return m.id }

func (m *migration) Up(d Driver) (io.ReadCloser, error) {
	for _, ext := range d.Extensions() {
		if fname, ok := m.ups[ext]; ok {
			return m.fetcher(fname)
		}
	}

	return nil, ErrNotExist
}

func (m *migration) Down(d Driver) (io.ReadCloser, error) {
	for _, ext := range d.Extensions() {
		if fname, ok := m.downs[ext]; ok {
			return m.fetcher(fname)
		}
	}

	return nil, ErrNotExist
}

type migrations []*migration

func (m migrations) Len() int               { return len(m) }
func (m migrations) Less(i int, j int) bool { return m[i].id < m[j].id }
func (m migrations) Swap(i int, j int)      { m[i], m[j] = m[j], m[i] }

type staticSource struct {
	ms migrations
}

func (s *staticSource) findPos(id uint) (int, error) {
	for i, m := range s.ms {
		if m.id == id {
			return i, nil
		}
	}

	return 0, ErrNotExist
}

func (s *staticSource) Get(_ context.Context, id uint) (Migration, error) {
	var i, err = s.findPos(id)

	if err != nil {
		return nil, err
	}

	return s.ms[i], nil
}

func (s *staticSource) First(context.Context) (Migration, error) {
	if len(s.ms) == 0 {
		return nil, ErrNotExist
	}

	return s.ms[0], nil
}

func (s *staticSource) Next(_ context.Context, id uint) (bool, uint, error) {
	var i, err = s.findPos(id)

	if err != nil {
		return false, 0, err
	}

	if len(s.ms) == i+1 {
		return false, 0, nil
	}

	return true, s.ms[i+1].id, nil
}

func (s *staticSource) Prev(_ context.Context, id uint) (bool, uint, error) {
	var i, err = s.findPos(id)

	if err != nil {
		return false, 0, err
	}

	if len(s.ms) == 0 || i == 0 {
		return false, 0, nil
	}

	return true, s.ms[i-1].id, nil
}

func splitFilename(fname string) (bool, uint, string, bool, string) {
	var fchunks = strings.Split(fname, ".")

	if len(fchunks) < 3 || (fchunks[1] != "up" && fchunks[1] != "down") {
		return false, 0, "", false, ""
	}

	mname := fchunks[0]
	mchunks := strings.Split(mname, "_")

	id, err := strconv.Atoi(mchunks[0])

	if id < 0 || err != nil {
		return false, 0, "", false, ""
	}

	return true, uint(id), mname, fchunks[1] == "up", strings.Join(
		fchunks[2:],
		".",
	)
}

func wrapFetcher(fn StaticFetcher) func(string) (io.ReadCloser, error) {
	return func(fname string) (io.ReadCloser, error) {
		var buf, err = fn(fname)

		if err != nil {
			return nil, err
		}

		return ioutil.NopCloser(bytes.NewReader(buf)), nil
	}
}

func NewStaticSource(fs []string, fn StaticFetcher, logger log.Logger) Source {
	var (
		migrationMap   = make(map[uint]*migration)
		wrappedFetcher = wrapFetcher(fn)
	)

	for _, f := range fs {
		ok, id, name, up, extension := splitFilename(f)

		if !ok {
			logger.Warningf("Can't process %q as a migration file", f)
			continue
		}

		m, ok := migrationMap[id]

		if !ok {
			m = &migration{
				id:      id,
				name:    name,
				fetcher: wrappedFetcher,
				ups:     make(map[string]string),
				downs:   make(map[string]string),
			}
		} else if m.name != name {
			logger.Warningf("Name mismatch between migration %q, skipping it", f)
			continue
		}

		if up {
			m.ups[extension] = f
		} else {
			m.downs[extension] = f
		}

		migrationMap[id] = m
	}

	var ms migrations

	for _, m := range migrationMap {
		ms = append(ms, m)
	}

	sort.Sort(ms)

	return &staticSource{ms: ms}
}
