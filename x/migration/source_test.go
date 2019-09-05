package migration

import (
	"context"
	"io/ioutil"
	"testing"

	"github.com/upfluence/log"
	"github.com/upfluence/log/record"
)

type mockFetcher map[string]string

func (mf mockFetcher) keys() []string {
	var res []string

	for k := range mf {
		res = append(res, k)
	}

	return res
}

func (mf mockFetcher) fetch(k string) ([]byte, error) {
	v, ok := mf[k]

	if !ok {
		panic("does not exist")
	}

	return []byte(v), nil
}

type sink struct{}

func (sink) Log(record.Record) error { return nil }

func newMockSource(vs map[string]string) Source {
	mf := mockFetcher(vs)

	return NewStaticSource(
		mf.keys(),
		mf.fetch,
		log.NewLogger(log.WithSink(sink{})),
	)
}

func assertMigration(t *testing.T, m Migration, d Driver, id uint, up, down string) {
	if mid := m.ID(); mid != id {
		t.Errorf("migration.ID() = %v, want = %v", mid, id)
	}

	rc, err := m.Up(d)

	if up == "" && err != ErrNotExist {
		t.Errorf("migration.Up() = %v, want = %v", err, ErrNotExist)
	}

	if rc != nil {
		buf, _ := ioutil.ReadAll(rc)

		if sbuf := string(buf); sbuf != up {
			t.Errorf("migration.Up() = %v, want = %v", sbuf, up)
		}
	}

	rc, err = m.Down(d)

	if up == "" && err != ErrNotExist {
		t.Errorf("migration.Down() = %v, want = %v", err, ErrNotExist)
	}

	if rc != nil {
		buf, _ := ioutil.ReadAll(rc)

		if sbuf := string(buf); sbuf != down {
			t.Errorf("migration.Down() = %v, want = %v", sbuf, down)
		}
	}
}

func TestFetcher(t *testing.T) {
	s := newMockSource(
		map[string]string{
			"3_final.down.sql":      "bar",
			"2_initial.up.postgres": "foo",
			"3_final.up.sql":        "bar",
			"other_file":            "fuz",
		},
	)

	ctx := context.Background()
	m, err := s.First(ctx)

	if err != nil {
		t.Errorf("source.First() = %v, want = nil", err)
	}

	assertMigration(t, m, fetchDriver("postgres"), 2, "foo", "")
	assertMigration(t, m, fetchDriver("sqlite3"), 2, "", "")

	_, id, _ := s.Next(ctx, 2)

	if id != 3 {
		t.Errorf("source.Next(_, 2) = %v, want = %v", id, 3)
	}

	m, _ = s.Get(ctx, id)
	assertMigration(t, m, fetchDriver("postgres"), 3, "bar", "bar")

	ok, _, _ := s.Prev(ctx, 2)

	if ok {
		t.Errorf("source.Prev(_, 2) = %v, want = %v", ok, false)
	}

	ok, _, err = s.Next(ctx, 3)

	if err != nil {
		t.Errorf("source.Next(_, 3) = %v, want = %v", err, nil)
	}

	if ok {
		t.Errorf("source.Next(_, 3) = %v, want = %v", ok, false)
	}
}
