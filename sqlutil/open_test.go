package sqlutil

import (
	"testing"

	"github.com/upfluence/sql/backend/postgres"
)

func TestOpenPostgresDB(t *testing.T) {
	db, err := Open(WithMaster("postgres", "foobar"))

	if err != nil {
		t.Errorf("Open() = (_, %+v) wanted nil", err)
	}

	if !postgres.IsPostgresDB(db) {
		t.Errorf("invalid wrapping of the DB")
	}
}
