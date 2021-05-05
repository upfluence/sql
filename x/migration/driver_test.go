package migration

import "testing"

func TestRegisterDriver(t *testing.T) {
	d := fetchDriver("foo")

	if n := d.Name(); n != "default" {
		t.Errorf("driver.Name() = %q [ want: default ]", n)
	}

	RegisterDriver("foo", PostgresDriver)

	d = fetchDriver("foo")

	if n := d.Name(); n != "postgres" {
		t.Errorf("driver.Name() = %q [ want: postgres ]", n)
	}
}
