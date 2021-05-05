package migration

import "sync"

var (
	defaultDriver = &driver{name: "default"}

	PostgresDriver Driver = &driver{
		name:       "postgres",
		extensions: []string{"postgres", "psql"},
	}

	driversMu = &sync.Mutex{}
	drivers   = map[string]Driver{
		"postgres": PostgresDriver,
		"sqlite3": &driver{
			name:       "sqlite3",
			extensions: []string{"sqlite3", "sqlite"},
		},
	}
)

func RegisterDriver(n string, d Driver) {
	driversMu.Lock()
	defer driversMu.Unlock()

	drivers[n] = d
}

type Driver interface {
	Name() string
	Extensions() []string
}

func fetchDriver(dname string) Driver {
	driversMu.Lock()
	d, ok := drivers[dname]
	driversMu.Unlock()

	if ok {
		return d
	}

	return defaultDriver
}

type driver struct {
	name       string
	extensions []string
}

func (d *driver) Name() string         { return d.name }
func (d *driver) Extensions() []string { return append(d.extensions, "sql") }
