package migration

var (
	defaultDriver = &driver{name: "default"}

	driverMap = map[string]Driver{
		"postgres": &driver{
			name:       "postgres",
			extensions: []string{"postgres", "psql"},
		},
		"sqlite3": &driver{
			name:       "sqlite3",
			extensions: []string{"sqlite3", "sqlite"},
		},
	}
)

type Driver interface {
	Name() string
	Extensions() []string
}

func fetchDriver(dname string) Driver {
	d, ok := driverMap[dname]

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
