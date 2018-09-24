package migration

import "fmt"

const (
	createTableMigrationStmtTmpl = `
create table if not exists %s (
	num integer not null,
	created_at timestamp not null,
)
	`
	lastMigrationStmtTmpl = `select max(num) from %s`
)

var defaultOptions = &options{migrationTable: "migrations"}

type Option func(*options)

func MigrationTable(tableName string) Option {
	return func(o *options) { o.migrationTable = tableName }
}

type options struct {
	migrationTable string
}

func (o *options) createTableMigrationStmt() string {
	return fmt.Sprintf(createTableMigrationStmtTmpl, o.migrationTable)
}

func (o *options) lastMigrationStmt() string {
	return fmt.Sprintf(lastMigrationStmtTmpl, o.migrationTable)
}
