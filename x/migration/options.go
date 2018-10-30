package migration

import "fmt"

const (
	createTableMigrationStmtTmpl = `
create table if not exists %s (
	num integer not null,
	created_at timestamp not null
)
	`
	lastMigrationStmtTmpl   = `select max(num) from %s`
	addMigrationStmtTmpl    = `INSERT INTO "%s" (num, created_at) VALUES ($1, $2)`
	deleteMigrationStmtTmpl = `DELETE FROM "%s" WHERE num = $1`
)

var defaultOptions = &options{migrationTable: "migrations"}

type Option func(*options)

func MigrationTable(tableName string) Option {
	return func(o *options) { o.migrationTable = tableName }
}

func AddErrorTransformer(v ErrorTransformer) Option {
	return func(o *options) { o.transformers = append(o.transformers, v) }
}

type options struct {
	migrationTable string
	transformers   []ErrorTransformer
}

func (o *options) errorTransformer() ErrorTransformer {
	return wrapTransformers(o.transformers)
}

func (o *options) createTableMigrationStmt() string {
	return fmt.Sprintf(createTableMigrationStmtTmpl, o.migrationTable)
}

func (o *options) lastMigrationStmt() string {
	return fmt.Sprintf(lastMigrationStmtTmpl, o.migrationTable)
}

func (o *options) addMigrationStmt() string {
	return fmt.Sprintf(addMigrationStmtTmpl, o.migrationTable)
}

func (o *options) deleteMigrationStmt() string {
	return fmt.Sprintf(deleteMigrationStmtTmpl, o.migrationTable)
}
