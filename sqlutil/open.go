package sqlutil

import (
	"errors"

	"github.com/upfluence/sql"
	"github.com/upfluence/sql/backend/postgres"
	"github.com/upfluence/sql/backend/replication"
	"github.com/upfluence/sql/backend/roundrobin"
	"github.com/upfluence/sql/backend/simple"
	"github.com/upfluence/sql/backend/sqlite3"
	"github.com/upfluence/sql/sqlparser"
)

var (
	defaultOptions = &builder{parser: sqlparser.DefaultSQLParser()}

	ErrNoDBProvided = errors.New("sql/sqlutil: No DB provided")
)

type dbInput struct {
	isMaster    bool
	driver, uri string
}

func (i *dbInput) buildDB(p sqlparser.SQLParser) (sql.DB, error) {
	var db, err = simple.NewDB(i.driver, i.uri)

	if err != nil {
		return nil, err
	}

	switch i.driver {
	case "postgres":
		db = postgres.NewDB(db, p)
	case "sqlite3":
		db = sqlite3.NewDB(db)
	}

	return db, nil
}

type dbs []sql.DB

func (dbs dbs) buildDB() sql.DB {
	if len(dbs) == 1 {
		return dbs[0]
	}

	return roundrobin.NewDB(dbs...)
}

func (b *builder) buildDB() (sql.DB, error) {
	switch len(b.dbs) {
	case 0:
		return nil, ErrNoDBProvided
	case 1:
		return b.dbs[0].buildDB(b.parser)
	}

	var masters, slaves []sql.DB

	for _, i := range b.dbs {
		db, err := i.buildDB(b.parser)

		if err != nil {
			return nil, err
		}

		if i.isMaster {
			masters = append(masters, db)
		} else {
			slaves = append(slaves, db)
		}
	}

	if len(masters) == 0 || len(slaves) == 0 {
		return roundrobin.NewDB(append(masters, slaves...)...), nil
	}

	return replication.NewDB(
		dbs(masters).buildDB(),
		dbs(slaves).buildDB(),
		b.parser,
	), nil
}

type builder struct {
	dbs         []*dbInput
	middlewares []sql.MiddlewareFactory

	parser sqlparser.SQLParser
}

type Option func(*builder)

func WithDatabase(driver, dsn string, readOnly bool) Option {
	return func(b *builder) {
		b.dbs = append(
			b.dbs,
			&dbInput{driver: driver, uri: dsn, isMaster: !readOnly},
		)
	}
}

func WithMaster(driver, dsn string) Option {
	return WithDatabase(driver, dsn, false)
}

func WithSlave(driver, dsn string) Option {
	return WithDatabase(driver, dsn, true)
}

func WithMiddleware(f sql.MiddlewareFactory) Option {
	return func(b *builder) { b.middlewares = append(b.middlewares, f) }
}

func Open(os ...Option) (sql.DB, error) {
	var opts = *defaultOptions

	for _, o := range os {
		o(&opts)
	}

	db, err := opts.buildDB()

	if err != nil {
		return nil, err
	}

	for _, m := range opts.middlewares {
		db = m.Wrap(db)
	}

	return db, nil
}
