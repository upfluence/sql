package migration

import (
	"context"
	"io/ioutil"

	"github.com/pkg/errors"
	"github.com/upfluence/pkg/multierror"
	"github.com/upfluence/sql"
)

type Migrator interface {
	Migrate(context.Context) error
}

type migrator struct {
	sql.DB
	d Driver

	source Source

	opts *options
}

func NewMigrator(db sql.DB, s Source, opts ...Option) Migrator {
	o := *defaultOptions

	for _, opt := range opts {
		opt(&o)
	}

	return &migrator{DB: db, source: s, d: fetchDriver(db.Driver()), opts: &o}
}

func (m *migrator) Migrate(ctx context.Context) error {
	if _, err := m.Exec(ctx, m.opts.createTableMigrationStmt()); err != nil {
		return errors.Wrap(err, "cant build migration table")
	}

	for {
		done, err := m.migrateOne(ctx)
		if err != nil {
			return errors.Wrap(err, "migration failed")
		}

		if done {
			return nil
		}
	}
}

func (m *migrator) migrateOne(ctx context.Context) (bool, error) {
	tx, err := m.BeginTx(ctx)

	if err != nil {
		return false, errors.Wrap(err, "can not open tx")
	}

	var (
		num uint

		wrapErr = func(err error, msg string, args ...interface{}) error {
			return multierror.Combine(
				errors.Wrapf(err, msg, args...),
				errors.Wrap(tx.Rollback(), "rollback"),
			)
		}
	)

	if err := tx.QueryRow(ctx, m.opts.lastMigrationStmt()).Scan(&num); err != nil {
		return false, wrapErr(err, "fetch last migration")
	}

	ok, mID, err := m.source.Next(ctx, num)

	if err != nil {
		return false, wrapErr(err, "cant fetch migration %d", mID)
	}

	if !ok {
		return true, nil
	}

	mi, err := m.source.Get(ctx, mID)

	if err != nil {
		return false, wrapErr(err, "cant fetch migration %d", mID)
	}

	r, err := mi.Up(m.d)

	if err != nil {
		return false, wrapErr(err, "cant open UP migration file for %d", mi.ID)
	}

	buf, err := ioutil.ReadAll(r)

	if err != nil {
		return false, wrapErr(err, "cant read migration %d", mi.ID)
	}

	r.Close()

	if _, err := tx.Exec(ctx, string(buf)); err != nil {
		return false, wrapErr(err, "cant execute migration %d", mi.ID)
	}

	return false, errors.Wrap(tx.Commit(), "cant commit")
}
