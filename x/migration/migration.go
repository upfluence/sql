package migration

import (
	"context"
	"io"
	"io/ioutil"
	"time"

	"github.com/upfluence/errors"
	"github.com/upfluence/sql"
)

type Migrator interface {
	Up(context.Context) error
	Down(context.Context) error
}

type MultiMigrator []Migrator

func (ms MultiMigrator) Up(ctx context.Context) error {
	var errs []error

	for _, m := range ms {
		if err := m.Up(ctx); err != nil {
			errs = append(errs, err)
		}
	}

	return errors.WrapErrors(errs)
}

func (ms MultiMigrator) Down(ctx context.Context) error {
	var errs []error

	for _, m := range ms {
		if err := m.Down(ctx); err != nil {
			errs = append(errs, err)
		}
	}

	return errors.WrapErrors(errs)
}

type migrator struct {
	sql.DB
	d Driver

	source      Source
	transformer ErrorTransformer

	opts *options
}

func NewMigrator(db sql.DB, s Source, opts ...Option) Migrator {
	o := *defaultOptions

	for _, opt := range opts {
		opt(&o)
	}

	return &migrator{
		DB:          db,
		source:      s,
		d:           fetchDriver(db.Driver()),
		transformer: o.errorTransformer(),
		opts:        &o,
	}
}

func (m *migrator) Down(ctx context.Context) error {
	if _, err := m.Exec(ctx, m.opts.createTableMigrationStmt()); err != nil {
		return errors.Wrap(err, "cant build migration table")
	}

	for {
		done, err := m.downOne(ctx)

		if done || err != nil {
			return errors.Wrap(err, "migration failed")
		}
	}
}

func (m *migrator) Up(ctx context.Context) error {
	if _, err := m.Exec(ctx, m.opts.createTableMigrationStmt()); err != nil {
		return errors.Wrap(err, "cant build migration table")
	}

	for {
		done, err := m.upOne(ctx)

		if done || err != nil {
			return errors.Wrap(err, "migration failed")
		}
	}
}

func (m *migrator) downOne(ctx context.Context) (bool, error) {
	var done bool

	err := m.executeTx(ctx, func(q sql.Queryer) error {
		mi, err := m.currentMigration(ctx, q)

		if mi == nil || err != nil {
			done = (mi == nil)
			return err
		}

		r, err := mi.Down(m.d)

		if err != nil {
			return errors.Wrapf(err, "cant open DOWN migration file for %d", mi.ID())
		}

		if errM := m.transformer.Transform(
			mi,
			executeMigration(ctx, r, q),
		); errM != nil {
			return errors.Wrapf(errM, "migration %d", mi.ID())
		}

		_, err = q.Exec(ctx, m.opts.deleteMigrationStmt(), mi.ID())

		return errors.Wrapf(err, "cant remove migration from the table %d", mi.ID())
	})

	return done, err
}

func (m *migrator) upOne(ctx context.Context) (bool, error) {
	var done bool

	err := m.executeTx(ctx, func(q sql.Queryer) error {
		mi, err := m.nextMigration(ctx, q)

		if mi == nil || err != nil {
			done = (mi == nil)
			return err
		}

		r, err := mi.Up(m.d)

		if err != nil {
			return errors.Wrapf(err, "cant open UP migration file for %d", mi.ID())
		}

		if errM := m.transformer.Transform(
			mi,
			executeMigration(ctx, r, q),
		); errM != nil {
			return errors.Wrapf(errM, "migration %d", mi.ID())
		}

		_, err = q.Exec(ctx, m.opts.addMigrationStmt(), mi.ID(), time.Now())

		return errors.Wrapf(err, "cant add migration to the table %d", mi.ID())
	})

	return done, err
}

func (m *migrator) executeTx(ctx context.Context, fn func(sql.Queryer) error) error {
	// Isolation level serializable will avoid having multiple migration to be
	// executed at the same time.
	return sql.ExecuteTx(
		ctx,
		m,
		sql.TxOptions{Isolation: sql.LevelSerializable},
		fn,
	)
}

func (m *migrator) currentMigration(ctx context.Context, q sql.Queryer) (Migration, error) {
	var num sql.NullInt64

	if err := q.QueryRow(ctx, m.opts.lastMigrationStmt()).Scan(&num); err != nil {
		return nil, errors.Wrap(err, "fetch last migration")
	}

	if num.Valid {
		return m.source.Get(ctx, uint(num.Int64))
	}

	return nil, nil
}

func (m *migrator) nextMigration(ctx context.Context, q sql.Queryer) (Migration, error) {
	var (
		num sql.NullInt64
		mi  Migration
		err error
	)

	if err := q.QueryRow(ctx, m.opts.lastMigrationStmt()).Scan(&num); err != nil {
		return nil, errors.Wrap(err, "fetch last migration")
	}

	if num.Valid {
		ok, mID, errNext := m.source.Next(ctx, uint(num.Int64))

		if errNext != nil {
			return nil, errors.Wrapf(errNext, "next migration from %d", num.Int64)
		}

		if !ok {
			return nil, nil
		}

		mi, err = m.source.Get(ctx, mID)
	} else {
		mi, err = m.source.First(ctx)
	}

	return mi, errors.Wrapf(err, "fetching %d", mi.ID())
}

func executeMigration(ctx context.Context, r io.ReadCloser, q sql.Queryer) error {
	buf, err := ioutil.ReadAll(r)

	if err != nil {
		return errors.Wrap(err, "cant read migration")
	}

	defer r.Close()

	_, err = q.Exec(ctx, string(buf))
	return errors.Wrap(err, "cant execute migration")
}
