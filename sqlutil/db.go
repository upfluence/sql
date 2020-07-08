package sqlutil

import (
	"context"
	"strings"

	"github.com/pkg/errors"
	"github.com/upfluence/cfg"
	"github.com/upfluence/cfg/provider/static"
	"github.com/upfluence/pkg/log"

	"github.com/upfluence/sql"
	"github.com/upfluence/sql/middleware/logger"
)

const maxOpenConns = 100

type sqlConfig struct {
	DatabaseURL        string `env:"DATABASE_URL" json:"DATABASE_URL"`
	ReplicaDatabaseURL string `env:"REPLICA_DATABASE_URL" json:"REPLICA_DATABASE_URL"`
	DatabaseDriver     string `env:"DATABASE_DRIVER" json:"DATABASE_DRIVER"`
}

var defaultConfig = sqlConfig{
	DatabaseURL:    "file:local.sqlite3",
	DatabaseDriver: "sqlite3",
}

func OpenDB() (sql.DB, error) {
	var (
		s sqlConfig

		dbOpts = []DBOption{WithMaxOpenConns(maxOpenConns)}
	)

	if err := cfg.NewDefaultConfigurator(
		static.NewProvider(&defaultConfig),
	).Populate(context.Background(), &s); err != nil {
		return nil, errors.Wrap(err, "can't populate config")
	}

	var opts = []Option{
		WithMaster(s.DatabaseDriver, s.DatabaseURL, dbOpts...),
		WithMiddleware(logger.NewDebugFactory(log.Logger)),
	}

	if s.ReplicaDatabaseURL != "" {
		for _, url := range strings.Split(s.ReplicaDatabaseURL, ",") {
			opts = append(opts, WithSlave(s.DatabaseDriver, url, dbOpts...))
		}
	}

	return Open(opts...)
}
