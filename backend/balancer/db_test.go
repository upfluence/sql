package balancer

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/upfluence/sql/backend/static"
)

type emptyScanner struct{}

func (esc emptyScanner) Scan(...interface{}) error { return nil }

func TestRoundRobin(t *testing.T) {
	var (
		db1 = static.DB{Queryer: static.Queryer{QueryRowScanner: emptyScanner{}}}
		db2 = static.DB{Queryer: static.Queryer{QueryRowScanner: emptyScanner{}}}

		db  = NewDB(RoundRobinBalancerBuilder, &db1, &db2)
		ctx = context.Background()
	)

	sc1 := db.QueryRow(ctx, "foo")

	sc2 := db.QueryRow(ctx, "bar")
	assert.Nil(t, sc2.Scan())

	assert.Nil(t, db.QueryRow(ctx, "buz").Scan())

	assert.Nil(t, sc1.Scan())

	assert.Equal(
		t,
		db1.QueryRowQueries,
		[]static.Query{{Query: "foo"}, {Query: "buz"}},
	)
	assert.Equal(t, db2.QueryRowQueries, []static.Query{{Query: "bar"}})
}

func TestLeastPending(t *testing.T) {
	var (
		db1 = static.DB{Queryer: static.Queryer{QueryRowScanner: emptyScanner{}}}
		db2 = static.DB{Queryer: static.Queryer{QueryRowScanner: emptyScanner{}}}

		db  = NewDB(LeastPendingBalancerBuilder, &db1, &db2)
		ctx = context.Background()
	)

	sc1 := db.QueryRow(ctx, "foo")

	sc2 := db.QueryRow(ctx, "bar")
	assert.Nil(t, sc2.Scan())

	assert.Nil(t, db.QueryRow(ctx, "buz").Scan())

	assert.Nil(t, sc1.Scan())

	assert.Equal(t, db1.QueryRowQueries, []static.Query{{Query: "foo"}})
	assert.Equal(
		t,
		db2.QueryRowQueries,
		[]static.Query{{Query: "bar"}, {Query: "buz"}},
	)
}
