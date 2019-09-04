package postgres

import (
	"context"
	"testing"

	"github.com/upfluence/sql"
	"github.com/upfluence/sql/backend/static"
	"github.com/upfluence/sql/sqlparser"
)

func testQueryer(t *testing.T, qfn func(sql.DB) sql.Queryer) {
	updateQ := static.Query{
		Query: "UPDATE foo SET bar = $1 WHERE buz= $2",
		Args:  []interface{}{1, 2},
	}

	insertQ := static.Query{
		Query: "INSERT INTO foo(bar, buz) VALUES ($1, $2)",
		Args:  []interface{}{1, 2},
	}

	for _, tt := range []struct {
		name    string
		in, out static.Query
		id      int64
	}{
		{
			name: "update",
			in:   updateQ,
			out:  updateQ,
			id:   1,
		},
		{
			name: "insert no returning",
			in:   insertQ,
			out:  insertQ,
			id:   1,
		},
		{
			name: "insert with returning",
			in: static.Query{
				Query: "INSERT INTO foo(bar, buz) VALUES ($1, $2)",
				Args:  []interface{}{1, 2, &sql.Returning{Field: "baz"}},
			},
			out: static.Query{
				Query: "INSERT INTO foo(bar, buz) VALUES ($1, $2) RETURNING baz",
				Args:  []interface{}{1, 2},
			},
			id: 2,
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			q := static.Queryer{
				QueryRowScanner: &static.Scanner{
					Args: []static.ScanArg{static.Int64Arg(2)},
				},
				ExecResult: fakeResult(1),
			}

			b := static.DB{Queryer: q, Tx: &static.Tx{Queryer: q}}
			f := qfn(NewDB(&b, sqlparser.DefaultSQLParser()))

			res, err := f.Exec(context.Background(), tt.in.Query, tt.in.Args...)

			if err != nil {
				t.Errorf("db.Exec() = %v, want %v", err, nil)
			}

			if id, _ := res.LastInsertId(); id != tt.id {
				t.Errorf("res.LastInsertId() = %v, want %v", id, tt.id)
			}

			qs := append(b.ExecQueries, b.QueryRowQueries...)
			qs = append(qs, b.Tx.(*static.Tx).ExecQueries...)
			qs = append(qs, b.Tx.(*static.Tx).QueryRowQueries...)

			if len(qs) != 1 {
				t.Errorf("len(q.ExecQueries) = %v, want %v", len(b.ExecQueries), 1)
			}

			qs[0].Assert(t, tt.out.Query, tt.out.Args...)
		})
	}
}

func TestExec(t *testing.T) {
	testQueryer(t, func(db sql.DB) sql.Queryer { return db })
}

func TestTxExec(t *testing.T) {
	testQueryer(t, func(db sql.DB) sql.Queryer {
		tx, err := db.BeginTx(context.Background())

		if err != nil {
			t.Fatalf("db.BeginTx() = %v, want: nil", err)
		}

		return tx
	})
}
