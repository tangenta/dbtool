package cases

import (
	"context"
	"database/sql"
	"time"
)

// https://github.com/pingcap/tidb/issues/50012.
// Preconditions:
//
//	tiup playground nightly --db 1 --kv 1 --pd 1 --tiflash 0
func RunTest50012(repeatedly bool) {
	db, err := sql.Open("mysql", "root@tcp(127.0.0.1:4000)/test")
	mustNil(err)
	defer db.Close()
	ctx := context.Background()
	conn, err := db.Conn(ctx)
	mustNil(err)
	defer conn.Close()
	_, err = conn.ExecContext(ctx, "SET tidb_multi_statement_mode='ON';")
	mustNil(err)
	_, err = conn.ExecContext(ctx, "drop table if exists t;")
	mustNil(err)
	_, err = conn.ExecContext(ctx, `CREATE TABLE t (
		a bigint(20),
		b int(10),
		PRIMARY KEY (b, a),
		UNIQUE KEY uk_a (a)
	  );
	  `)
	mustNil(err)

	_, err = conn.ExecContext(ctx, `insert into t values (1, 1);`)
	mustNil(err)

	startTime := time.Now()
	for {
		if time.Since(startTime) >= 10*time.Minute {
			break
		}
		_, err = conn.ExecContext(ctx, "begin;")
		mustNil(err)
		rs, err := conn.QueryContext(ctx, "update t set a = 2 where a = 1; select 1;")
		mustNil(err)
		printAll(rs)
		_, err = conn.ExecContext(ctx, "rollback;")
		mustNil(err)
		if !repeatedly {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
}
