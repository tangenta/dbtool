package cases

import (
	"context"
	"database/sql"
	"fmt"
	"sync"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

func RunTest50073() {
	fmt.Println("Determine the owner of tidb...")
	tidb1Addr := "root@tcp(127.0.0.1:4000)/test"
	tidb2Addr := "root@tcp(127.0.0.1:4001)/test"

	db, err := sql.Open("mysql", tidb1Addr)
	mustNil(err)
	rs, err := db.Query("select tidb_is_ddl_owner();")
	mustNil(err)
	ss := ReadAll(rs)
	db.Close()

	if ss[0][0] == "0" {
		tidb1Addr, tidb2Addr = tidb2Addr, tidb1Addr
	}
	fmt.Printf("Get tidb owner: %s\n", tidb1Addr)

	fmt.Println("Prepare data...")
	db1a, err := sql.Open("mysql", tidb1Addr)
	mustNil(err)
	defer db1a.Close()

	_, err = db1a.Exec("drop table if exists t;")
	mustNil(err)
	_, err = db1a.Exec("create table t (a int);")
	mustNil(err)
	_, err = db1a.Exec("insert into t values (1), (2), (3);")
	mustNil(err)

	db1b, err := sql.Open("mysql", tidb1Addr)
	mustNil(err)
	defer db1b.Close()

	// Start to add index on tidb-1.
	// Evict the ddl owner from tidb-1 to tidb-2.
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		fmt.Println("Start add index on owner...")
		_, err = db1a.Exec("alter table t add index idx_a(a);")
		mustNil(err)
		wg.Done()
	}()
	go func() {
		<-time.After(300 * time.Millisecond)
		fmt.Println("Evict DDL owner...")
		_, err = db1b.Exec("set tidb_enable_ddl = false;")
		mustNil(err)
		wg.Done()
	}()
	wg.Wait()

	// Evict back ddl owner from tidb-2 to tidb-1.
	fmt.Println("Evict back DDL owner...")
	db2, err := sql.Open("mysql", tidb2Addr)
	mustNil(err)
	defer db2.Close()
	_, err = db2.Exec("set tidb_enable_ddl = false;")
	mustNil(err)
	_, err = db1a.Exec("set tidb_enable_ddl = true;")
	mustNil(err)

	// Confirm the ddl owner is tidb-1.
	<-time.After(3 * time.Second)
	rs, err = db1a.Query("select tidb_is_ddl_owner();")
	mustNil(err)
	ss = ReadAll(rs)
	if ss[0][0] != "1" {
		panic("ddl owner is not tidb-1")
	}

	// New adding index should not be blocked forever.
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		startTime := time.Now()
		select {
		case <-time.After(10 * time.Second):
		case <-ctx.Done():
			return
		}

		fmt.Println("Query last 10 seconds log...")
		endTime := time.Now()
		fmtTmpl := "2006-01-02 15:04:05"
		rs, err := db1b.Query(`select * from information_schema.cluster_log where type = 'tidb' 
			and time > ? and time < ?`,
			startTime.Format(fmtTmpl),
			endTime.Format(fmtTmpl))
		mustNil(err)
		printAll(rs)
		rs.Close()

		rs, err = db1b.Query("admin show ddl jobs 1;")
		mustNil(err)
		ss = ReadAll(rs)
		jobID := ss[0][0]
		fmt.Printf("Admin cancel ddl job(%s)...\n", jobID)
		_, err = db1b.Exec(fmt.Sprintf("admin cancel ddl jobs %s;", jobID))
		mustNil(err)
		fmt.Printf("Admin cancel ddl job(%s) done.\n", jobID)
	}()
	fmt.Println("Add another add index should not block...")
	_, err = db1a.Exec("alter table t add index idx_a2(a);")
	mustNil(err)
	cancel()

	fmt.Println("The test passed!")
}

func mustNil(err error) {
	if err != nil {
		fmt.Printf("Panic error: %s\n", err.Error())
		panic(err)
	}
}

func ReadAll(rows *sql.Rows) [][]string {
	defer func() {
		rows.Close()
	}()

	data := make([][]string, 0)
	columns, err := rows.Columns()
	if err != nil {
		panic(err)
	}
	for rows.Next() {
		row := make([]interface{}, len(columns))
		scan := make([]interface{}, len(columns))
		for i := range row {
			scan[i] = &row[i]
		}
		err := rows.Scan(scan...)
		if err != nil {
			panic(err)
		}

		rowstr := make([]string, len(columns))
		for i := range rowstr {
			switch r := row[i].(type) {
			case nil:
				rowstr[i] = "NULL"
			case []byte:
				rowstr[i] = string(r)
			default:
				rowstr[i] = fmt.Sprintf("%v", row[i])
			}
		}
		data = append(data, rowstr)
	}
	err = rows.Err()
	if err != nil {
		panic(err)
	}
	return data
}

func testMultiStmtBug() {
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
		ss := ReadAll(rs)
		_ = ss
		// printAll(rs)
		_, err = conn.ExecContext(ctx, "rollback;")
		mustNil(err)
		time.Sleep(50 * time.Millisecond)
	}
}

func printAll(rows *sql.Rows) {
	ss := ReadAll(rows)
	for _, s := range ss {
		for _, v := range s {
			fmt.Printf("%v ", v)
		}
		fmt.Println()
	}
}
