package cases

import (
	"context"
	"database/sql"
	"fmt"
	"sync"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/tangenta/dbtool/util"
)

// https://github.com/pingcap/tidb/issues/50073.
// Preconditions:
//
//	tiup playground nightly --db 2 --kv 1 --pd 1 --tiflash 0
func RunTest50073() {
	fmt.Println("Determine the owner of tidb...")
	tidb1Addr := "root@tcp(127.0.0.1:4000)/test"
	tidb2Addr := "root@tcp(127.0.0.1:4001)/test"

	db, err := sql.Open("mysql", tidb1Addr)
	mustNil(err)
	rs, err := db.Query("select tidb_is_ddl_owner();")
	mustNil(err)
	ss := util.ReadAll(rs)
	db.Close()

	if ss[0][0] == "0" {
		tidb1Addr, tidb2Addr = tidb2Addr, tidb1Addr
	}
	fmt.Printf("Get tidb owner: %s\n", tidb1Addr)

	fmt.Println("Initialize environment...")
	db1a, err := sql.Open("mysql", tidb1Addr)
	mustNil(err)
	defer db1a.Close()
	db2, err := sql.Open("mysql", tidb2Addr)
	mustNil(err)
	defer db2.Close()

	_, err = db1a.Exec("set tidb_enable_ddl = true;")
	mustNil(err)
	_, err = db2.Exec("set tidb_enable_ddl = true;")
	mustNil(err)
	_, err = db1a.Exec("set global tidb_ddl_enable_fast_reorg = 1;")
	mustNil(err)
	_, err = db1a.Exec("set global tidb_enable_dist_task = 1;")
	mustNil(err)

	fmt.Println("Prepare data...")
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
	_, err = db2.Exec("set tidb_enable_ddl = false;")
	mustNil(err)
	_, err = db1a.Exec("set tidb_enable_ddl = true;")
	mustNil(err)

	// Confirm the ddl owner is tidb-1.
	<-time.After(3 * time.Second)
	rs, err = db1a.Query("select tidb_is_ddl_owner();")
	mustNil(err)
	ss = util.ReadAll(rs)
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
		ss = util.ReadAll(rs)
		jobID := ss[0][0]
		fmt.Printf("Admin cancel ddl job(%s)...\n", jobID)
		_, err = db1b.Exec(fmt.Sprintf("admin cancel ddl jobs %s;", jobID))
		mustNil(err)
		fmt.Printf("Admin cancel ddl job(%s) done.\n", jobID)
	}()
	fmt.Println("Add another index should not block...")
	_, err = db1a.Exec("alter table t add index idx_a2(a);")
	mustNil(err)
	cancel()

	// Clean up.
	_, err = db1a.Exec("set tidb_enable_ddl = true;")
	mustNil(err)
	_, err = db2.Exec("set tidb_enable_ddl = true;")
	mustNil(err)

	fmt.Println("The test passed!")
}

func mustNil(err error) {
	if err != nil {
		fmt.Printf("Panic error: %s\n", err.Error())
		panic(err)
	}
}

func printAll(rows *sql.Rows) {
	ss := util.ReadAll(rows)
	for _, s := range ss {
		for _, v := range s {
			fmt.Printf("%v ", v)
		}
		fmt.Println()
	}
}
