package cmd

import (
	"bytes"
	"database/sql"
	"fmt"
	"log"
	"math/rand"
	"strconv"
	"strings"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/spf13/cobra"
	"github.com/zyguan/sqlz"
)

// test48304Cmd represents the test command
var test48304Cmd = &cobra.Command{
	Use:   "test48304",
	Short: "test script for https://github.com/pingcap/tidb/issues/48304",
	Run:   runTest48304Command,
}

func init() {
	rootCmd.AddCommand(test48304Cmd)
}

func runTest48304Command(cmd *cobra.Command, args []string) {
	// ctx := context.Background()
	rg := newRandGen(1699936277163892274)

	db, err := sql.Open("mysql", "root@tcp(127.0.0.1:4000)/test")
	if err != nil {
		panic(err)
	}
	sqlz.MustExec(db, "set global tidb_ddl_enable_fast_reorg=on;")
	sqlz.MustExec(db, "drop table if exists t;")
	sqlz.MustExec(db, "create table t(pk bigint primary key auto_increment, j json, i bigint, c char(64)) partition by hash(pk) PARTITIONS 10;")

	prepare(rg, db, 10000)

	// withCancel, cancelFunc := context.WithCancel(ctx)
	// go runDML(withCancel, db)

	sqlz.MustExec(db, "alter table t add index (c, (cast(j->'$.string' as char(64) array)), i)")

	// cancelFunc()

	checkTable(rg, db)
}

func prepare(rg *randGen, db *sql.DB, cnt int) {
	var buf bytes.Buffer
	for i := 0; i < cnt; i++ {
		insertSQL := fmt.Sprintf("insert into t(j, i, c) values (%s, %s, %s)", rg.randJSON(), rg.randNumber(buf), rg.randString(buf))
		sqlz.MustExec(db, insertSQL)
		// if i == 8423 {
		// 	sqlz.MustExec(db, insertSQL)
		// }
	}
}

// func runDML(ctx context.Context, db *sql.DB) {
// 	conns := make([]*sql.Conn, 0, 1)
// 	rg := newRandGen(1699936371278837918)
// 	for i := 0; i < 1; i++ {
// 		conn, err := db.Conn(ctx)
// 		if err != nil && err != context.Canceled {
// 			panic(err)
// 		}
// 		conns = append(conns, conn)
// 	}
// 	exit := false
// 	for !exit {
// 		select {
// 		case <-ctx.Done():
// 			return
// 		default:
// 			dml, _ := randomDML(rg, conns[rg.Intn(len(conns))])
// 			if dml == "delete from t where c = \"\"" {
// 				exit = true
// 			}
// 		}
// 	}
// }

func checkTable(rg *randGen, db *sql.DB) {
	sqlz.MustExec(db, "admin check table t")
	log.Print("admin check table passed\n")
}

var randChars = []byte("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789 ")

type randGen struct {
	*rand.Rand
}

func newRandGen(seeds ...int64) *randGen {
	var seed int64
	if len(seeds) == 0 {
		seed = time.Now().UnixNano()
	} else {
		seed = seeds[0]
	}

	log.Printf("create random generator: %d", seed)
	return &randGen{
		Rand: rand.New(rand.NewSource(seed)),
	}
}

func (r *randGen) randString(arr bytes.Buffer) string {
	lens := r.Intn(64)
	arr.WriteByte('"')
	for i := 0; i < lens; i++ {
		arr.WriteByte(randChars[r.Intn(len(randChars))])
	}
	if r.Intn(500) == 0 {
		arr.WriteByte(' ')
	}
	arr.WriteByte('"')
	s := arr.String()
	arr.Reset()
	return s
}

func (r *randGen) randJSON() string {
	if r.Intn(1000) == 0 {
		return "null"
	}
	arr := make([]string, 2)
	arr[0] = "\"string\":" + r.array(r.randString)
	arr[1] = "\"number\":" + r.array(r.randNumber)
	j := "{" + strings.Join(arr, ",") + "}"

	return "\"" + strings.ReplaceAll(j, "\"", "\\\"") + "\""
}

func (r *randGen) randNumber(_ bytes.Buffer) string {
	return strconv.Itoa(r.Intn(2147483647*2) - 2147483648)
}

func (r *randGen) array(f func(bytes.Buffer) string) string {
	cnt := r.randInt()
	arr := make([]string, cnt)
	var buf bytes.Buffer
	for i := range arr {
		arr[i] = f(buf)
	}
	return "[" + strings.Join(arr, ",") + "]"
}

func (r *randGen) randInt() int {
	cnt := r.Intn(20) + r.Intn(20) + r.Intn(20) + r.Intn(20) + r.Intn(20) - 5
	if cnt < 0 {
		cnt = 0
	}
	return cnt
}

// func randomDML(rg *randGen, conn *sql.Conn) (sql string, err error) {
// 	ctx := context.Background()
// 	var buf bytes.Buffer
// 	var randSQL string
// 	switch rg.Intn(5) {
// 	case 0, 4:
// 		// insert
// 		switch rg.Intn(6) {
// 		case 0:
// 			randSQL = fmt.Sprintf("insert into t(j, i, c) values(%s, %s, %s)", rg.randJSON(), rg.randNumber(buf), rg.randString(buf))
// 		case 1:
// 			randSQL = fmt.Sprintf("insert ignore into t values(%s, %s, %s, %s)", rg.randNumber(buf), rg.randJSON(), rg.randNumber(buf), rg.randString(buf))
// 		case 2:
// 			randSQL = fmt.Sprintf("insert into t values(%s, %s, %s, %s) on duplicate key update j = %s", rg.randNumber(buf), rg.randJSON(), rg.randNumber(buf), rg.randString(buf), rg.randJSON())
// 		case 3:
// 			randSQL = fmt.Sprintf("insert into t values(%s, %s, %s, %s) on duplicate key update c = %s", rg.randNumber(buf), rg.randJSON(), rg.randNumber(buf), rg.randString(buf), rg.randString(buf))
// 		case 4:
// 			randSQL = fmt.Sprintf("insert into t values(%s, %s, %s, %s) on duplicate key update i = %s", rg.randNumber(buf), rg.randJSON(), rg.randNumber(buf), rg.randString(buf), rg.randNumber(buf))
// 		case 5:
// 			randSQL = fmt.Sprintf("insert into t values(%s, %s, %s, %s) on duplicate key update j = %s, c = %s, i = %s", rg.randNumber(buf), rg.randJSON(), rg.randNumber(buf), rg.randString(buf), rg.randJSON(), rg.randString(buf), rg.randNumber(buf))
// 		}
// 	case 1, 3:
// 		// update
// 		switch rg.Intn(5) {
// 		case 0:
// 			randSQL = fmt.Sprintf("update t set j = %s where pk = %d", rg.randJSON(), rg.Intn(100000))
// 		case 1:
// 			randSQL = fmt.Sprintf("update t set c = %s where pk = %d", rg.randString(buf), rg.Intn(100000))
// 		case 2:
// 			randSQL = fmt.Sprintf("update t set i = %s where pk = %d", rg.randNumber(buf), rg.Intn(100000))
// 		case 3:
// 			randSQL = fmt.Sprintf("update t set j = %s, c = %s, i = %s where pk = %d", rg.randJSON(), rg.randString(buf), rg.randNumber(buf), rg.Intn(100000))
// 		case 4:
// 			randSQL = fmt.Sprintf("replace into t values (%s,%s,%s,%s)", rg.randNumber(buf), rg.randJSON(), rg.randNumber(buf), rg.randString(buf))
// 		}
// 	case 2:
// 		// delete
// 		switch rg.Intn(4) {
// 		case 0:
// 			randSQL = fmt.Sprintf("delete from t where pk = %d", rg.Intn(100000))
// 		case 1:
// 			randSQL = fmt.Sprintf("delete from t where i = %s", rg.randNumber(buf))
// 		case 2:
// 			randSQL = fmt.Sprintf("delete from t where c = %s", rg.randString(buf))
// 		case 3:
// 			num := rg.Intn(100000)
// 			randSQL = fmt.Sprintf("delete from t where i > %d and i < %d", num, num+rg.Intn(30))
// 		}
// 	}
// 	log.Printf("exec sql: %s\n", randSQL)
// 	_, err = conn.ExecContext(ctx, randSQL)
// 	return randSQL, err
// }
