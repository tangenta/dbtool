package cases

import (
	"bytes"
	"database/sql"
	"fmt"
	"log"
	"math/rand"
	"strconv"
	"strings"
	"time"

	"github.com/zyguan/sqlz"
)

// https://github.com/pingcap/tidb/issues/48304.
func RunTest48304() {
	rg := newRandGen(1699936277163892274)

	db, err := sql.Open("mysql", "root@tcp(127.0.0.1:4000)/test")
	if err != nil {
		panic(err)
	}
	sqlz.MustExec(db, "set global tidb_ddl_enable_fast_reorg=on;")
	sqlz.MustExec(db, "set global tidb_enable_dist_task=off;")
	sqlz.MustExec(db, "drop table if exists t;")
	sqlz.MustExec(db, "create table t(pk bigint primary key auto_increment, j json, i bigint, c char(64)) partition by hash(pk) PARTITIONS 10;")

	prepare(rg, db, 10000)

	sqlz.MustExec(db, "alter table t add index (c, (cast(j->'$.string' as char(64) array)), i)")

	checkTable(rg, db)
}

func prepare(rg *randGen, db *sql.DB, cnt int) {
	var buf bytes.Buffer
	for i := 0; i < cnt; i++ {
		insertSQL := fmt.Sprintf("insert into t(j, i, c) values (%s, %s, %s)", rg.randJSON(), rg.randNumber(buf), rg.randString(buf))
		// sqlz.MustExec(db, insertSQL)
		if i == 8423 {
			sqlz.MustExec(db, insertSQL)
		}
	}
}

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
