package cases

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"net/http"
	"strings"
	"time"

	"github.com/tangenta/dbtool/util"
	"golang.org/x/sync/errgroup"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/tools/clientcmd"
)

// https://github.com/pingcap/tidb/issues/50894.
// Preconditions:
//
//  1. ./create-cluster.sh
//  2. kubeconfig.yml in project root dir.
func RunTest50894() {
	config, err := clientcmd.BuildConfigFromFlags("", "kubeconfig.yml")
	mustNil(err)

	dynCli, err := dynamic.NewForConfig(config)
	mustNil(err)

	db, err := sql.Open("mysql", "root@tcp(127.0.0.1:4000)/test")
	mustNil(err)
	defer db.Close()

	prepareData(db)

	eg := &errgroup.Group{}

	goWithRecover(eg, func() {
		runAddIndex(db)
	})
	goWithRecover(eg, func() {
		// Wait add index job submit.
		waitSubtaskSubmited(db)
		sendUpgradeRequest("start")
		// Wait upgrade request submit.
		<-time.After(1 * time.Second)
		upgradeTiDBClusterAndWait(dynCli)
		sendUpgradeRequest("finish")
		waitConnReady()
		waitAddIndexFinish()
	})

	err = eg.Wait()
	mustNil(err)
}

func prepareData(db *sql.DB) {
	ctx := context.TODO()
	_, err := db.ExecContext(ctx, "drop table if exists t;")
	mustNil(err)
	_, err = db.ExecContext(ctx, "create table t (a bigint primary key clustered, c varchar(255));")
	mustNil(err)

	log.Println("prepare data...")

	eg := errgroup.Group{}
	eg.SetLimit(10)
	batchCnt := 10000
	batchSize := 1000
	for i := 0; i < batchCnt; i++ {
		i := i
		eg.Go(func() error {
			sb := strings.Builder{}
			sb.WriteString("insert into t values ")
			for j := 0; j < batchSize; j++ {
				sb.WriteString(fmt.Sprintf("(%d, '%s'),", j+i*batchSize, strings.Repeat("a", 10)))
			}
			_, err := db.ExecContext(ctx, sb.String()[:len(sb.String())-1]+";")
			return err
		})
	}
	err = eg.Wait()
	mustNil(err)
}

func runAddIndex(db *sql.DB) {
	log.Println("add index for t...")
	ctx := context.TODO()
	conn, err := db.Conn(ctx)
	mustNil(err)
	_, err = conn.ExecContext(ctx, "set global tidb_enable_dist_task = on;")
	mustNil(err)
	_, err = conn.ExecContext(ctx, "set global tidb_ddl_reorg_worker_cnt = 1;")
	mustNil(err)
	_, err = conn.ExecContext(ctx, "alter table t add index idx(c);")
	if err != nil {
		// Ignore invalid connection error because TiDB restarts.
		if !strings.Contains(err.Error(), "invalid connection") {
			log.Panicf("unexpected error during adding index: %v", err.Error())
		}
	}
	_, err = conn.ExecContext(ctx, "admin check table t;")
	mustNil(err)
}

func waitSubtaskSubmited(db *sql.DB) {
	log.Println("wait subtask to be submited...")
	err := wait.PollUntilContextTimeout(context.TODO(), 200*time.Millisecond, 10*time.Second, true,
		func(ctx context.Context) (bool, error) {
			rs, err := db.QueryContext(ctx, "select 1 from mysql.tidb_background_subtask;")
			if err != nil {
				return false, err
			}
			ss := util.ReadAll(rs)
			return len(ss) > 0, nil
		})
	mustNil(err)
}

func waitAddIndexFinish() {
	log.Println("wait add index to finish...")
	db, err := sql.Open("mysql", "root@tcp(127.0.0.1:4000)/test")
	mustNil(err)
	defer db.Close()
	ctx := context.TODO()
	err = wait.PollUntilContextTimeout(ctx, 5*time.Second, 3*time.Minute, false, func(ctx context.Context) (bool, error) {
		rs, err := db.QueryContext(ctx, "admin show ddl jobs 1;")
		if err != nil {
			return false, err
		}
		ss := util.ReadAll(rs)
		state := ss[0][11]
		if state == "synced" {
			log.Println("add index job state is synced")
			return true, nil
		}
		return false, nil
	})
	mustNil(err)
}

func waitConnReady() {
	log.Println("wait tidb connection ready...")
	err := wait.PollUntilContextTimeout(context.TODO(), 500*time.Millisecond, 10*time.Second, true,
		func(ctx context.Context) (bool, error) {
			fmt.Printf(".")
			db, err := sql.Open("mysql", "root@tcp(127.0.0.1:4000)/test")
			if err != nil {
				return false, nil
			}
			rs, err := db.Query("select 1;")
			if err != nil {
				return false, nil
			}
			ss := util.ReadAll(rs)
			fmt.Println()
			return len(ss) > 0, nil
		})
	mustNil(err)
}

func sendUpgradeRequest(action string) {
	log.Printf("send upgrade http request %s", action)
	url := fmt.Sprintf("http://127.0.0.1:10080/upgrade/%s", action)
	r, err := http.NewRequest("POST", url, http.NoBody)
	mustNil(err)

	err = withRetry(10, 6*time.Second, func() error {
		res, err := http.DefaultClient.Do(r)
		if err != nil {
			return err
		}
		defer res.Body.Close()
		if res.StatusCode != http.StatusOK {
			return fmt.Errorf("unexpected status code: %d", res.StatusCode)
		}
		return nil
	})
	mustNil(err)
}

func withRetry(cnt int, backoff time.Duration, fn func() error) error {
	var err error
	for i := 0; i < cnt; i++ {
		err = fn()
		if err == nil {
			return nil
		}
		log.Printf("meet error %s, retrying...", err.Error())
		<-time.After(backoff)
	}
	return err
}

var gvr = schema.GroupVersionResource{Version: "v1alpha1", Resource: "tidbclusters", Group: "pingcap.com"}

func upgradeTiDBClusterAndWait(cli dynamic.Interface) {
	log.Println("upgrade TiDB cluster to nightly...")
	_, err := cli.
		Resource(gvr).
		Namespace("tidb-cluster").
		Patch(context.TODO(), "tc", types.MergePatchType, []byte(`{"spec": {"version": "nightly"} }`), metav1.PatchOptions{})
	mustNil(err)

	waitTiDBClusterEdited(cli)
	waitTiDBClusterUp(cli)
}

func waitTiDBClusterEdited(cli dynamic.Interface) {
	log.Printf("wait for tidb cluster to be edited")
	waitGetTiDBClusterMsg(cli, 2*time.Second, "pingcap/tidb:nightly")
}

func waitTiDBClusterUp(cli dynamic.Interface) {
	log.Printf("wait for tidb cluster up...")
	waitGetTiDBClusterMsg(cli, 3*time.Second, "TiDB cluster is fully up and running")
}

func waitGetTiDBClusterMsg(cli dynamic.Interface, interval time.Duration, msg string) {
	err := wait.PollUntilContextTimeout(context.TODO(), interval, 3*time.Minute, false, func(ctx context.Context) (bool, error) {
		fmt.Printf(".")
		unstructured, err := cli.Resource(gvr).Namespace("tidb-cluster").Get(context.TODO(), "tc", metav1.GetOptions{})
		if err != nil {
			return false, err
		}
		json, err := unstructured.MarshalJSON()
		if err != nil {
			return false, err
		}
		if strings.Contains(string(json), msg) {
			fmt.Println()
			log.Printf("got message: %s", msg)
			return true, nil
		}
		return false, nil
	})
	mustNil(err)
}

func goWithRecover(eg *errgroup.Group, fn func()) {
	eg.Go(func() (err error) {
		defer func() {
			if r := recover(); r != nil {
				log.Printf("panic: %v", r)
				err = fmt.Errorf("%v", r)
			}
		}()
		fn()
		return nil
	})
}
