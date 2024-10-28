package cases

import (
	"bufio"
	"context"
	"io"
	"time"

	"github.com/tangenta/dbtool/util"
	"golang.org/x/sync/errgroup"
	"k8s.io/client-go/kubernetes"

	apiv1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// https://github.com/pingcap/tidb/issues/50895.
// Preconditions:
//
//  1. ./create-cluster.sh
//  2. kubeconfig.yml in project root dir.
func RunTest50895() {
	_, cli := util.BuildClientSetFromCfg("kubeconfig.yml")

	eg := &errgroup.Group{}

	w := &logWatcher{msg: make(chan string)}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	goWithRecover(eg, func() {
		watchTiDBLogs(ctx, cli, "tc-tidb-0", w)
		close(w.msg)
	})
	goWithRecover(eg, func() {
		for msg := range w.msg {
			if msg == "job get table range" {
				deletePDPod(cli, "tc-pd-0")
			}
		}
	})

	eg.Wait()

	// db, err := sql.Open("mysql", "root@tcp(127.0.0.1:4000)/test")
	// mustNil(err)
	// defer db.Close()

	// prepareData(db)

	// goWithRecover(eg, func() {
	// 	runAddIndex(db)
	// })
	// goWithRecover(eg, func() {
	// 	// Wait add index job submit.
	// 	waitSubtaskSubmited(db)
	// 	sendUpgradeRequest("start")
	// 	// Wait upgrade request submit.
	// 	<-time.After(1 * time.Second)
	// 	upgradeTiDBClusterAndWait(dynCli)
	// 	sendUpgradeRequest("finish")
	// 	waitConnReady()
	// 	waitAddIndexFinish()
	// })

	// err = eg.Wait()
	// mustNil(err)
}

func watchTiDBLogs(ctx context.Context, cli *kubernetes.Clientset, name string, w watcher) {
	timeNow := v1.NewTime(time.Now())
	podLogOpts := apiv1.PodLogOptions{Follow: true, Container: "tidb", SinceTime: &timeNow}
	req := cli.CoreV1().Pods("tidb-cluster").GetLogs(name, &podLogOpts)
	podLogs, err := req.Stream(ctx)
	mustNil(err)
	defer podLogs.Close()

	rd := bufio.NewReader(podLogs)

	line := make([]byte, 0, 32)
	ch := [1]byte{}
	for {
		_, err := rd.Read(ch[:])
		if err == io.EOF || err == context.Canceled || err == context.DeadlineExceeded {
			return
		}
		mustNil(err)
		if ch[0] == '\n' {
			w.notify(string(line))
			line = line[:0]
		} else {
			line = append(line, ch[0])
		}
	}
}

func deletePDPod(cli *kubernetes.Clientset, name string) {
	ctx := context.Background()
	err := cli.CoreV1().Pods("tidb-cluster").Delete(ctx, name, metav1.DeleteOptions{})
	mustNil(err)
}

type watcher interface {
	notify(msg string)
}

type logWatcher struct {
	msg chan string
}

func (w *logWatcher) notify(msg string) {
	w.msg <- msg
}

func int64Ptr(i int64) *int64 { return &i }
