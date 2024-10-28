package cmd

import (
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/spf13/cobra"
	appsv1 "k8s.io/api/apps/v1"
	apiv1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/tools/remotecommand"
	"k8s.io/kubectl/pkg/scheme"
)

type benchCtx struct {
	kubeCfgPath   string
	tidbNamespace string
	dataset       string
	tableCount    string
	tableCountInt int
	rowCount      string

	clientset *kubernetes.Clientset
	config    *rest.Config
	benchName string
	tidbSVC   string
}

func init() {
	var bCtx benchCtx
	var benchCmd = &cobra.Command{
		Use: "bench",
		Run: runBenchCmd(&bCtx),
	}
	benchCmd.Flags().StringVar(&bCtx.kubeCfgPath, "kubecfg", "./kubeconfig.yml", "Set the path of kube config file.")
	benchCmd.Flags().StringVar(&bCtx.tidbNamespace, "namespace", "", "Set the namespace of TiDB cluster.")
	benchCmd.Flags().StringVar(&bCtx.dataset, "dataset", "sysbench", "Set the dataset to prepare.")
	benchCmd.Flags().StringVar(&bCtx.tableCount, "tables", "1", "Set the table count of dataset.")
	benchCmd.Flags().StringVar(&bCtx.rowCount, "rows", "100", "Set the row count of dataset.")
	rootCmd.AddCommand(benchCmd)
}

func runBenchCmd(d *benchCtx) func(cmd *cobra.Command, args []string) {
	return func(cmd *cobra.Command, args []string) {
		if len(args) > 1 {
			cmd.Usage()
			return
		}

		if len(args) == 1 {
			switch args[0] {
			case "clean":
				d.init()
				d.deleteBenchToolDeployment()
				return
			default:
			}
		}

		d.init()
		d.deployBenchTool()

		switch d.dataset {
		case "sysbench":
			d.sysbenchPrepare()
			d.benchCreateMultiIndexes()
			// d.benchCreateIndex()
		default:
			panic(fmt.Sprintf("unsupported dataset: %s", d.dataset))
		}
	}
}

func (b *benchCtx) init() {
	b.validateAndFillArgs()
	b.buildClientSetFromCfg()
	b.detectNamespace()
	b.detectClusterInfo()
}

func (b *benchCtx) buildClientSetFromCfg() {
	config, err := clientcmd.BuildConfigFromFlags("", b.kubeCfgPath)
	if err != nil {
		panic(err)
	}
	b.config = config
	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		panic(err)
	}
	b.clientset = clientset
	log.Println("Build clientset from kube config success.")
}

func (b *benchCtx) detectNamespace() {
	if b.tidbNamespace != "" {
		log.Printf("Use specified namespace: %s\n", b.tidbNamespace)
		return
	}
	log.Println("Namespace is not specified, use current context namespace...")
	apiCfg := clientcmd.GetConfigFromFileOrDie(b.kubeCfgPath)
	b.tidbNamespace = apiCfg.Contexts[apiCfg.CurrentContext].Namespace
	if b.tidbNamespace != "" {
		log.Printf("Use current context namespace: %s\n", b.tidbNamespace)
		return
	}
	log.Println("Namespace is not specified, detecting namespace...")
	ns, err := b.clientset.CoreV1().Namespaces().List(context.Background(), metav1.ListOptions{})
	if err != nil {
		panic(err)
	}
	if len(ns.Items) != 1 {
		panic("namespace is not specified and there are multiple namespaces.")
	}
	b.tidbNamespace = ns.Items[0].Name
	log.Printf("Detected namespace: %s\n", b.tidbNamespace)
}

func (b *benchCtx) detectClusterInfo() {
	cli := b.clientset.CoreV1().Services(b.tidbNamespace)
	svcList, err := cli.List(context.Background(), metav1.ListOptions{
		LabelSelector: "app.kubernetes.io/component=tidb",
	})
	if err != nil {
		panic(err)
	}
	var tidbSvc string
	for _, item := range svcList.Items {
		if strings.HasSuffix(item.Name, "tidb") {
			tidbSvc = item.Name
			break
		}
	}
	b.tidbSVC = tidbSvc
	log.Printf("Get TiDB service name: %s\n", tidbSvc)
}

func (b *benchCtx) validateAndFillArgs() {
	// Convert tableCount to tableCountInt
	tableCountInt, err := strconv.Atoi(b.tableCount)
	if err != nil {
		panic(err)
	}
	b.tableCountInt = tableCountInt
}

func (b *benchCtx) deployBenchTool() {
	cli := b.clientset.AppsV1().Deployments(b.tidbNamespace)

	_, err := cli.Get(context.TODO(), "bench", metav1.GetOptions{})

	if err != nil && !errors.IsNotFound(err) {
		panic(err)
	}

	if !errors.IsNotFound(err) {
		b.getRunningBenchPodName()
		log.Printf("Exist deployment %q.\n", b.benchName)
		return
	}

	log.Println("Creating deployment...")
	_, err = cli.Create(context.TODO(), benchToolDeployment, metav1.CreateOptions{})
	if err != nil {
		panic(err)
	}

	b.waitPodStatus(podStatusRunning)
	log.Printf("Created deployment %q.\n", b.benchName)
}

func (b *benchCtx) executeSQLInTestDB(sql string) string {
	cmd := fmt.Sprintf("mysql -h %s -P 4000 -u root -D test -e '%s'", b.tidbSVC, sql)
	return b.execCmdOnPod(cmd)
}

func (b *benchCtx) executeSQL(sql string) string {
	cmd := fmt.Sprintf("mysql -h %s -P 4000 -u root -e '%s'", b.tidbSVC, sql)
	return b.execCmdOnPod(cmd)
}

func (b *benchCtx) sysbenchPrepare() {
	ret := b.executeSQL("create database if not exists test;")
	fmt.Print(ret)
	ret = b.executeSQLInTestDB("select count(1) from sbtest1;")
	if strings.Contains(ret, "doesn't exist") {
		command := "sysbench --test=oltp_read_write --tables=%s --table-size=%s --db-driver=mysql --mysql-user=root " +
			"--mysql-password='' --mysql-host=%s --mysql-port=4000 --mysql-db=test --threads=4 --mysql-ignore-errors=8028 prepare"
		command = fmt.Sprintf(command, b.tableCount, b.rowCount, b.tidbSVC)
		ret = b.execCmdOnPod(command)
		fmt.Print(ret)
	} else {
		rowCount := extractSQLResult(ret, 0, 0)
		log.Printf("Found table sbtest1(row count = %s), skip creating table.\n", rowCount[0])
	}
}

func (b *benchCtx) benchCreateIndex() {
	var jobID string

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		b.executeSQLInTestDB("alter table sbtest1 drop index idx;")
		ret := b.executeSQLInTestDB("create index idx on sbtest1(c);")
		fmt.Print(ret)
	}()

	go func() {
		defer wg.Done()
		<-time.After(1 * time.Second)
		ret := b.executeSQLInTestDB("admin show ddl jobs 1;")
		jobID = extractSQLResult(ret, 0, 0)[0]
	}()

	wg.Wait()

	ret := b.executeSQLInTestDB(fmt.Sprintf("admin show ddl jobs where job_id = %s;", jobID))
	startEnd := extractSQLResult(ret, 0, 9, 10)
	elapseTime := calculateElapseTime(startEnd[0], startEnd[1])
	log.Printf("Create index elapse time: %s\n", elapseTime.String())
}

func (b *benchCtx) benchCreateMultiIndexes() {
	var wg sync.WaitGroup
	wg.Add(b.tableCountInt)
	startTime := time.Now()
	log.Printf("Create multiple indexes begin time: %s\n", startTime.String())
	for i := 0; i < b.tableCountInt; i++ {
		go func(i int) {
			defer wg.Done()
			ret := b.executeSQLInTestDB(fmt.Sprintf("create index idx on sbtest%d(c);", i+1))
			fmt.Print(ret)
		}(i)
	}
	wg.Wait()
	log.Printf("Create multiple indexes elapse: %s\n", time.Since(startTime).String())
}

func calculateElapseTime(start, end string) time.Duration {
	template := "2006-01-02 15:04:05"
	startTime, err := time.Parse(template, start)
	if err != nil {
		panic(err)
	}
	endTime, err := time.Parse(template, end)
	if err != nil {
		panic(err)
	}
	return endTime.Sub(startTime)
}

func extractSQLResult(result string, rowIdx int, colIdxes ...int) []string {
	rows := strings.Split(result, "\n")
	str := rows[rowIdx+3] // skip header.
	cols := strings.Split(str, "|")
	ret := make([]string, 0, len(colIdxes))
	for _, idx := range colIdxes {
		ret = append(ret, strings.Trim(cols[idx+1], " "))
	}
	return ret
}

func (b *benchCtx) getRunningBenchPodName() {
	opts := metav1.ListOptions{
		TypeMeta:      metav1.TypeMeta{},
		LabelSelector: "app=bench",
		FieldSelector: "",
	}

	pods, err := b.clientset.CoreV1().Pods(b.tidbNamespace).List(context.Background(), opts)
	if err != nil {
		panic(err)
	}

	for _, pod := range pods.Items {
		if pod.Status.Phase == apiv1.PodRunning {
			b.benchName = pod.Name
			return
		}
	}
}

type podStatus int

const (
	podStatusRunning = 1
	podStatusDeleted = 2
)

func (b *benchCtx) waitPodStatus(status podStatus) {
	opts := metav1.ListOptions{
		TypeMeta:      metav1.TypeMeta{},
		LabelSelector: "app=bench",
		FieldSelector: "",
	}

	w, err := b.clientset.CoreV1().Pods(b.tidbNamespace).Watch(context.Background(), opts)
	if err != nil {
		panic(err)
	}

	defer w.Stop()

	for {
		select {
		case event := <-w.ResultChan():
			switch status {
			case podStatusDeleted:
				if event.Type == watch.Deleted {
					log.Println("The pod is deleted.")
					return
				}
			case podStatusRunning:
				pod := event.Object.(*apiv1.Pod)
				if pod.Status.Phase == apiv1.PodRunning {
					b.benchName = pod.Name
					log.Printf("The pod %s is running.", pod.Name)
					return
				}
			default:
				panic("unknown pod status")
			}
		case <-time.After(1 * time.Minute):
			log.Println("Wait for pod running timeout.")
			return
		}
	}
}

func (b *benchCtx) deleteBenchToolDeployment() {
	log.Println("Deleting deployment...")
	cli := b.clientset.AppsV1().Deployments(b.tidbNamespace)

	deletePolicy := metav1.DeletePropagationForeground
	if err := cli.Delete(context.TODO(), "bench", metav1.DeleteOptions{
		PropagationPolicy: &deletePolicy,
	}); err != nil {
		panic(err)
	}
	b.waitPodStatus(podStatusDeleted)
	log.Println("Deleted deployment.")
}

func (b *benchCtx) execCmdOnPod(command string) string {
	req := b.clientset.CoreV1().
		RESTClient().
		Post().
		Namespace(b.tidbNamespace).
		Resource("pods").
		Name(b.benchName).
		SubResource("exec").
		VersionedParams(&apiv1.PodExecOptions{
			Command: []string{"sh", "-c", command},
			Stdin:   true,
			Stdout:  true,
			Stderr:  true,
			TTY:     true,
		}, scheme.ParameterCodec)
	// log.Printf("exec namespace: %s, bench pod name: %s\n", d.tidbNamespace, d.benchName)
	exec, err := remotecommand.NewSPDYExecutor(b.config, "POST", req.URL())
	if err != nil {
		panic(err)
	}
	buf := &buffer{}
	err = exec.StreamWithContext(context.Background(), remotecommand.StreamOptions{
		Stdin:  os.Stdin,
		Stdout: buf,
		Stderr: buf,
		Tty:    true,
	})
	status := "✔"
	if err != nil {
		status = "✘"
	}
	log.Printf("exec command: %s %s", command, status)
	if err != nil {
		log.Printf("meet exec error: %s [%s]\n", err.Error(), strings.Trim(buf.String(), "\r\n"))
	}
	return buf.String()
}

var _ io.Writer = &buffer{}

type buffer struct {
	data []byte
}

func (b *buffer) Write(v []byte) (int, error) {
	b.data = append(b.data, v...)
	return len(v), nil
}

func (b *buffer) String() string {
	return string(b.data)
}

var benchToolDeployment = &appsv1.Deployment{
	ObjectMeta: metav1.ObjectMeta{
		Name: "bench",
	},
	Spec: appsv1.DeploymentSpec{
		Replicas: int32Ptr(1),
		Selector: &metav1.LabelSelector{
			MatchLabels: map[string]string{
				"app": "bench",
			},
		},
		Template: apiv1.PodTemplateSpec{
			ObjectMeta: metav1.ObjectMeta{
				Labels: map[string]string{
					"app": "bench",
				},
			},
			Spec: apiv1.PodSpec{
				Containers: []apiv1.Container{
					{
						Name:            "bench",
						Image:           "hub.pingcap.net/perf_testing/bench-toolset:latest",
						Command:         []string{"tail"},
						Args:            []string{"-f", "/dev/null"},
						ImagePullPolicy: apiv1.PullAlways,
					},
				},
			},
		},
	},
}

func int32Ptr(i int32) *int32 { return &i }
