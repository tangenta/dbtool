package util

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"time"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/kubernetes/pkg/client/conditions"
)

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

func BuildClientSetFromCfg(kubeCfgPath string) (*rest.Config, *kubernetes.Clientset) {
	config, err := clientcmd.BuildConfigFromFlags("", kubeCfgPath)
	if err != nil {
		panic(err)
	}
	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		panic(err)
	}
	log.Println("Build clientset from kube config success.")
	return config, clientset
}

func isAllPodsReady(c kubernetes.Interface) wait.ConditionWithContextFunc {
	return func(ctx context.Context) (bool, error) {
		fmt.Printf(".")

		podList, err := c.CoreV1().Pods("tidb-cluster").List(context.TODO(), metav1.ListOptions{})
		if err != nil {
			return false, err
		}

		for _, p := range podList.Items {
			log.Printf("%s: %s", p.Name, p.Status.Phase)
			switch p.Status.Phase {
			case v1.PodRunning:
				continue
			case v1.PodFailed, v1.PodSucceeded:
				return false, conditions.ErrPodCompleted
			}
		}
		fmt.Println()
		log.Println("all pods are ready!")
		return true, nil
	}
}

func waitForPodRunning(c kubernetes.Interface, timeout time.Duration) error {
	return wait.PollUntilContextTimeout(context.TODO(), time.Second, timeout, true, isAllPodsReady(c))
}
