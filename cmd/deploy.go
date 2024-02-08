package cmd

import (
	"fmt"

	"github.com/spf13/cobra"
	"github.com/tangenta/dbtool/util"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

type deployCtx struct {
	kubeCfgPath string

	clientset *kubernetes.Clientset
	config    *rest.Config
}

func init() {
	var ctx deployCtx
	var deployCmd = &cobra.Command{
		Use: "deploy",
		Run: runDeployCmd(&ctx),
	}
	deployCmd.Flags().StringVar(&ctx.kubeCfgPath, "kubecfg", "./kubeconfig.yml", "Set the path of kube config file.")
	rootCmd.AddCommand(deployCmd)
}

func runDeployCmd(ctx *deployCtx) func(cmd *cobra.Command, args []string) {
	return func(cmd *cobra.Command, args []string) {
		if len(args) != 1 {
			cmd.Usage()
			return
		}
		ctx.init()
		version := args[0]
		fmt.Println(version)
	}
}

func (d *deployCtx) init() {
	d.config, d.clientset = util.BuildClientSetFromCfg(d.kubeCfgPath)
}
