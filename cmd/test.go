package cmd

import (
	"fmt"

	_ "github.com/go-sql-driver/mysql"
	"github.com/spf13/cobra"
	"github.com/tangenta/dbtool/cases"
)

// testCmd represents the testCmd command
var testCmd = &cobra.Command{
	Use:   "test",
	Short: "test scripts",
	Run:   runTestCmd,
}

func init() {
	rootCmd.AddCommand(testCmd)
}

func runTestCmd(cmd *cobra.Command, args []string) {
	cmd.SetUsageFunc(func(c *cobra.Command) error {
		fmt.Println("Usage: \n  test [48304|50012|50073|all]")
		return nil
	})
	if len(args) == 0 {
		cmd.Usage()
		return
	}
	switch args[0] {
	case "48304":
		cases.RunTest48304()
	case "50012":
		repeat := len(args) > 2 && args[1] == "repeat"
		cases.RunTest50012(repeat)
	case "50073":
		cases.RunTest50073()
	case "50894":
		cases.RunTest50894()
	case "50895":
		cases.RunTest50895()
	case "all":
		cases.RunTest48304()
		cases.RunTest50012(false)
		cases.RunTest50073()
	case "?":
		cmd.Usage()
	}
}
