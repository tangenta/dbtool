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
	if len(args) == 0 {
		cmd.SetUsageFunc(func(c *cobra.Command) error {
			fmt.Println("Usage: \n  test [48304]")
			return nil
		})
		cmd.Usage()
		return
	}
	switch args[0] {
	case "48304":
		cases.RunTest48304()
	case "50073":
		// Precondition:
		//   tiup playground nightly --db 2 --kv 1 --pd 1 --tiflash 0
		cases.RunTest50073()
	case "?":
		fmt.Println("Usage: \n  test [48304]")
	}
}
