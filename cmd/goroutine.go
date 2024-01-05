package cmd

import (
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/spf13/cobra"
)

// testCmd represents the testCmd command
var goroutineCmd = &cobra.Command{
	Use:   "goroutine",
	Short: "display goroutines of a TiDB process",
	Run:   runGoroutineCmd,
}

func init() {
	rootCmd.AddCommand(goroutineCmd)
}

func runGoroutineCmd(cmd *cobra.Command, args []string) {
	cmd.SetUsageFunc(func(c *cobra.Command) error {
		fmt.Println("Usage: \n  goroutine http://127.0.0.1:10080")
		return nil
	})
	if len(args) == 0 {
		cmd.Usage()
		return
	}
	addr := args[0]
	if !strings.HasPrefix(addr, "http://") {
		addr = "http://" + addr
	}
	addr += "/debug/pprof/goroutine?debug=2"

	resp, err := http.Get(addr)
	mustNil(err)
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	mustNil(err)
	fmt.Println(string(body))
}

func mustNil(err error) {
	if err != nil {
		fmt.Printf("Panic error: %s\n", err.Error())
		panic(err)
	}
}
