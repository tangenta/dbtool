package cmd

import (
	"encoding/hex"
	"fmt"
	"strings"

	"github.com/spf13/cobra"
)

// decodeCmd represents the decode command
var decodeCmd = &cobra.Command{
	Use: "decode",
	Run: runDecodeCmd,
}

func init() {
	rootCmd.AddCommand(decodeCmd)

	// Here you will define your flags and configuration settings.

	// Cobra supports Persistent Flags which will work for this command
	// and all subcommands, e.g.:
	// decodeCmd.PersistentFlags().String("foo", "", "A help for foo")

	// Cobra supports local flags which will only run when this command
	// is called directly, e.g.:
	// decodeCmd.Flags().BoolP("toggle", "t", false, "Help message for toggle")
}

func runDecodeCmd(cmd *cobra.Command, args []string) {
	if len(args) == 0 {
		cmd.SetUsageFunc(func(c *cobra.Command) error {
			fmt.Println("Usage: \n  decode <hex string>, [<hex string>...] [flags]")
			return nil
		})
		cmd.Usage()
		return
	}
	for _, arg := range args {
		fmt.Printf("%s\n", decodeKey(arg))
	}
}

func decodeKey(hexStr string) string {
	v, err := hex.DecodeString(hexStr)
	if err != nil {
		panic(err)
	}
	sb := strings.Builder{}
	sb.WriteByte(byte('['))
	for i, r := range v {
		sb.WriteString(fmt.Sprintf("%d", r))
		if i < len(v)-1 {
			sb.WriteByte(byte(','))
		}
	}
	sb.WriteByte(byte(']'))
	return sb.String()
}
