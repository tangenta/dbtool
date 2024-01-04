package cmd

import (
	"encoding/hex"
	"encoding/json"
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
}

func runDecodeCmd(cmd *cobra.Command, args []string) {
	if len(args) == 0 {
		cmd.SetUsageFunc(func(c *cobra.Command) error {
			fmt.Println("Usage: \n  decode key <hex string>, [<hex string>...] [flags]\n   decode stat <stat string>")
			return nil
		})
		cmd.Usage()
		return
	}
	switch args[0] {
	case "key":
		for _, arg := range args[1:] {
			fmt.Printf("%s\n", decodeKey(arg))
		}
	case "stat":
		for _, arg := range args[1:] {
			printDecodedStat(arg)
		}
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

func printDecodedStat(data string) {
	var v interface{}
	err := json.Unmarshal([]byte(data), &v)
	if err != nil {
		panic(err)
	}
	m := v.(map[string]interface{})
	total := m["add-index-job"].(float64)
	fmt.Printf("%.3f\n", total)
	read := extractOneOfSum(m["op-scan-records-1"], m["scan-records-1"])
	wait := extractOneOfSum(m["op-send-chunk-1"], m["send-chunk-1"])
	write := extractOneOfSum(m["op-write-local-1"], m["write-local-1"])
	fmt.Printf("%.3f [%.3f] / %.3f\n", read, wait, write)
	ingest := extractOneOfSum(m["op-result-collect-flush"], m["finish-import-1"], m["finish-import-2"], m["finish-import-3"])
	fmt.Printf("%.3f\n", ingest)
}

func extractOneOfSum(vs ...interface{}) float64 {
	for _, v := range vs {
		if v == nil {
			continue
		}
		return extractSum(v)
	}
	panic(fmt.Sprintf("no sum found, tried: %v", vs))
}

func extractSum(v interface{}) float64 {
	switch x := v.(type) {
	case map[string]interface{}:
		return x["sum"].(float64)
	case float64:
		return x
	}
	panic(fmt.Sprintf("no sum found, tried: %v", v))
}
