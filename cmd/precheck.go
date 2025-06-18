package cmd

import (
	"context"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/pingcap/log"
	"github.com/pingcap/tidb/pkg/ddl/schematracker"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/format"
	_ "github.com/pingcap/tidb/pkg/planner/core"
	"github.com/pingcap/tidb/pkg/planner/core/resolve"
	_ "github.com/pingcap/tidb/pkg/types/parser_driver"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/collate"
	"github.com/pingcap/tidb/pkg/util/mock"
	"github.com/pingcap/tidb/pkg/util/sqlexec"
	"github.com/spf13/cobra"
)

// precheckCmd represents the precheck command

type precheckCtx struct {
	filePath         string
	collationEnabled bool
	verbose          bool
}

func init() {
	ctx := &precheckCtx{}
	var precheckCmd = &cobra.Command{
		Use: "precheck",
		Run: runPrecheckCmd(ctx),
	}
	rootCmd.AddCommand(precheckCmd)
	precheckCmd.Flags().StringVarP(&ctx.filePath, "file", "f", "", "path to the SQL file to precheck")
	precheckCmd.Flags().BoolVar(&ctx.collationEnabled, "new-collation", true, "whether the new collation feature is enabled")
	precheckCmd.Flags().BoolVarP(&ctx.verbose, "verbose", "v", false, "print verbose output")
}

func runPrecheckCmd(ctx *precheckCtx) func(cmd *cobra.Command, args []string) {
	return func(cmd *cobra.Command, args []string) {
		cmd.SetUsageFunc(func(c *cobra.Command) error {
			fmt.Println("Usage: \n  precheck [flags]")
			return nil
		})
		precheckSQLFile(ctx)
	}
}

func precheckSQLFile(ctx *precheckCtx) {
	supressLogOutput()

	var content []byte
	if len(ctx.filePath) > 0 {
		content = readFile(ctx.filePath)
	} else {
		var err error
		fmt.Printf("Please input the SQL statements (end with Ctrl+D):\n")
		content, err = io.ReadAll(os.Stdin)
		printErrAndExit(err)
	}

	stmts := parseContent(string(content))

	checkStatements(stmts)

	collate.SetNewCollationEnabledForTest(ctx.collationEnabled)
	tracker := schematracker.NewSchemaTracker(0)
	sessCtx := &mockCtx{mock.NewContext()}
	isLossyChange := false
	for _, stmt := range stmts {
		isAlterTable := false
		switch v := stmt.(type) {
		case *ast.CreateDatabaseStmt:
			err := tracker.CreateSchema(sessCtx, v)
			printErrAndExit(err)
		case *ast.CreateTableStmt:
			err := tracker.CreateTable(sessCtx, v)
			printErrAndExit(err)
		case *ast.AlterTableStmt:
			isAlterTable = true
			err := tracker.AlterTable(context.Background(), sessCtx, v)
			printErrAndExit(err)
			isLossyChange = tracker.Job.CtxVars[0].(bool)
		default:
			printErrAndExit(fmt.Errorf("Unsupported statement type: %T", v))
		}
		if ctx.verbose {
			builder := strings.Builder{}
			stmt.Restore(format.NewRestoreCtx(format.DefaultRestoreFlags, &builder))
			fmt.Printf("Ok: %v\n", builder.String())
			if isAlterTable {
				fmt.Printf("Lossy change: %v\n", isLossyChange)
			}
		}
	}
	if !ctx.verbose {
		output := 0
		if isLossyChange {
			output = 1
		}
		fmt.Printf("%d", output)
	}
}

func supressLogOutput() {
	conf := new(log.Config)
	conf.Level = "error"
	lg, p, err := log.InitLogger(conf)
	printErrAndExit(err)
	log.ReplaceGlobals(lg, p)
}

func readFile(filePath string) []byte {
	file, err := os.Open(filePath)
	printErrAndExit(err)
	defer file.Close()

	content, err := os.ReadFile(filePath)
	printErrAndExit(err)
	return content
}

func parseContent(content string) []ast.StmtNode {
	stmts, _, err := parser.New().Parse(string(content), "", "")
	printErrAndExit(err)
	return stmts
}

func checkStatements(stmts []ast.StmtNode) {
	if len(stmts) == 0 {
		printErrAndExit(fmt.Errorf("No statements found"))
	}
	if _, ok := stmts[len(stmts)-1].(*ast.AlterTableStmt); !ok {
		printErrAndExit(fmt.Errorf("The last statement must be an ALTER TABLE statement"))
	}
}

func printErrAndExit(err error) {
	if err != nil {
		fmt.Printf("Error: %s\n", err)
		os.Exit(1)
	}
}

type mockCtx struct {
	*mock.Context
}

func (m *mockCtx) GetRestrictedSQLExecutor() sqlexec.RestrictedSQLExecutor {
	return m
}

func (m *mockCtx) ExecRestrictedSQL(
	ctx context.Context,
	opts []sqlexec.OptionFuncAlias,
	sql string,
	args ...any,
) ([]chunk.Row, []*resolve.ResultField, error) {
	return nil, nil, nil
}
