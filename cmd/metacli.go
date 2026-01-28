package cmd

import (
	"context"
	"flag"
	"fmt"
	"os"
	"strconv"

	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta"
	kvstore "github.com/pingcap/tidb/pkg/store"
	"github.com/pingcap/tidb/pkg/store/driver"
	"github.com/spf13/cobra"
)

type metaCliCtx struct {
	configPath string
	store      string
	path       string
	opType     string
	key        string
	value      string

	setter func(*meta.Mutator)
	getter func(*meta.Mutator) float64
}

func init() {
	ctx := &metaCliCtx{}
	var metaCliCmd = &cobra.Command{
		Use: "metacli",
		Run: runMetaCliCmd(ctx),
	}
	rootCmd.AddCommand(metaCliCmd)
	metaCliCmd.Flags().StringVar(&ctx.configPath, "config", "", "path to the tidb config file")
	metaCliCmd.Flags().StringVar(&ctx.store, "store", "tikv", "the storage type, supports tikv, unistore")
	metaCliCmd.Flags().StringVar(&ctx.path, "path", "", "the path to the storage")
	metaCliCmd.Flags().StringVar(&ctx.opType, "op", "get", "the operation type, supports get, put, delete")
	metaCliCmd.Flags().StringVar(&ctx.key, "key", "", "the key to operate")
	metaCliCmd.Flags().StringVar(&ctx.value, "value", "", "the value to put")
}

func runMetaCliCmd(ctx *metaCliCtx) func(cmd *cobra.Command, args []string) {
	return func(cmd *cobra.Command, args []string) {
		if ctx.configPath == "" {
			cmd.Usage()
			os.Exit(1)
		}
		runMetaKVChange(ctx)
	}
}

func runMetaKVChange(mCtx *metaCliCtx) {
	if !metaCliConfigIsValid(mCtx) {
		return
	}
	supressLogOutput()
	config.InitializeConfig(mCtx.configPath, false, false, func(c *config.Config, fs *flag.FlagSet) {}, nil)
	config.UpdateGlobal(func(conf *config.Config) {
		conf.Store = config.StoreType(mCtx.store)
		conf.Path = mCtx.path
	})
	cfg := config.GetGlobalConfig()
	err := kvstore.Register(config.StoreTypeTiKV, &driver.TiKVDriver{})
	mustNil(err)
	store := kvstore.MustInitStorage(cfg.KeyspaceName)
	defer store.Close()

	err = kv.RunInNewTxn(context.Background(), store, false, func(ctx context.Context, txn kv.Transaction) error {
		m := meta.NewMutator(txn)
		switch mCtx.opType {
		case "get":
			v := mCtx.getter(m)
			fmt.Printf("key %s has value %f\n", mCtx.key, v)
			return nil
		case "put", "delete":
			mCtx.setter(m)
		}
		return nil
	})
	mustNil(err)
	err = kv.RunInNewTxn(context.Background(), store, false, func(ctx context.Context, txn kv.Transaction) error {
		m := meta.NewMutator(txn)
		v := mCtx.getter(m)
		fmt.Printf("key %s has value %f\n", mCtx.key, v)
		return nil
	})
	mustNil(err)
}

func metaCliConfigIsValid(ctx *metaCliCtx) bool {
	if ctx.configPath == "" {
		fmt.Println("please provide the path to the tidb config file")
		return false
	}
	storeTp := config.StoreType(ctx.store)
	if !storeTp.Valid() {
		fmt.Printf("invalid store type %s\n", ctx.store)
		return false
	}
	if ctx.path == "" {
		fmt.Printf("please provide the path to the storage\n")
		return false
	}
	var val float64
	switch ctx.opType {
	case "get":
	case "delete":
		val = 0
	case "put":
		v, err := strconv.ParseFloat(ctx.value, 64)
		if err != nil || v < 0 {
			fmt.Printf("invalid value %s for key %s\n", ctx.value, ctx.key)
			return false
		}
		val = float64(v)
	default:
		fmt.Printf("invalid operation type %s\n", ctx.opType)
		return false
	}
	switch ctx.key {
	case "max-batch-split-ranges":
		ctx.getter = func(m *meta.Mutator) float64 {
			v, _, err := m.GetIngestMaxBatchSplitRanges()
			mustNil(err)
			return float64(v)
		}
		ctx.setter = func(m *meta.Mutator) {
			m.SetIngestMaxBatchSplitRanges(int(val))
		}
	case "max-split-ranges-per-sec":
		ctx.getter = func(m *meta.Mutator) float64 {
			v, _, err := m.GetIngestMaxSplitRangesPerSec()
			mustNil(err)
			return v
		}
		ctx.setter = func(m *meta.Mutator) {
			m.SetIngestMaxSplitRangesPerSec(val)
		}

	case "max-ingest-per-sec":
		ctx.getter = func(m *meta.Mutator) float64 {
			v, _, err := m.GetIngestMaxPerSec()
			mustNil(err)
			return v
		}
		ctx.setter = func(m *meta.Mutator) {
			m.SetIngestMaxPerSec(val)
		}
	case "max-ingest-inflight":
		ctx.getter = func(m *meta.Mutator) float64 {
			v, _, err := m.GetIngestMaxInflight()
			mustNil(err)
			return float64(v)
		}
		ctx.setter = func(m *meta.Mutator) {
			m.SetIngestMaxInflight(int(val))
		}
	default:
		fmt.Printf("invalid key %s\n", ctx.key)
		return false
	}
	return true
}
