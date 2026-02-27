package commands

import (
	"context"
	"fmt"
	"os"

	"github.com/spf13/cobra"

	"github.com/notwillk/sqlfs/internal/builder"
	"github.com/notwillk/sqlfs/internal/config"
)

var buildCmd = &cobra.Command{
	Use:   "build <root>",
	Short: "Build a SQLite database from static files",
	Long: `Parse schema.dbml and populate a SQLite database with all supported
static files in the root directory.`,
	Args: cobra.ExactArgs(1),
	RunE: runBuild,
}

var buildOutputFile string
var buildInvalid string

func init() {
	buildCmd.Flags().StringVarP(&buildOutputFile, "output-file", "o", "", "Output database file (required)")
	buildCmd.Flags().StringVar(&buildInvalid, "invalid", "", "Behavior on validation failure: silent, warn, fail (default: fail)")
	buildCmd.MarkFlagRequired("output-file")
}

func runBuild(cmd *cobra.Command, args []string) error {
	rootDir := args[0]

	cfg, err := config.Load(rootDir)
	if err != nil {
		return fmt.Errorf("loading config: %w", err)
	}
	cfg = cfg.WithInvalid(buildInvalid)

	result, err := builder.Build(context.Background(), builder.Options{
		RootDir:    rootDir,
		OutputFile: buildOutputFile,
		Config:     cfg,
	})
	if err != nil {
		return err
	}

	for _, w := range result.Warnings {
		fmt.Fprintln(os.Stderr, "warning:", w.Error())
	}

	fmt.Fprintf(cmd.OutOrStdout(), "Built %d records across %d tables in %s\n",
		result.RecordsTotal, result.TablesBuilt, result.Duration)
	return nil
}
