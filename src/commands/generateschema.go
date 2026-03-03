package commands

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/spf13/cobra"

	"github.com/notwillk/sqlfs/internal/builder"
	"github.com/notwillk/sqlfs/internal/config"
)

var generateSchemaCmd = &cobra.Command{
	Use:   "generate-schema <root>",
	Short: "Generate a schema.dbml from entity files",
	Long: `Walk the entity files in <root> and produce a schema.dbml that describes
the discovered tables and columns. All columns are typed as varchar; add
proper types and constraints by hand once the file is generated.

The file is written to <root>/schema.dbml by default. Use --output to
specify a different path, or --force to overwrite an existing file.`,
	Args: cobra.ExactArgs(1),
	RunE: runGenerateSchema,
}

var generateSchemaOutput string
var generateSchemaForce bool

func init() {
	generateSchemaCmd.Flags().StringVarP(&generateSchemaOutput, "output", "o", "", "Output path (default: <root>/schema.dbml)")
	generateSchemaCmd.Flags().BoolVar(&generateSchemaForce, "force", false, "Overwrite existing schema file")
}

func runGenerateSchema(cmd *cobra.Command, args []string) error {
	rootDir := args[0]

	cfg, err := config.Load(rootDir)
	if err != nil {
		return fmt.Errorf("loading config: %w", err)
	}

	outPath := generateSchemaOutput
	if outPath == "" {
		outPath = filepath.Join(rootDir, cfg.SchemaFile)
	}

	if !generateSchemaForce {
		if _, err := os.Stat(outPath); !errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("%s already exists; use --force to overwrite", outPath)
		}
	}

	dbml, err := builder.GenerateSchema(builder.GenerateSchemaOptions{
		RootDir: rootDir,
		Config:  cfg,
	})
	if err != nil {
		return err
	}

	if err := os.WriteFile(outPath, []byte(dbml), 0644); err != nil {
		return fmt.Errorf("writing %s: %w", outPath, err)
	}

	fmt.Fprintf(cmd.OutOrStdout(), "Wrote %s\n", outPath)
	return nil
}
