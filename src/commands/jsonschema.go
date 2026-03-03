package commands

import (
	"errors"
	"fmt"
	"os"

	"github.com/spf13/cobra"

	"github.com/notwillk/sqlfs/internal/builder"
	"github.com/notwillk/sqlfs/internal/config"
	"github.com/notwillk/sqlfs/internal/dbml"
	"github.com/notwillk/sqlfs/internal/jsonschema"
)

var jsonSchemaCmd = &cobra.Command{
	Use:   "json-schema <root>",
	Short: "Generate a JSON Schema from schema.dbml",
	Long: `Parse schema.dbml from the root directory and output a JSON Schema
document that can be used to validate data files.`,
	Args: cobra.ExactArgs(1),
	RunE: runJSONSchema,
}

var jsonSchemaOutputFile string

func init() {
	jsonSchemaCmd.Flags().StringVarP(&jsonSchemaOutputFile, "output-file", "o", "", "Output file (default: stdout)")
}

func runJSONSchema(cmd *cobra.Command, args []string) error {
	rootDir := args[0]

	cfg, err := config.Load(rootDir)
	if err != nil {
		return fmt.Errorf("loading config: %w", err)
	}

	var data []byte

	schemaPath := rootDir + "/" + cfg.SchemaFile
	if _, statErr := os.Stat(schemaPath); errors.Is(statErr, os.ErrNotExist) {
		// Schema-less mode: infer structure from entity files.
		cols, err := builder.DiscoverColumnMap(rootDir, cfg)
		if err != nil {
			return fmt.Errorf("discovering schema: %w", err)
		}
		data, err = jsonschema.GenerateFromColumns(cols, cfg)
		if err != nil {
			return fmt.Errorf("generating JSON schema: %w", err)
		}
	} else {
		schema, err := dbml.ParseFile(schemaPath)
		if err != nil {
			return fmt.Errorf("parsing schema: %w", err)
		}
		data, err = jsonschema.Generate(schema, cfg)
		if err != nil {
			return fmt.Errorf("generating JSON schema: %w", err)
		}
	}

	if jsonSchemaOutputFile != "" {
		if err := os.WriteFile(jsonSchemaOutputFile, data, 0644); err != nil {
			return fmt.Errorf("writing output file: %w", err)
		}
		fmt.Fprintf(cmd.OutOrStdout(), "JSON schema written to %s\n", jsonSchemaOutputFile)
		return nil
	}

	_, err = cmd.OutOrStdout().Write(append(data, '\n'))
	return err
}
