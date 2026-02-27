package commands

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"

	"github.com/notwillk/sqlfs/internal/jsonschema"
)

var configSchemaCmd = &cobra.Command{
	Use:   "config-schema",
	Short: "Generate a JSON Schema for sqlfs.yaml",
	Long:  `Output a JSON Schema document that validates the sqlfs.yaml configuration file.`,
	Args:  cobra.NoArgs,
	RunE:  runConfigSchema,
}

var configSchemaOutputFile string

func init() {
	configSchemaCmd.Flags().StringVarP(&configSchemaOutputFile, "output-file", "o", "", "Output file (default: stdout)")
}

func runConfigSchema(cmd *cobra.Command, args []string) error {
	data, err := jsonschema.GenerateConfigSchema()
	if err != nil {
		return fmt.Errorf("generating config schema: %w", err)
	}

	if configSchemaOutputFile != "" {
		if err := os.WriteFile(configSchemaOutputFile, data, 0644); err != nil {
			return fmt.Errorf("writing output file: %w", err)
		}
		fmt.Fprintf(cmd.OutOrStdout(), "Config schema written to %s\n", configSchemaOutputFile)
		return nil
	}

	_, err = cmd.OutOrStdout().Write(append(data, '\n'))
	return err
}
