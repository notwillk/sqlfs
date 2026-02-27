package commands

import (
	"fmt"
	"io"

	"github.com/spf13/cobra"

	"github.com/notwillk/sqlfs/internal/version"
)

var showVersion bool

var rootCmd = &cobra.Command{
	Use:   "sqlfs",
	Short: "Build and serve a SQLite database from static files",
	Long: `sqlfs creates a SQLite database from static data files (YAML, TOML, JSON, XML, plist)
validated against a DBML schema.`,
	SilenceUsage:  true,
	SilenceErrors: true,
	RunE: func(cmd *cobra.Command, args []string) error {
		if showVersion {
			fmt.Fprintf(cmd.OutOrStdout(), "sqlfs %s\n", version.Version)
			return nil
		}
		return cmd.Help()
	},
}

func init() {
	rootCmd.Flags().BoolVarP(&showVersion, "version", "v", false, "Print version and exit")
	rootCmd.AddCommand(buildCmd, serveCmd, jsonSchemaCmd, configSchemaCmd)
}

// Execute runs the root cobra command and returns an exit code.
func Execute(args []string, stdout, stderr io.Writer) int {
	rootCmd.SetOut(stdout)
	rootCmd.SetErr(stderr)
	rootCmd.SetArgs(args)

	if err := rootCmd.Execute(); err != nil {
		fmt.Fprintln(stderr, err)
		return 1
	}
	return 0
}
