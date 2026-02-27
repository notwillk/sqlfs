package commands

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/spf13/cobra"

	"github.com/notwillk/sqlfs/internal/builder"
	"github.com/notwillk/sqlfs/internal/config"
	"github.com/notwillk/sqlfs/internal/pgserver"
	"github.com/notwillk/sqlfs/internal/watcher"
)

var serveCmd = &cobra.Command{
	Use:   "serve <root>",
	Short: "Build and serve a SQLite database via PostgreSQL wire protocol",
	Long: `Parse schema.dbml, build a SQLite database, and serve it as a
read-only PostgreSQL-compatible server. Watches for file changes and rebuilds.`,
	Args: cobra.ExactArgs(1),
	RunE: runServe,
}

var serveOutputFile string
var serveInvalid string
var servePort int

func init() {
	serveCmd.Flags().StringVarP(&serveOutputFile, "output-file", "o", "", "Database file path (required)")
	serveCmd.Flags().StringVar(&serveInvalid, "invalid", "", "Behavior on validation failure: silent, warn (default: warn)")
	serveCmd.Flags().IntVar(&servePort, "port", 0, "Port to serve on (default: 5432)")
	serveCmd.MarkFlagRequired("output-file")
}

func runServe(cmd *cobra.Command, args []string) error {
	rootDir := args[0]

	cfg, err := config.Load(rootDir)
	if err != nil {
		return fmt.Errorf("loading config: %w", err)
	}

	// serve defaults to 'warn' for invalid behavior (unlike build which defaults to 'fail').
	if serveInvalid == "" && cfg.Invalid == config.InvalidFail {
		cfg = cfg.WithInvalid("warn")
	} else {
		cfg = cfg.WithInvalid(serveInvalid)
	}
	cfg = cfg.WithPort(servePort)

	// Initial build.
	fmt.Fprintf(cmd.OutOrStdout(), "Building database...\n")
	buildResult, err := builder.Build(context.Background(), builder.Options{
		RootDir:    rootDir,
		OutputFile: serveOutputFile,
		Config:     cfg,
	})
	if err != nil {
		return fmt.Errorf("initial build: %w", err)
	}
	for _, w := range buildResult.Warnings {
		fmt.Fprintln(os.Stderr, "warning:", w.Error())
	}
	fmt.Fprintf(cmd.OutOrStdout(), "Built %d records in %s\n", buildResult.RecordsTotal, buildResult.Duration)

	// Resolve credentials from environment.
	username := os.Getenv(cfg.UsernameEnvVar)
	password := os.Getenv(cfg.PasswordEnvVar)

	// Start PostgreSQL server.
	srv, err := pgserver.New(pgserver.Options{
		Port:     cfg.Port,
		DBPath:   serveOutputFile,
		Username: username,
		Password: password,
	})
	if err != nil {
		return fmt.Errorf("creating server: %w", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Handle SIGINT/SIGTERM.
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigCh
		fmt.Fprintln(cmd.OutOrStdout(), "\nShutting down...")
		cancel()
	}()

	// Start server.
	serverDone := make(chan error, 1)
	go func() {
		serverDone <- srv.Serve(ctx)
	}()

	fmt.Fprintf(cmd.OutOrStdout(), "Serving on port %d (press Ctrl+C to stop)\n", cfg.Port)

	// Set up file watcher.
	watcherDone := make(chan error, 1)
	w, err := watcher.New(rootDir, 300*time.Millisecond, func(wctx context.Context) error {
		fmt.Fprintln(cmd.OutOrStdout(), "Change detected, rebuilding...")
		tmpFile := serveOutputFile + ".tmp"
		result, err := builder.Build(wctx, builder.Options{
			RootDir:    rootDir,
			OutputFile: tmpFile,
			Config:     cfg,
		})
		if err != nil {
			fmt.Fprintf(os.Stderr, "rebuild error: %v\n", err)
			return err
		}
		for _, w := range result.Warnings {
			fmt.Fprintln(os.Stderr, "warning:", w.Error())
		}
		// Atomically swap the file.
		if err := os.Rename(tmpFile, serveOutputFile); err != nil {
			return fmt.Errorf("swapping database: %w", err)
		}
		// Reload the server.
		if err := srv.Reload(serveOutputFile); err != nil {
			return fmt.Errorf("reloading server: %w", err)
		}
		fmt.Fprintf(cmd.OutOrStdout(), "Rebuilt %d records\n", result.RecordsTotal)
		return nil
	})
	if err != nil {
		return fmt.Errorf("creating watcher: %w", err)
	}
	defer w.Close()

	go func() {
		watcherDone <- w.Start(ctx)
	}()

	// Wait for either server or watcher to finish.
	select {
	case err := <-serverDone:
		return err
	case err := <-watcherDone:
		return err
	case <-ctx.Done():
		return nil
	}
}
