package main

import (
	"fmt"
	"io"
	"os"

	"github.com/notwillk/sqlfs/internal/version"
)

func run(args []string, stdout io.Writer) int {
	if len(args) > 0 && (args[0] == "--version" || args[0] == "-v") {
		fmt.Fprintf(stdout, "sqlfs %s\n", version.Version)
		return 0
	}
	fmt.Fprintf(stdout, "sqlfs %s\n\nUsage: sqlfs [--version]\n", version.Version)
	return 0
}

func main() {
	os.Exit(run(os.Args[1:], os.Stdout))
}
