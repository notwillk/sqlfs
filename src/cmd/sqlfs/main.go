package main

import (
	"os"

	"github.com/notwillk/sqlfs/commands"
)

func main() {
	os.Exit(commands.Execute(os.Args[1:], os.Stdout, os.Stderr))
}
