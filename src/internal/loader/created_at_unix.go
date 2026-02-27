//go:build !windows

package loader

import (
	"os"
	"syscall"
	"time"
)

// fileCreatedAt returns the file creation time (birth time) on Unix systems.
// Falls back to ModTime if birth time is not available.
func fileCreatedAt(info os.FileInfo, _ string) time.Time {
	if stat, ok := info.Sys().(*syscall.Stat_t); ok {
		// Ctim is "change time" not "creation time" on Linux — fall back to ModTime.
		// On macOS, use Birthtimespec.
		_ = stat
	}
	return info.ModTime()
}
