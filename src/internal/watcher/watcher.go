package watcher

import (
	"context"
	"log"
	"os"
	"path/filepath"
	"time"

	"github.com/fsnotify/fsnotify"
)

// RebuildFn is called whenever watched files change.
type RebuildFn func(ctx context.Context) error

// Watcher watches a directory tree for changes and calls RebuildFn with debouncing.
type Watcher struct {
	rootDir  string
	debounce time.Duration
	fn       RebuildFn
	fw       *fsnotify.Watcher
}

// New creates a new Watcher.
func New(rootDir string, debounce time.Duration, fn RebuildFn) (*Watcher, error) {
	fw, err := fsnotify.NewWatcher()
	if err != nil {
		return nil, err
	}
	w := &Watcher{
		rootDir:  rootDir,
		debounce: debounce,
		fn:       fn,
		fw:       fw,
	}
	if err := w.addAll(rootDir); err != nil {
		fw.Close()
		return nil, err
	}
	return w, nil
}

// Start begins watching and blocks until ctx is cancelled.
func (w *Watcher) Start(ctx context.Context) error {
	defer w.fw.Close()

	// Use a stopped timer so we can reset it on events.
	timer := time.NewTimer(0)
	if !timer.Stop() {
		<-timer.C
	}
	pending := false

	for {
		select {
		case <-ctx.Done():
			return nil

		case event, ok := <-w.fw.Events:
			if !ok {
				return nil
			}
			// If a new directory was created, watch it too.
			if event.Has(fsnotify.Create) {
				if info, err := os.Stat(event.Name); err == nil && info.IsDir() {
					w.addAll(event.Name) //nolint:errcheck
				}
			}
			// Reset debounce timer.
			if pending {
				timer.Stop()
				// Drain channel if it already fired.
				select {
				case <-timer.C:
				default:
				}
			}
			timer.Reset(w.debounce)
			pending = true

		case <-timer.C:
			pending = false
			if err := w.fn(ctx); err != nil {
				log.Printf("watcher: rebuild error: %v", err)
			}

		case err, ok := <-w.fw.Errors:
			if !ok {
				return nil
			}
			log.Printf("watcher: fsnotify error: %v", err)
		}
	}
}

// Close stops the watcher.
func (w *Watcher) Close() error {
	return w.fw.Close()
}

// addAll adds the directory and all its subdirectories to the watcher.
func (w *Watcher) addAll(dir string) error {
	return filepath.WalkDir(dir, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return w.fw.Add(path)
		}
		return nil
	})
}
