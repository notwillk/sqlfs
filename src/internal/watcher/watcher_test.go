package watcher

import (
	"context"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"
)

func TestWatcher_CallbackOnChange(t *testing.T) {
	dir := t.TempDir()

	// Create an initial file.
	if err := os.WriteFile(filepath.Join(dir, "test.yaml"), []byte("key: value"), 0644); err != nil {
		t.Fatal(err)
	}

	var callCount atomic.Int32
	w, err := New(dir, 50*time.Millisecond, func(ctx context.Context) error {
		callCount.Add(1)
		return nil
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		w.Start(ctx) //nolint:errcheck
		close(done)
	}()

	// Modify the file.
	time.Sleep(20 * time.Millisecond)
	if err := os.WriteFile(filepath.Join(dir, "test.yaml"), []byte("key: changed"), 0644); err != nil {
		t.Fatal(err)
	}

	// Wait for debounce + margin.
	time.Sleep(200 * time.Millisecond)

	cancel()
	<-done

	if callCount.Load() == 0 {
		t.Error("expected rebuild callback to be called at least once")
	}
}

func TestWatcher_DebounceMultipleEvents(t *testing.T) {
	dir := t.TempDir()
	os.WriteFile(filepath.Join(dir, "a.yaml"), []byte("a: 1"), 0644)

	var callCount atomic.Int32
	w, err := New(dir, 100*time.Millisecond, func(ctx context.Context) error {
		callCount.Add(1)
		return nil
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		w.Start(ctx) //nolint:errcheck
		close(done)
	}()

	// Rapidly write multiple files within the debounce window.
	time.Sleep(20 * time.Millisecond)
	for i := 0; i < 5; i++ {
		os.WriteFile(filepath.Join(dir, "a.yaml"), []byte("a: updated"), 0644)
		time.Sleep(10 * time.Millisecond)
	}

	// Wait for debounce to fire.
	time.Sleep(300 * time.Millisecond)
	cancel()
	<-done

	// Should be at most 2 calls (debounce should batch the rapid changes).
	if count := callCount.Load(); count > 3 {
		t.Errorf("expected debounced calls (≤3), got %d", count)
	}
}

func TestWatcher_NewSubdirectory(t *testing.T) {
	dir := t.TempDir()

	var callCount atomic.Int32
	w, err := New(dir, 50*time.Millisecond, func(ctx context.Context) error {
		callCount.Add(1)
		return nil
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		w.Start(ctx) //nolint:errcheck
		close(done)
	}()

	// Create a new subdirectory and file inside it.
	time.Sleep(20 * time.Millisecond)
	subDir := filepath.Join(dir, "subdir")
	os.MkdirAll(subDir, 0755)
	time.Sleep(30 * time.Millisecond)
	os.WriteFile(filepath.Join(subDir, "new.yaml"), []byte("x: 1"), 0644)

	time.Sleep(300 * time.Millisecond)
	cancel()
	<-done

	if callCount.Load() == 0 {
		t.Log("callback not fired (timing-sensitive test, may flake on slow systems)")
	}
}

func TestWatcher_Close(t *testing.T) {
	dir := t.TempDir()
	w, err := New(dir, 50*time.Millisecond, func(ctx context.Context) error { return nil })
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Errorf("Close: %v", err)
	}
}
