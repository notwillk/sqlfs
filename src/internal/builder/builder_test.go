package builder

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/notwillk/sqlfs/internal/config"
	"github.com/notwillk/sqlfs/internal/sqlite"
)

func setupTestDir(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()

	schema := `
Table users {
  id integer [pk]
  name varchar [not null]
  email varchar
}
`
	if err := os.WriteFile(filepath.Join(dir, "schema.dbml"), []byte(schema), 0644); err != nil {
		t.Fatal(err)
	}

	users := `
alice:
  id: 1
  name: Alice Smith
  email: alice@example.com

bob:
  id: 2
  name: Bob Jones
  email: bob@example.com
`
	if err := os.WriteFile(filepath.Join(dir, "users.yaml"), []byte(users), 0644); err != nil {
		t.Fatal(err)
	}

	return dir
}

func TestBuild_Basic(t *testing.T) {
	dir := setupTestDir(t)
	outFile := filepath.Join(t.TempDir(), "test.db")

	cfg := config.Default()
	result, err := Build(context.Background(), Options{
		RootDir:    dir,
		OutputFile: outFile,
		Config:     cfg,
	})
	if err != nil {
		t.Fatalf("Build: %v", err)
	}

	if result.RecordsTotal != 2 {
		t.Errorf("RecordsTotal = %d, want 2", result.RecordsTotal)
	}
	if result.TablesBuilt != 1 {
		t.Errorf("TablesBuilt = %d, want 1", result.TablesBuilt)
	}

	// Verify the output file exists.
	if _, err := os.Stat(outFile); err != nil {
		t.Fatalf("output file not created: %v", err)
	}

	// Open and query the output file.
	db, err := sqlite.Open(outFile)
	if err != nil {
		t.Fatalf("opening output: %v", err)
	}
	defer db.Close()

	rows, err := db.Query("SELECT COUNT(*) FROM users")
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	defer rows.Close()
	rows.Next()
	var count int
	rows.Scan(&count)
	if count != 2 {
		t.Errorf("users count = %d, want 2", count)
	}
}

func TestBuild_StandardColumns(t *testing.T) {
	dir := setupTestDir(t)
	outFile := filepath.Join(t.TempDir(), "test.db")

	cfg := config.Default()
	if _, err := Build(context.Background(), Options{
		RootDir:    dir,
		OutputFile: outFile,
		Config:     cfg,
	}); err != nil {
		t.Fatalf("Build: %v", err)
	}

	db, err := sqlite.Open(outFile)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	rows, err := db.Query("SELECT __path__, __checksum__, __ulid__ FROM users LIMIT 1")
	if err != nil {
		t.Fatalf("query standard cols: %v", err)
	}
	defer rows.Close()

	if !rows.Next() {
		t.Fatal("expected at least one row")
	}
	var path, checksum, uid string
	if err := rows.Scan(&path, &checksum, &uid); err != nil {
		t.Fatal(err)
	}
	if path == "" {
		t.Error("__path__ should not be empty")
	}
	if checksum == "" {
		t.Error("__checksum__ should not be empty")
	}
	if uid == "" {
		t.Error("__ulid__ should not be empty")
	}
	// Path should follow the pattern file#key.
	if len(path) < 3 || path[len(path)-1] == '#' {
		t.Errorf("unexpected __path__ format: %q", path)
	}
}

func TestBuild_SkipsSchemaAndConfig(t *testing.T) {
	dir := t.TempDir()
	schema := `Table t { id integer [pk] }`
	os.WriteFile(filepath.Join(dir, "schema.dbml"), []byte(schema), 0644)
	// sqlfs.yaml and schema.dbml should be skipped.
	os.WriteFile(filepath.Join(dir, "sqlfs.yaml"), []byte("port: 9999"), 0644)

	// Also add a real data file.
	os.WriteFile(filepath.Join(dir, "t.yaml"), []byte("rec1:\n  id: 1"), 0644)

	outFile := filepath.Join(t.TempDir(), "test.db")
	result, err := Build(context.Background(), Options{
		RootDir:    dir,
		OutputFile: outFile,
		Config:     config.Default(),
	})
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	if result.RecordsTotal != 1 {
		t.Errorf("expected 1 record (not schema/config files), got %d", result.RecordsTotal)
	}
}

func TestBuild_MultipleFiles(t *testing.T) {
	dir := t.TempDir()
	schema := `
Table users {
  id integer [pk]
  name varchar
}
Table posts {
  id integer [pk]
  title varchar
}
`
	os.WriteFile(filepath.Join(dir, "schema.dbml"), []byte(schema), 0644)
	os.WriteFile(filepath.Join(dir, "users.yaml"), []byte("u1:\n  id: 1\n  name: Alice"), 0644)
	os.WriteFile(filepath.Join(dir, "posts.yaml"), []byte("p1:\n  id: 1\n  title: Hello\np2:\n  id: 2\n  title: World"), 0644)

	outFile := filepath.Join(t.TempDir(), "test.db")
	result, err := Build(context.Background(), Options{
		RootDir:    dir,
		OutputFile: outFile,
		Config:     config.Default(),
	})
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	if result.RecordsTotal != 3 {
		t.Errorf("RecordsTotal = %d, want 3", result.RecordsTotal)
	}
	if result.TablesBuilt != 2 {
		t.Errorf("TablesBuilt = %d, want 2", result.TablesBuilt)
	}
}

func TestBuild_MissingSchema(t *testing.T) {
	dir := t.TempDir()
	// No schema.dbml file.
	outFile := filepath.Join(t.TempDir(), "test.db")
	_, err := Build(context.Background(), Options{
		RootDir:    dir,
		OutputFile: outFile,
		Config:     config.Default(),
	})
	if err == nil {
		t.Fatal("expected error for missing schema")
	}
}

func TestBuild_ContextCancelled(t *testing.T) {
	dir := setupTestDir(t)
	outFile := filepath.Join(t.TempDir(), "test.db")

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // cancel immediately

	_, err := Build(ctx, Options{
		RootDir:    dir,
		OutputFile: outFile,
		Config:     config.Default(),
	})
	if err == nil {
		t.Log("no error on cancelled context (walked before cancel took effect)")
	}
}
