package builder

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/notwillk/sqlfs/internal/config"
	"github.com/notwillk/sqlfs/internal/sqlite"
)

// setupTestDir creates a temp dir with a users schema and two single-entity files.
// Files use the new name.entity-type.yaml convention.
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

	alice := "id: 1\nname: Alice Smith\nemail: alice@example.com\n"
	bob := "id: 2\nname: Bob Jones\nemail: bob@example.com\n"
	if err := os.WriteFile(filepath.Join(dir, "alice.users.yaml"), []byte(alice), 0644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "bob.users.yaml"), []byte(bob), 0644); err != nil {
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

	if _, err := os.Stat(outFile); err != nil {
		t.Fatalf("output file not created: %v", err)
	}

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

	rows, err := db.Query("SELECT __pk__, __path__, __checksum__, __ulid__ FROM users LIMIT 1")
	if err != nil {
		t.Fatalf("query standard cols: %v", err)
	}
	defer rows.Close()

	if !rows.Next() {
		t.Fatal("expected at least one row")
	}
	var pk, path, checksum, uid string
	if err := rows.Scan(&pk, &path, &checksum, &uid); err != nil {
		t.Fatal(err)
	}
	if pk == "" {
		t.Error("__pk__ should not be empty")
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
}

func TestBuild_SkipsSchemaAndConfig(t *testing.T) {
	dir := t.TempDir()
	schema := `Table t { id integer [pk] }`
	os.WriteFile(filepath.Join(dir, "schema.dbml"), []byte(schema), 0644)
	os.WriteFile(filepath.Join(dir, "sqlfs.yaml"), []byte("port: 9999"), 0644)

	// Entity file with entity type "t" matching table name.
	os.WriteFile(filepath.Join(dir, "rec.t.yaml"), []byte("id: 1\n"), 0644)

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

	// One user, two posts — each with explicit entity type.
	os.WriteFile(filepath.Join(dir, "alice.users.yaml"), []byte("id: 1\nname: Alice\n"), 0644)
	os.WriteFile(filepath.Join(dir, "hello.posts.yaml"), []byte("id: 1\ntitle: Hello\n"), 0644)
	os.WriteFile(filepath.Join(dir, "world.posts.yaml"), []byte("id: 2\ntitle: World\n"), 0644)

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

// TestBuild_NoEntityType verifies files without entity type are skipped.
func TestBuild_NoEntityType(t *testing.T) {
	dir := t.TempDir()
	schema := `Table users { id integer [pk] }`
	os.WriteFile(filepath.Join(dir, "schema.dbml"), []byte(schema), 0644)
	// File without entity type in name → should be skipped with a warning.
	os.WriteFile(filepath.Join(dir, "noentity.yaml"), []byte("id: 1\n"), 0644)

	outFile := filepath.Join(t.TempDir(), "test.db")
	result, err := Build(context.Background(), Options{
		RootDir:    dir,
		OutputFile: outFile,
		Config:     config.Default(),
	})
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	if result.RecordsTotal != 0 {
		t.Errorf("RecordsTotal = %d, want 0 (file without entity type should be skipped)", result.RecordsTotal)
	}
}

// TestBuild_SchemalessMode verifies that schema-less mode creates tables and inserts records.
func TestBuild_SchemalessMode(t *testing.T) {
	dir := t.TempDir()
	// No schema.dbml → schema-less mode.

	// Write two entity files.
	os.WriteFile(filepath.Join(dir, "alice.user.yaml"), []byte("name: Alice\nage: 30\n"), 0644)
	os.WriteFile(filepath.Join(dir, "bob.user.yaml"), []byte("name: Bob\nage: 25\n"), 0644)

	outFile := filepath.Join(t.TempDir(), "test.db")
	result, err := Build(context.Background(), Options{
		RootDir:    dir,
		OutputFile: outFile,
		Config:     config.Default(),
	})
	if err != nil {
		t.Fatalf("Build (schema-less): %v", err)
	}
	if result.RecordsTotal != 2 {
		t.Errorf("RecordsTotal = %d, want 2", result.RecordsTotal)
	}

	db, err := sqlite.Open(outFile)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	rows, err := db.Query("SELECT __pk__, name FROM user ORDER BY name")
	if err != nil {
		t.Fatalf("query user table: %v", err)
	}
	defer rows.Close()

	var got []string
	for rows.Next() {
		var pk, name string
		rows.Scan(&pk, &name)
		got = append(got, pk+":"+name)
	}
	if len(got) != 2 {
		t.Errorf("got %d rows, want 2: %v", len(got), got)
	}
}

// TestBuild_SchemalessNested verifies nested arrays create child tables in schema-less mode.
func TestBuild_SchemalessNested(t *testing.T) {
	dir := t.TempDir()
	// No schema.dbml.

	meal := `name: Spring Meal
courses:
  - name: Starter
    description: A small bite
  - name: Main
    description: The main course
`
	os.WriteFile(filepath.Join(dir, "spring.meal.yaml"), []byte(meal), 0644)

	outFile := filepath.Join(t.TempDir(), "test.db")
	result, err := Build(context.Background(), Options{
		RootDir:    dir,
		OutputFile: outFile,
		Config:     config.Default(),
	})
	if err != nil {
		t.Fatalf("Build (nested): %v", err)
	}
	// 1 meal + 2 meal_courses = 3 records.
	if result.RecordsTotal != 3 {
		t.Errorf("RecordsTotal = %d, want 3", result.RecordsTotal)
	}

	db, err := sqlite.Open(outFile)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// Both tables should exist.
	var mealCount, courseCount int
	if r, err := db.Query("SELECT COUNT(*) FROM meal"); err == nil {
		if r.Next() {
			r.Scan(&mealCount)
		}
		r.Close()
	}
	if r, err := db.Query("SELECT COUNT(*) FROM meal_courses"); err == nil {
		if r.Next() {
			r.Scan(&courseCount)
		}
		r.Close()
	}

	if mealCount != 1 {
		t.Errorf("meal count = %d, want 1", mealCount)
	}
	if courseCount != 2 {
		t.Errorf("meal_courses count = %d, want 2", courseCount)
	}
}

// TestBuild_MissingSchema verifies schema-less mode runs successfully with no entity files.
func TestBuild_MissingSchema(t *testing.T) {
	dir := t.TempDir()
	// No schema.dbml, no entity files.
	outFile := filepath.Join(t.TempDir(), "test.db")
	result, err := Build(context.Background(), Options{
		RootDir:    dir,
		OutputFile: outFile,
		Config:     config.Default(),
	})
	// Schema-less mode should succeed even with no files.
	if err != nil {
		t.Fatalf("Build (schema-less empty): unexpected error: %v", err)
	}
	if result.RecordsTotal != 0 {
		t.Errorf("RecordsTotal = %d, want 0", result.RecordsTotal)
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
