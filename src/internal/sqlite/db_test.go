package sqlite

import (
	"testing"
)

func TestOpenMemory(t *testing.T) {
	db, err := OpenMemory()
	if err != nil {
		t.Fatalf("OpenMemory: %v", err)
	}
	defer db.Close()

	if err := db.Exec("SELECT 1"); err != nil {
		t.Fatalf("Exec SELECT 1: %v", err)
	}
}

func TestExecDDL(t *testing.T) {
	db, err := OpenMemory()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	ddl := []string{
		`CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL)`,
		`CREATE INDEX idx_users_name ON users (name)`,
	}
	if err := db.ExecDDL(ddl); err != nil {
		t.Fatalf("ExecDDL: %v", err)
	}
}

func TestInsertRecord(t *testing.T) {
	db, err := OpenMemory()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	if err := db.ExecDDL([]string{`CREATE TABLE users (id INTEGER, name TEXT, __path__ TEXT)`}); err != nil {
		t.Fatal(err)
	}

	if err := db.InsertRecord("users", []string{"id", "name", "__path__"}, []any{1, "Alice", "users.yaml#alice"}); err != nil {
		t.Fatalf("InsertRecord: %v", err)
	}

	rows, err := db.Query("SELECT id, name, __path__ FROM users")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()

	if !rows.Next() {
		t.Fatal("expected 1 row")
	}
	var id int
	var name, path string
	if err := rows.Scan(&id, &name, &path); err != nil {
		t.Fatal(err)
	}
	if id != 1 || name != "Alice" || path != "users.yaml#alice" {
		t.Errorf("got id=%d name=%q path=%q", id, name, path)
	}
}

func TestInsertRecord_MultipleRows(t *testing.T) {
	db, err := OpenMemory()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	if err := db.ExecDDL([]string{`CREATE TABLE t (id INTEGER, val TEXT)`}); err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 5; i++ {
		if err := db.InsertRecord("t", []string{"id", "val"}, []any{i, "v"}); err != nil {
			t.Fatalf("insert %d: %v", i, err)
		}
	}

	rows, err := db.Query("SELECT COUNT(*) FROM t")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	rows.Next()
	var count int
	rows.Scan(&count)
	if count != 5 {
		t.Errorf("expected 5 rows, got %d", count)
	}
}

func TestSaveMemoryTo(t *testing.T) {
	db, err := OpenMemory()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	if err := db.ExecDDL([]string{`CREATE TABLE t (id INTEGER)`}); err != nil {
		t.Fatal(err)
	}
	db.InsertRecord("t", []string{"id"}, []any{42})

	path := t.TempDir() + "/test.db"
	if err := db.SaveTo(path); err != nil {
		t.Fatalf("SaveTo: %v", err)
	}

	// Open the saved file and verify.
	db2, err := Open(path)
	if err != nil {
		t.Fatalf("Open saved: %v", err)
	}
	defer db2.Close()

	rows, err := db2.Query("SELECT id FROM t")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	if !rows.Next() {
		t.Fatal("expected 1 row in saved db")
	}
	var id int
	rows.Scan(&id)
	if id != 42 {
		t.Errorf("id = %d, want 42", id)
	}
}

func TestOpenReadOnly(t *testing.T) {
	// Create a DB file first.
	tmp := t.TempDir() + "/ro.db"
	db, err := Open(tmp)
	if err != nil {
		t.Fatal(err)
	}
	db.ExecDDL([]string{`CREATE TABLE t (id INTEGER)`})
	db.InsertRecord("t", []string{"id"}, []any{1})
	db.Close()

	// Open read-only.
	rodb, err := OpenReadOnly(tmp)
	if err != nil {
		t.Fatalf("OpenReadOnly: %v", err)
	}
	defer rodb.Close()

	rows, err := rodb.Query("SELECT id FROM t")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	if !rows.Next() {
		t.Fatal("expected row")
	}
}

func TestQuoteName(t *testing.T) {
	tests := []struct {
		in   string
		want string
	}{
		{"users", `"users"`},
		{"my table", `"my table"`},
		{`it's "quoted"`, `"it's ""quoted"""`},
	}
	for _, tt := range tests {
		got := quoteName(tt.in)
		if got != tt.want {
			t.Errorf("quoteName(%q) = %q, want %q", tt.in, got, tt.want)
		}
	}
}
