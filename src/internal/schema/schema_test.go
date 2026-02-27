package schema

import (
	"strings"
	"testing"

	"github.com/notwillk/sqlfs/internal/config"
	"github.com/notwillk/sqlfs/internal/dbml"
)

func makeSchema(src string, t *testing.T) *dbml.Schema {
	t.Helper()
	s, err := dbml.Parse([]byte(src))
	if err != nil {
		t.Fatalf("parse error: %v", err)
	}
	return s
}

func defaultConfig() *config.Config {
	return config.Default()
}

func TestCreateTableSQL_BasicColumns(t *testing.T) {
	src := `
Table users {
  id integer [pk, increment]
  name varchar [not null]
  score decimal
  active boolean [default: true]
}
`
	schema := makeSchema(src, t)
	g := New(schema, defaultConfig())
	sql, err := g.CreateTableSQL(schema.Tables[0])
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if !strings.Contains(sql, `"id" INTEGER PRIMARY KEY AUTOINCREMENT`) {
		t.Errorf("missing id column: %s", sql)
	}
	if !strings.Contains(sql, `"name" TEXT NOT NULL`) {
		t.Errorf("missing name column: %s", sql)
	}
	if !strings.Contains(sql, `"score" REAL`) {
		t.Errorf("missing score column: %s", sql)
	}
	if !strings.Contains(sql, `"active" INTEGER DEFAULT 1`) {
		t.Errorf("missing active column: %s", sql)
	}
	if !strings.Contains(sql, "CREATE TABLE IF NOT EXISTS") {
		t.Errorf("missing CREATE TABLE: %s", sql)
	}
}

func TestCreateTableSQL_StandardColumns(t *testing.T) {
	src := `Table t { id integer [pk] }`
	schema := makeSchema(src, t)
	g := New(schema, defaultConfig())
	sql, err := g.CreateTableSQL(schema.Tables[0])
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	for _, col := range []string{"__path__", "__created_at__", "__modified_at__", "__checksum__", "__ulid__"} {
		if !strings.Contains(sql, `"`+col+`" TEXT`) {
			t.Errorf("missing standard column %q: %s", col, sql)
		}
	}
}

func TestCreateTableSQL_CustomStandardColumnNames(t *testing.T) {
	src := `Table t { id integer [pk] }`
	schema := makeSchema(src, t)
	cfg := defaultConfig()
	cfg.StandardColumns.Path = "path"
	cfg.StandardColumns.ULID = "ulid"
	g := New(schema, cfg)
	sql, err := g.CreateTableSQL(schema.Tables[0])
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !strings.Contains(sql, `"path" TEXT`) {
		t.Errorf("custom path column missing: %s", sql)
	}
	if !strings.Contains(sql, `"ulid" TEXT`) {
		t.Errorf("custom ulid column missing: %s", sql)
	}
}

func TestDDL_MultipleTablesAndIndexes(t *testing.T) {
	src := `
Table users { id integer [pk] }
Table posts {
  id integer [pk]
  user_id integer

  indexes {
    user_id [name: 'idx_user']
    (id, user_id) [unique]
  }
}
`
	schema := makeSchema(src, t)
	g := New(schema, defaultConfig())
	stmts, err := g.DDL()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Should have 2 CREATE TABLE + 2 CREATE INDEX (non-pk indexes)
	tableCount := 0
	indexCount := 0
	for _, s := range stmts {
		if strings.HasPrefix(s, "CREATE TABLE") {
			tableCount++
		}
		if strings.HasPrefix(s, "CREATE") && strings.Contains(s, "INDEX") {
			indexCount++
		}
	}
	if tableCount != 2 {
		t.Errorf("expected 2 CREATE TABLE, got %d", tableCount)
	}
	if indexCount != 2 {
		t.Errorf("expected 2 CREATE INDEX, got %d", indexCount)
	}
}

func TestDBMLTypeToSQLite(t *testing.T) {
	tests := []struct {
		typeName string
		want     string
	}{
		{"integer", "INTEGER"},
		{"int", "INTEGER"},
		{"bigint", "INTEGER"},
		{"serial", "INTEGER"},
		{"float", "REAL"},
		{"decimal", "REAL"},
		{"numeric", "REAL"},
		{"double", "REAL"},
		{"boolean", "INTEGER"},
		{"bool", "INTEGER"},
		{"varchar", "TEXT"},
		{"text", "TEXT"},
		{"uuid", "TEXT"},
		{"timestamp", "TEXT"},
		{"json", "TEXT"},
		{"blob", "BLOB"},
	}
	for _, tt := range tests {
		got := DBMLTypeToSQLite(dbml.ColumnType{Name: tt.typeName})
		if got != tt.want {
			t.Errorf("DBMLTypeToSQLite(%q) = %q, want %q", tt.typeName, got, tt.want)
		}
	}
}

func TestCreateTableSQL_DefaultValues(t *testing.T) {
	src := `
Table t {
  a varchar [default: 'hello']
  b integer [default: 42]
  c boolean [default: false]
  d timestamp [default: ` + "`now()`" + `]
  e integer [default: null]
}
`
	schema := makeSchema(src, t)
	g := New(schema, defaultConfig())
	sql, err := g.CreateTableSQL(schema.Tables[0])
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !strings.Contains(sql, "DEFAULT 'hello'") {
		t.Errorf("string default missing: %s", sql)
	}
	if !strings.Contains(sql, "DEFAULT 42") {
		t.Errorf("number default missing: %s", sql)
	}
	if !strings.Contains(sql, "DEFAULT 0") {
		t.Errorf("bool false default missing: %s", sql)
	}
	if !strings.Contains(sql, "DEFAULT (now())") {
		t.Errorf("expr default missing: %s", sql)
	}
	if !strings.Contains(sql, "DEFAULT NULL") {
		t.Errorf("null default missing: %s", sql)
	}
}
