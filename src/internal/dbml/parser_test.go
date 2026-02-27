package dbml

import (
	"strings"
	"testing"
)

func TestParse_SimpleTable(t *testing.T) {
	src := `
Table users {
  id integer [pk, increment]
  name varchar [not null]
  email varchar(255) [unique]
  active boolean [default: true]
  score decimal [default: 0]
  label varchar [default: 'hello']
}
`
	schema, err := Parse([]byte(src))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(schema.Tables) != 1 {
		t.Fatalf("expected 1 table, got %d", len(schema.Tables))
	}
	tbl := schema.Tables[0]
	if tbl.Name != "users" {
		t.Errorf("table name = %q, want users", tbl.Name)
	}
	if len(tbl.Columns) != 6 {
		t.Fatalf("expected 6 columns, got %d", len(tbl.Columns))
	}

	id := tbl.Columns[0]
	if !id.PK {
		t.Error("id: expected PK")
	}
	if !id.Increment {
		t.Error("id: expected Increment")
	}

	name := tbl.Columns[1]
	if !name.NotNull {
		t.Error("name: expected NotNull")
	}

	email := tbl.Columns[2]
	if !email.Unique {
		t.Error("email: expected Unique")
	}
	if email.Type.Args[0] != 255 {
		t.Errorf("email type arg = %d, want 255", email.Type.Args[0])
	}

	active := tbl.Columns[3]
	if active.Default == nil || active.Default.Kind != DefaultBool || active.Default.Value != "true" {
		t.Errorf("active default = %+v, want bool true", active.Default)
	}

	score := tbl.Columns[4]
	if score.Default == nil || score.Default.Kind != DefaultNumber {
		t.Errorf("score default = %+v, want number", score.Default)
	}

	label := tbl.Columns[5]
	if label.Default == nil || label.Default.Kind != DefaultString || label.Default.Value != "hello" {
		t.Errorf("label default = %+v, want string hello", label.Default)
	}
}

func TestParse_TableAlias(t *testing.T) {
	src := `Table orders as O { id integer [pk] }`
	schema, err := Parse([]byte(src))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if schema.Tables[0].Alias != "O" {
		t.Errorf("alias = %q, want O", schema.Tables[0].Alias)
	}
}

func TestParse_TableNote(t *testing.T) {
	src := `Table users { id integer Note: 'user accounts' }`
	schema, err := Parse([]byte(src))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if schema.Tables[0].Note != "user accounts" {
		t.Errorf("note = %q", schema.Tables[0].Note)
	}
}

func TestParse_Enum(t *testing.T) {
	src := `
enum status {
  draft
  published [note: 'live']
  archived
}
`
	schema, err := Parse([]byte(src))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(schema.Enums) != 1 {
		t.Fatalf("expected 1 enum, got %d", len(schema.Enums))
	}
	en := schema.Enums[0]
	if en.Name != "status" {
		t.Errorf("enum name = %q", en.Name)
	}
	if len(en.Values) != 3 {
		t.Fatalf("expected 3 values, got %d", len(en.Values))
	}
	if en.Values[1].Note != "live" {
		t.Errorf("published note = %q", en.Values[1].Note)
	}
}

func TestParse_Ref(t *testing.T) {
	src := `
Table posts {
  id integer [pk]
  user_id integer
}
Table users { id integer [pk] }
Ref: posts.user_id > users.id [delete: cascade]
`
	schema, err := Parse([]byte(src))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(schema.Refs) != 1 {
		t.Fatalf("expected 1 ref, got %d", len(schema.Refs))
	}
	ref := schema.Refs[0]
	if ref.From.Table != "posts" || ref.From.Column != "user_id" {
		t.Errorf("ref from = %+v", ref.From)
	}
	if ref.To.Table != "users" || ref.To.Column != "id" {
		t.Errorf("ref to = %+v", ref.To)
	}
	if ref.Relation != ManyToOne {
		t.Errorf("relation = %d, want ManyToOne", ref.Relation)
	}
	if ref.OnDelete != "cascade" {
		t.Errorf("OnDelete = %q, want cascade", ref.OnDelete)
	}
}

func TestParse_InlineRef(t *testing.T) {
	src := `
Table posts {
  id integer [pk]
  user_id integer [ref: > users.id]
}
Table users { id integer [pk] }
`
	schema, err := Parse([]byte(src))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	col := schema.Tables[0].ColumnByName("user_id")
	if col == nil {
		t.Fatal("user_id column not found")
	}
	if len(col.Refs) != 1 {
		t.Fatalf("expected 1 inline ref, got %d", len(col.Refs))
	}
	if col.Refs[0].To.Table != "users" {
		t.Errorf("ref to table = %q", col.Refs[0].To.Table)
	}
}

func TestParse_Indexes(t *testing.T) {
	src := `
Table posts {
  id integer [pk]
  user_id integer
  status varchar

  indexes {
    user_id
    (user_id, status) [name: 'idx_us', unique]
    ` + "`now()`" + ` [name: 'idx_now']
  }
}
`
	schema, err := Parse([]byte(src))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	idxs := schema.Tables[0].Indexes
	if len(idxs) != 3 {
		t.Fatalf("expected 3 indexes, got %d", len(idxs))
	}
	if idxs[1].Name != "idx_us" {
		t.Errorf("idx name = %q", idxs[1].Name)
	}
	if !idxs[1].Unique {
		t.Error("idx[1] expected unique")
	}
	if len(idxs[1].Columns) != 2 {
		t.Errorf("composite idx columns = %d", len(idxs[1].Columns))
	}
	if !idxs[2].IsExpr {
		t.Error("idx[2] expected IsExpr")
	}
}

func TestParse_Project(t *testing.T) {
	src := `
Project myapp {
  database_type: 'SQLite'
  Note: 'My application'
}
`
	schema, err := Parse([]byte(src))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if schema.Project == nil {
		t.Fatal("expected project")
	}
	if schema.Project.DatabaseType != "SQLite" {
		t.Errorf("DatabaseType = %q", schema.Project.DatabaseType)
	}
}

func TestParse_Comments(t *testing.T) {
	src := `
// This is a comment
Table users {
  id integer [pk] // inline comment
  /* block
     comment */
  name varchar
}
`
	schema, err := Parse([]byte(src))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(schema.Tables[0].Columns) != 2 {
		t.Errorf("expected 2 columns, got %d", len(schema.Tables[0].Columns))
	}
}

func TestParse_QuotedNames(t *testing.T) {
	src := `Table "my table" { "my col" varchar }`
	schema, err := Parse([]byte(src))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if schema.Tables[0].Name != "my table" {
		t.Errorf("table name = %q", schema.Tables[0].Name)
	}
	if schema.Tables[0].Columns[0].Name != "my col" {
		t.Errorf("col name = %q", schema.Tables[0].Columns[0].Name)
	}
}

func TestParse_BacktickDefault(t *testing.T) {
	src := `Table t { created_at timestamp [default: ` + "`now()`" + `] }`
	schema, err := Parse([]byte(src))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	col := schema.Tables[0].Columns[0]
	if col.Default == nil || col.Default.Kind != DefaultExpr || col.Default.Value != "now()" {
		t.Errorf("default = %+v", col.Default)
	}
}

func TestParseFile_Simple(t *testing.T) {
	schema, err := ParseFile("testdata/simple.dbml")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(schema.Tables) != 2 {
		t.Errorf("expected 2 tables, got %d", len(schema.Tables))
	}
	if len(schema.Enums) != 1 {
		t.Errorf("expected 1 enum, got %d", len(schema.Enums))
	}
	if len(schema.Refs) != 1 {
		t.Errorf("expected 1 ref, got %d", len(schema.Refs))
	}
}

func TestParseFile_Complex(t *testing.T) {
	schema, err := ParseFile("testdata/complex.dbml")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(schema.Tables) != 3 {
		t.Errorf("expected 3 tables, got %d", len(schema.Tables))
	}
	if schema.Project == nil || schema.Project.Name != "ecommerce" {
		t.Errorf("project = %+v", schema.Project)
	}
}

func TestParse_Error_UnknownKeyword(t *testing.T) {
	_, err := Parse([]byte(`foobar {}`))
	if err == nil {
		t.Fatal("expected error")
	}
	if !strings.Contains(err.Error(), "foobar") {
		t.Errorf("error should mention 'foobar': %v", err)
	}
}

func TestParse_MultipleRefs_StandaloneAndInline(t *testing.T) {
	src := `
Table A { id integer [pk] }
Table B {
  id integer [pk]
  a_id integer [ref: > A.id]
}
Ref r1: B.a_id > A.id
`
	schema, err := Parse([]byte(src))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(schema.Refs) != 1 {
		t.Errorf("expected 1 standalone ref, got %d", len(schema.Refs))
	}
	colB := schema.TableByName("B").ColumnByName("a_id")
	if len(colB.Refs) != 1 {
		t.Errorf("expected 1 inline ref on a_id")
	}
}
