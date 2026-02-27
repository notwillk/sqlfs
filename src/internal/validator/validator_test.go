package validator

import (
	"testing"

	"github.com/notwillk/sqlfs/internal/config"
	"github.com/notwillk/sqlfs/internal/dbml"
	"github.com/notwillk/sqlfs/internal/loader"
)

func makeSchema(src string, t *testing.T) *dbml.Schema {
	t.Helper()
	s, err := dbml.Parse([]byte(src))
	if err != nil {
		t.Fatalf("parse error: %v", err)
	}
	return s
}

func makeFileRecord(tableName string, records []loader.Record) *loader.FileRecord {
	return &loader.FileRecord{
		TableName: tableName,
		FilePath:  tableName + ".yaml",
		Records:   records,
	}
}

func TestValidate_AllValid(t *testing.T) {
	schema := makeSchema(`
Table users {
  id integer [pk]
  name varchar [not null]
  age integer
}
`, t)

	cfg := config.Default()
	v := New(schema, cfg)
	fr := makeFileRecord("users", []loader.Record{
		{Key: "alice", Fields: map[string]any{"id": 1, "name": "Alice", "age": 30}},
		{Key: "bob", Fields: map[string]any{"id": 2, "name": "Bob", "age": 25}},
	})

	valid, warns, err := v.Validate(fr)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(valid) != 2 {
		t.Errorf("expected 2 valid records, got %d", len(valid))
	}
	if len(warns) != 0 {
		t.Errorf("expected 0 warnings, got %d", len(warns))
	}
}

func TestValidate_MissingRequired_Fail(t *testing.T) {
	schema := makeSchema(`
Table users {
  id integer [pk]
  name varchar [not null]
}
`, t)

	cfg := config.Default() // invalid: fail
	v := New(schema, cfg)
	fr := makeFileRecord("users", []loader.Record{
		{Key: "alice", Fields: map[string]any{"id": 1}}, // missing 'name'
	})

	_, _, err := v.Validate(fr)
	if err == nil {
		t.Fatal("expected error for missing required field")
	}
}

func TestValidate_MissingRequired_Warn(t *testing.T) {
	schema := makeSchema(`
Table users {
  id integer [pk]
  name varchar [not null]
}
`, t)

	cfg := config.Default().WithInvalid("warn")
	v := New(schema, cfg)
	fr := makeFileRecord("users", []loader.Record{
		{Key: "alice", Fields: map[string]any{"id": 1}}, // missing 'name'
		{Key: "bob", Fields: map[string]any{"id": 2, "name": "Bob"}},
	})

	valid, warns, err := v.Validate(fr)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(valid) != 2 { // warn mode still includes the record
		t.Errorf("expected 2 valid records, got %d", len(valid))
	}
	if len(warns) == 0 {
		t.Error("expected warnings")
	}
}

func TestValidate_MissingRequired_Silent(t *testing.T) {
	schema := makeSchema(`
Table users {
  id integer [pk]
  name varchar [not null]
}
`, t)

	cfg := config.Default().WithInvalid("silent")
	v := New(schema, cfg)
	fr := makeFileRecord("users", []loader.Record{
		{Key: "alice", Fields: map[string]any{"id": 1}}, // missing 'name', dropped
		{Key: "bob", Fields: map[string]any{"id": 2, "name": "Bob"}},
	})

	valid, warns, err := v.Validate(fr)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(valid) != 1 {
		t.Errorf("expected 1 valid record (alice dropped), got %d", len(valid))
	}
	if len(warns) != 0 {
		t.Errorf("expected 0 warnings in silent mode, got %d", len(warns))
	}
}

func TestValidate_EnumValidation(t *testing.T) {
	schema := makeSchema(`
Table posts {
  id integer [pk]
  status post_status
}
enum post_status {
  draft
  published
  archived
}
`, t)

	cfg := config.Default()
	v := New(schema, cfg)

	fr := makeFileRecord("posts", []loader.Record{
		{Key: "p1", Fields: map[string]any{"id": 1, "status": "invalid_value"}},
	})

	_, _, err := v.Validate(fr)
	if err == nil {
		t.Fatal("expected error for invalid enum value")
	}
}

func TestValidate_UnknownField_Fail(t *testing.T) {
	schema := makeSchema(`
Table users {
  id integer [pk]
  name varchar
}
`, t)

	cfg := config.Default()
	v := New(schema, cfg)
	fr := makeFileRecord("users", []loader.Record{
		{Key: "alice", Fields: map[string]any{"id": 1, "name": "Alice", "unknown_col": "value"}},
	})

	_, _, err := v.Validate(fr)
	if err == nil {
		t.Fatal("expected error for unknown field")
	}
}

func TestValidate_NoSchemaForTable_PassThrough(t *testing.T) {
	schema := makeSchema(`Table other { id integer }`, t)
	cfg := config.Default()
	v := New(schema, cfg)

	fr := makeFileRecord("users", []loader.Record{
		{Key: "alice", Fields: map[string]any{"anything": "goes"}},
	})

	valid, _, err := v.Validate(fr)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(valid) != 1 {
		t.Errorf("expected record to pass through, got %d", len(valid))
	}
}

func TestValidate_DefaultedFieldNotRequired(t *testing.T) {
	schema := makeSchema(`
Table posts {
  id integer [pk]
  status varchar [not null, default: 'draft']
}
`, t)

	cfg := config.Default()
	v := New(schema, cfg)
	fr := makeFileRecord("posts", []loader.Record{
		{Key: "p1", Fields: map[string]any{"id": 1}}, // status has default, should be OK
	})

	valid, _, err := v.Validate(fr)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(valid) != 1 {
		t.Errorf("expected 1 valid record, got %d", len(valid))
	}
}
