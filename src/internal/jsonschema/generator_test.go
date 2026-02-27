package jsonschema

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/notwillk/sqlfs/internal/config"
	"github.com/notwillk/sqlfs/internal/dbml"
)

func parseSchema(src string, t *testing.T) *dbml.Schema {
	t.Helper()
	s, err := dbml.Parse([]byte(src))
	if err != nil {
		t.Fatalf("parse error: %v", err)
	}
	return s
}

func unmarshalJSON(data []byte, t *testing.T) map[string]any {
	t.Helper()
	var m map[string]any
	if err := json.Unmarshal(data, &m); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	return m
}

func TestGenerate_BasicTable(t *testing.T) {
	src := `
Table users {
  id integer [pk]
  name varchar [not null]
  age integer
  Note: 'User accounts'
}
`
	schema := parseSchema(src, t)
	cfg := config.Default()
	data, err := Generate(schema, cfg)
	if err != nil {
		t.Fatalf("Generate: %v", err)
	}

	doc := unmarshalJSON(data, t)

	if doc["$schema"] == nil {
		t.Error("missing $schema")
	}

	defs := doc["$defs"].(map[string]any)
	rowSchema := defs["users_row"].(map[string]any)
	props := rowSchema["properties"].(map[string]any)

	if _, ok := props["name"]; !ok {
		t.Error("missing name property")
	}
	if _, ok := props["id"]; !ok {
		t.Error("missing id property")
	}

	// Required should include 'name' (not null, no default), but not 'id' (pk).
	req, ok := rowSchema["required"].([]any)
	if !ok {
		t.Fatal("expected required array")
	}
	found := false
	for _, r := range req {
		if r == "name" {
			found = true
		}
	}
	if !found {
		t.Error("'name' should be required")
	}
}

func TestGenerate_StandardColumnsExcluded(t *testing.T) {
	src := `Table t { id integer [pk] }`
	schema := parseSchema(src, t)
	cfg := config.Default()
	data, err := Generate(schema, cfg)
	if err != nil {
		t.Fatal(err)
	}
	dataStr := string(data)
	for _, stdCol := range []string{"__path__", "__created_at__", "__modified_at__", "__checksum__", "__ulid__"} {
		if strings.Contains(dataStr, stdCol) {
			t.Errorf("standard column %q should not appear in JSON schema", stdCol)
		}
	}
}

func TestGenerate_TypeMapping(t *testing.T) {
	src := `
Table t {
  a integer
  b float
  c boolean
  d varchar
  e timestamp
  f date
  g json
}
`
	schema := parseSchema(src, t)
	cfg := config.Default()
	data, err := Generate(schema, cfg)
	if err != nil {
		t.Fatal(err)
	}

	doc := unmarshalJSON(data, t)
	defs := doc["$defs"].(map[string]any)
	rowSchema := defs["t_row"].(map[string]any)
	props := rowSchema["properties"].(map[string]any)

	checkType := func(col, want string) {
		t.Helper()
		p := props[col].(map[string]any)
		got, _ := p["type"].(string)
		if got != want {
			t.Errorf("column %q type = %q, want %q", col, got, want)
		}
	}
	checkType("a", "integer")
	checkType("b", "number")
	checkType("c", "boolean")
	checkType("d", "string")
	checkType("e", "string")
	checkType("f", "string")

	// json type has no type constraint.
	jsonProp := props["g"].(map[string]any)
	if _, hasType := jsonProp["type"]; hasType {
		t.Error("json column should not have a type constraint")
	}
}

func TestGenerate_EnumColumn(t *testing.T) {
	src := `
Table posts {
  id integer [pk]
  status post_status
}
enum post_status {
  draft
  published
  archived
}
`
	schema := parseSchema(src, t)
	cfg := config.Default()
	data, err := Generate(schema, cfg)
	if err != nil {
		t.Fatal(err)
	}

	doc := unmarshalJSON(data, t)
	defs := doc["$defs"].(map[string]any)
	rowSchema := defs["posts_row"].(map[string]any)
	props := rowSchema["properties"].(map[string]any)

	statusProp := props["status"].(map[string]any)
	if statusProp["type"] != "string" {
		t.Errorf("enum column type = %v, want string", statusProp["type"])
	}
	enumVals, ok := statusProp["enum"].([]any)
	if !ok {
		t.Fatal("expected enum values array")
	}
	if len(enumVals) != 3 {
		t.Errorf("expected 3 enum values, got %d", len(enumVals))
	}
}

func TestGenerate_MultipleTablesHaveOneOf(t *testing.T) {
	src := `
Table users { id integer [pk] }
Table posts  { id integer [pk] }
`
	schema := parseSchema(src, t)
	cfg := config.Default()
	data, err := Generate(schema, cfg)
	if err != nil {
		t.Fatal(err)
	}

	doc := unmarshalJSON(data, t)
	oneOf, ok := doc["oneOf"].([]any)
	if !ok {
		t.Fatal("expected oneOf array")
	}
	if len(oneOf) != 2 {
		t.Errorf("expected 2 oneOf entries, got %d", len(oneOf))
	}
}

func TestDBMLTypeToJSONSchema(t *testing.T) {
	tests := []struct {
		in         string
		wantType   string
		wantFormat string
	}{
		{"integer", "integer", ""},
		{"bigint", "integer", ""},
		{"serial", "integer", ""},
		{"float", "number", ""},
		{"decimal", "number", ""},
		{"boolean", "boolean", ""},
		{"bool", "boolean", ""},
		{"varchar", "string", ""},
		{"text", "string", ""},
		{"uuid", "string", ""},
		{"date", "string", "date"},
		{"timestamp", "string", "date-time"},
		{"json", "", ""},
		{"jsonb", "", ""},
	}
	for _, tt := range tests {
		gotType, gotFormat := dbmlTypeToJSONSchema(tt.in)
		if gotType != tt.wantType || gotFormat != tt.wantFormat {
			t.Errorf("dbmlTypeToJSONSchema(%q) = (%q, %q), want (%q, %q)",
				tt.in, gotType, gotFormat, tt.wantType, tt.wantFormat)
		}
	}
}

func TestGenerateConfigSchema(t *testing.T) {
	data, err := GenerateConfigSchema()
	if err != nil {
		t.Fatalf("GenerateConfigSchema: %v", err)
	}

	doc := unmarshalJSON(data, t)
	if doc["type"] != "object" {
		t.Errorf("type = %v, want object", doc["type"])
	}
	props := doc["properties"].(map[string]any)
	for _, key := range []string{"schema", "invalid", "port", "credentials", "columns"} {
		if _, ok := props[key]; !ok {
			t.Errorf("config schema missing property %q", key)
		}
	}
}
