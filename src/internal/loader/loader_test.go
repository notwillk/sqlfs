package loader

import (
	"path/filepath"
	"testing"
)

func absPath(name string) string {
	return filepath.Join("testdata", name)
}

func relPath(name string) string { return name }

func TestYAMLLoader(t *testing.T) {
	l := &YAMLLoader{}
	if exts := l.Extensions(); len(exts) == 0 {
		t.Fatal("no extensions")
	}

	fr, err := l.Load(absPath("users.yaml"), "users.yaml")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if fr.TableName != "users" {
		t.Errorf("TableName = %q, want users", fr.TableName)
	}
	if len(fr.Records) != 2 {
		t.Fatalf("expected 2 records, got %d", len(fr.Records))
	}
	if fr.Checksum == "" {
		t.Error("expected non-empty checksum")
	}

	// Find alice record.
	var alice *Record
	for i := range fr.Records {
		if fr.Records[i].Key == "alice" {
			alice = &fr.Records[i]
		}
	}
	if alice == nil {
		t.Fatal("alice record not found")
	}
	if alice.Fields["name"] != "Alice Smith" {
		t.Errorf("alice.name = %v, want Alice Smith", alice.Fields["name"])
	}
	if alice.Fields["active"] != true {
		t.Errorf("alice.active = %v, want true", alice.Fields["active"])
	}
}

func TestTOMLLoader(t *testing.T) {
	l := &TOMLLoader{}
	fr, err := l.Load(absPath("items.toml"), "items.toml")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if fr.TableName != "items" {
		t.Errorf("TableName = %q, want items", fr.TableName)
	}
	if len(fr.Records) != 2 {
		t.Fatalf("expected 2 records, got %d", len(fr.Records))
	}

	var widget *Record
	for i := range fr.Records {
		if fr.Records[i].Key == "widget" {
			widget = &fr.Records[i]
		}
	}
	if widget == nil {
		t.Fatal("widget record not found")
	}
	if widget.Fields["name"] != "Widget A" {
		t.Errorf("widget.name = %v", widget.Fields["name"])
	}
}

func TestHJSONLoader(t *testing.T) {
	l := &HJSONLoader{}
	fr, err := l.Load(absPath("config.json"), "config.json")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if fr.TableName != "config" {
		t.Errorf("TableName = %q, want config", fr.TableName)
	}
	if len(fr.Records) != 2 {
		t.Fatalf("expected 2 records, got %d", len(fr.Records))
	}
}

func TestXMLLoader(t *testing.T) {
	l := &XMLLoader{}
	fr, err := l.Load(absPath("catalog.xml"), "catalog.xml")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if fr.TableName != "catalog" {
		t.Errorf("TableName = %q, want catalog", fr.TableName)
	}
	if len(fr.Records) != 2 {
		t.Fatalf("expected 2 records, got %d; records: %+v", len(fr.Records), fr.Records)
	}
}

func TestPlistLoader(t *testing.T) {
	l := &PlistLoader{}
	fr, err := l.Load(absPath("settings.plist"), "settings.plist")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if fr.TableName != "settings" {
		t.Errorf("TableName = %q, want settings", fr.TableName)
	}
	if len(fr.Records) != 2 {
		t.Fatalf("expected 2 records, got %d", len(fr.Records))
	}
}

func TestRegistry_Dispatch(t *testing.T) {
	reg := NewRegistry()

	tests := []struct {
		file      string
		wantTable string
	}{
		{"users.yaml", "users"},
		{"items.toml", "items"},
		{"config.json", "config"},
		{"catalog.xml", "catalog"},
		{"settings.plist", "settings"},
	}
	for _, tt := range tests {
		t.Run(tt.file, func(t *testing.T) {
			fr, err := reg.LoadFile(absPath(tt.file), tt.file)
			if err != nil {
				t.Fatalf("LoadFile(%q): %v", tt.file, err)
			}
			if fr.TableName != tt.wantTable {
				t.Errorf("TableName = %q, want %q", fr.TableName, tt.wantTable)
			}
		})
	}
}

func TestRegistry_IsSupported(t *testing.T) {
	reg := NewRegistry()
	for _, ext := range []string{".yaml", ".yml", ".toml", ".json", ".jsonc", ".json5", ".xml", ".plist"} {
		if !reg.IsSupported("file" + ext) {
			t.Errorf("extension %q should be supported", ext)
		}
	}
	if reg.IsSupported("file.dbml") {
		t.Error(".dbml should not be supported")
	}
}

func TestRegistry_UnsupportedExtension(t *testing.T) {
	reg := NewRegistry()
	_, err := reg.LoadFile("file.xyz", "file.xyz")
	if err == nil {
		t.Fatal("expected error for unsupported extension")
	}
}

func TestFlattenValue(t *testing.T) {
	tests := []struct {
		in   any
		want any
	}{
		{"hello", "hello"},
		{42, 42},
		{3.14, 3.14},
		{true, true},
		{nil, nil},
	}
	for _, tt := range tests {
		got := flattenValue(tt.in)
		if got != tt.want {
			t.Errorf("flattenValue(%v) = %v, want %v", tt.in, got, tt.want)
		}
	}

	// Nested map should become JSON string.
	nested := map[string]any{"a": 1}
	got := flattenValue(nested)
	if _, ok := got.(string); !ok {
		t.Errorf("nested map should become string, got %T", got)
	}
}

func TestTableName(t *testing.T) {
	tests := []struct {
		path string
		want string
	}{
		{"users.yaml", "users"},
		{"sub/users.yaml", "users"},
		{"config.json", "config"},
		{"data.toml", "data"},
	}
	for _, tt := range tests {
		got := tableName(tt.path)
		if got != tt.want {
			t.Errorf("tableName(%q) = %q, want %q", tt.path, got, tt.want)
		}
	}
}
