package loader

import (
	"path/filepath"
	"testing"
)

func absPath(name string) string {
	return filepath.Join("testdata", name)
}

func TestYAMLLoader(t *testing.T) {
	l := &YAMLLoader{}
	if exts := l.Extensions(); len(exts) == 0 {
		t.Fatal("no extensions")
	}

	fr, err := l.Load(absPath("alice.yaml"), "alice.yaml")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// TableName is set by the builder via duck-typing, not by the loader.
	if fr.TableName != "" {
		t.Errorf("TableName = %q, want empty (set by builder)", fr.TableName)
	}
	if len(fr.Records) != 1 {
		t.Fatalf("expected 1 record, got %d", len(fr.Records))
	}
	if fr.Checksum == "" {
		t.Error("expected non-empty checksum")
	}

	rec := fr.Records[0]
	if rec.Key != "alice" {
		t.Errorf("Key = %q, want alice", rec.Key)
	}
	if rec.Fields["name"] != "Alice Smith" {
		t.Errorf("name = %v, want Alice Smith", rec.Fields["name"])
	}
	if rec.Fields["active"] != true {
		t.Errorf("active = %v, want true", rec.Fields["active"])
	}
}

func TestTOMLLoader(t *testing.T) {
	l := &TOMLLoader{}
	fr, err := l.Load(absPath("widget.toml"), "widget.toml")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if fr.TableName != "" {
		t.Errorf("TableName = %q, want empty (set by builder)", fr.TableName)
	}
	if len(fr.Records) != 1 {
		t.Fatalf("expected 1 record, got %d", len(fr.Records))
	}

	rec := fr.Records[0]
	if rec.Key != "widget" {
		t.Errorf("Key = %q, want widget", rec.Key)
	}
	if rec.Fields["name"] != "Widget A" {
		t.Errorf("name = %v, want Widget A", rec.Fields["name"])
	}
}

func TestHJSONLoader(t *testing.T) {
	l := &HJSONLoader{}
	fr, err := l.Load(absPath("config.json"), "config.json")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if fr.TableName != "" {
		t.Errorf("TableName = %q, want empty (set by builder)", fr.TableName)
	}
	if len(fr.Records) != 1 {
		t.Fatalf("expected 1 record, got %d", len(fr.Records))
	}

	rec := fr.Records[0]
	if rec.Key != "config" {
		t.Errorf("Key = %q, want config", rec.Key)
	}
	if rec.Fields["host"] != "localhost" {
		t.Errorf("host = %v, want localhost", rec.Fields["host"])
	}
}

func TestXMLLoader(t *testing.T) {
	l := &XMLLoader{}
	fr, err := l.Load(absPath("catalog.xml"), "catalog.xml")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if fr.TableName != "" {
		t.Errorf("TableName = %q, want empty (set by builder)", fr.TableName)
	}
	if len(fr.Records) != 1 {
		t.Fatalf("expected 1 record, got %d; records: %+v", len(fr.Records), fr.Records)
	}

	rec := fr.Records[0]
	if rec.Key != "catalog" {
		t.Errorf("Key = %q, want catalog", rec.Key)
	}
	if rec.Fields["title"] != "Go Programming" {
		t.Errorf("title = %v, want Go Programming", rec.Fields["title"])
	}
}

func TestPlistLoader(t *testing.T) {
	l := &PlistLoader{}
	fr, err := l.Load(absPath("settings.plist"), "settings.plist")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if fr.TableName != "" {
		t.Errorf("TableName = %q, want empty (set by builder)", fr.TableName)
	}
	if len(fr.Records) != 1 {
		t.Fatalf("expected 1 record, got %d", len(fr.Records))
	}

	rec := fr.Records[0]
	if rec.Key != "settings" {
		t.Errorf("Key = %q, want settings", rec.Key)
	}
	if rec.Fields["theme"] != "system" {
		t.Errorf("theme = %v, want system", rec.Fields["theme"])
	}
}

func TestRegistry_Dispatch(t *testing.T) {
	reg := NewRegistry()

	tests := []struct {
		file    string
		wantKey string
	}{
		{"alice.yaml", "alice"},
		{"widget.toml", "widget"},
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
			// TableName is empty; the builder sets it via duck-typing.
			if fr.TableName != "" {
				t.Errorf("TableName = %q, want empty", fr.TableName)
			}
			if len(fr.Records) != 1 {
				t.Fatalf("expected 1 record, got %d", len(fr.Records))
			}
			if fr.Records[0].Key != tt.wantKey {
				t.Errorf("Key = %q, want %q", fr.Records[0].Key, tt.wantKey)
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

func TestRecordKey(t *testing.T) {
	tests := []struct {
		path string
		want string
	}{
		{"alice.yaml", "alice"},
		{"users/alice.yaml", "alice"},
		{"config.json", "config"},
		{"widget.toml", "widget"},
	}
	for _, tt := range tests {
		got := recordKey(tt.path)
		if got != tt.want {
			t.Errorf("recordKey(%q) = %q, want %q", tt.path, got, tt.want)
		}
	}
}
