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

	fr, err := l.Load(absPath("alice.users.yaml"), "alice.users.yaml")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if fr.EntityType != "users" {
		t.Errorf("EntityType = %q, want users", fr.EntityType)
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

func TestYAMLLoader_Anchors(t *testing.T) {
	l := &YAMLLoader{}
	fr, err := l.Load(absPath("nested.meal.yaml"), "nested.meal.yaml")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if fr.EntityType != "meal" {
		t.Errorf("EntityType = %q, want meal", fr.EntityType)
	}

	rec := fr.Records[0]
	if rec.Key != "nested" {
		t.Errorf("Key = %q, want nested", rec.Key)
	}
	if rec.Fields["name"] != "Test Meal" {
		t.Errorf("name = %v, want Test Meal", rec.Fields["name"])
	}

	// courses should be preserved as []any (not flattened).
	courses, ok := rec.Fields["courses"].([]any)
	if !ok {
		t.Fatalf("courses should be []any, got %T", rec.Fields["courses"])
	}
	if len(courses) != 2 {
		t.Fatalf("expected 2 courses, got %d", len(courses))
	}

	// First course has recipes array containing an EntityRef.
	c0, ok := courses[0].(map[string]any)
	if !ok {
		t.Fatalf("course[0] should be map, got %T", courses[0])
	}
	recipes, ok := c0["recipes"].([]any)
	if !ok {
		t.Fatalf("recipes should be []any, got %T", c0["recipes"])
	}
	if len(recipes) != 1 {
		t.Fatalf("expected 1 recipe ref, got %d", len(recipes))
	}
	ref, ok := recipes[0].(EntityRef)
	if !ok {
		t.Fatalf("recipe ref should be EntityRef, got %T (%v)", recipes[0], recipes[0])
	}
	if ref.Path != "recipes/soup" {
		t.Errorf("EntityRef.Path = %q, want recipes/soup", ref.Path)
	}
}

func TestTOMLLoader(t *testing.T) {
	l := &TOMLLoader{}
	fr, err := l.Load(absPath("widget.things.toml"), "widget.things.toml")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if fr.EntityType != "things" {
		t.Errorf("EntityType = %q, want things", fr.EntityType)
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
	fr, err := l.Load(absPath("config.settings.json"), "config.settings.json")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if fr.EntityType != "settings" {
		t.Errorf("EntityType = %q, want settings", fr.EntityType)
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
	fr, err := l.Load(absPath("catalog.items.xml"), "catalog.items.xml")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if fr.EntityType != "items" {
		t.Errorf("EntityType = %q, want items", fr.EntityType)
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
	fr, err := l.Load(absPath("settings.prefs.plist"), "settings.prefs.plist")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if fr.EntityType != "prefs" {
		t.Errorf("EntityType = %q, want prefs", fr.EntityType)
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
		file           string
		wantKey        string
		wantEntityType string
	}{
		{"alice.users.yaml", "alice", "users"},
		{"widget.things.toml", "widget", "things"},
		{"config.settings.json", "config", "settings"},
		{"catalog.items.xml", "catalog", "items"},
		{"settings.prefs.plist", "settings", "prefs"},
	}
	for _, tt := range tests {
		t.Run(tt.file, func(t *testing.T) {
			fr, err := reg.LoadFile(absPath(tt.file), tt.file)
			if err != nil {
				t.Fatalf("LoadFile(%q): %v", tt.file, err)
			}
			if fr.EntityType != tt.wantEntityType {
				t.Errorf("EntityType = %q, want %q", fr.EntityType, tt.wantEntityType)
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

	// EntityRef should be returned as-is.
	ref := EntityRef{Path: "foo/bar"}
	got = flattenValue(ref)
	if got != ref {
		t.Errorf("EntityRef should be returned as-is, got %v", got)
	}
}

func TestEntityType(t *testing.T) {
	tests := []struct {
		path string
		want string
	}{
		{"alice.users.yaml", "users"},
		{"users/alice.users.yaml", "users"},
		{"celeriac-veloute.recipe.yaml", "recipe"},
		{"recipes/celeriac-veloute.recipe.yaml", "recipe"},
		{"alice.yaml", ""},
		{"users/alice.yaml", ""},
		{"post_tags/hw.post_tags.json", "post_tags"},
	}
	for _, tt := range tests {
		got := EntityType(tt.path)
		if got != tt.want {
			t.Errorf("EntityType(%q) = %q, want %q", tt.path, got, tt.want)
		}
	}
}

func TestEntityKey(t *testing.T) {
	tests := []struct {
		path string
		want string
	}{
		{"alice.users.yaml", "alice"},
		{"users/alice.users.yaml", "alice"},
		{"celeriac-veloute.recipe.yaml", "celeriac-veloute"},
		{"alice.yaml", "alice"},
		{"widget.toml", "widget"},
	}
	for _, tt := range tests {
		got := EntityKey(tt.path)
		if got != tt.want {
			t.Errorf("EntityKey(%q) = %q, want %q", tt.path, got, tt.want)
		}
	}
}

func TestEntityPK(t *testing.T) {
	tests := []struct {
		path string
		want string
	}{
		{"alice.users.yaml", "alice"},
		{"users/alice.users.yaml", "users/alice"},
		{"recipes/celeriac-veloute.recipe.yaml", "recipes/celeriac-veloute"},
		{"alice.yaml", "alice"},
	}
	for _, tt := range tests {
		got := EntityPK(tt.path)
		if got != tt.want {
			t.Errorf("EntityPK(%q) = %q, want %q", tt.path, got, tt.want)
		}
	}
}
