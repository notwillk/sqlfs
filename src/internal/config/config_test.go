package config

import (
	"os"
	"path/filepath"
	"testing"
)

func TestDefault(t *testing.T) {
	cfg := Default()

	if cfg.SchemaFile != "schema.dbml" {
		t.Errorf("SchemaFile = %q, want %q", cfg.SchemaFile, "schema.dbml")
	}
	if cfg.Invalid != InvalidFail {
		t.Errorf("Invalid = %q, want %q", cfg.Invalid, InvalidFail)
	}
	if cfg.Port != 5432 {
		t.Errorf("Port = %d, want 5432", cfg.Port)
	}
	if cfg.UsernameEnvVar != "SQLFS_USERNAME" {
		t.Errorf("UsernameEnvVar = %q, want SQLFS_USERNAME", cfg.UsernameEnvVar)
	}
	if cfg.PasswordEnvVar != "SQLFS_PASSWORD" {
		t.Errorf("PasswordEnvVar = %q, want SQLFS_PASSWORD", cfg.PasswordEnvVar)
	}
	if cfg.StandardColumns.Path != "__path__" {
		t.Errorf("Path = %q, want __path__", cfg.StandardColumns.Path)
	}
	if cfg.StandardColumns.ULID != "__ulid__" {
		t.Errorf("ULID = %q, want __ulid__", cfg.StandardColumns.ULID)
	}
}

func TestLoad_NoFile(t *testing.T) {
	dir := t.TempDir()
	cfg, err := Load(dir)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// Should return defaults.
	if cfg.SchemaFile != "schema.dbml" {
		t.Errorf("SchemaFile = %q, want schema.dbml", cfg.SchemaFile)
	}
}

func TestLoad_PartialOverride(t *testing.T) {
	dir := t.TempDir()
	content := `
port: 9876
invalid: warn
columns:
  path: path
`
	if err := os.WriteFile(filepath.Join(dir, "sqlfs.yaml"), []byte(content), 0644); err != nil {
		t.Fatal(err)
	}

	cfg, err := Load(dir)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if cfg.Port != 9876 {
		t.Errorf("Port = %d, want 9876", cfg.Port)
	}
	if cfg.Invalid != InvalidWarn {
		t.Errorf("Invalid = %q, want warn", cfg.Invalid)
	}
	if cfg.StandardColumns.Path != "path" {
		t.Errorf("Path = %q, want path", cfg.StandardColumns.Path)
	}
	// Un-overridden fields keep defaults.
	if cfg.SchemaFile != "schema.dbml" {
		t.Errorf("SchemaFile = %q, want schema.dbml", cfg.SchemaFile)
	}
	if cfg.StandardColumns.ULID != "__ulid__" {
		t.Errorf("ULID = %q, want __ulid__", cfg.StandardColumns.ULID)
	}
}

func TestLoad_FullOverride(t *testing.T) {
	dir := t.TempDir()
	content := `
schema: custom.dbml
invalid: silent
port: 1234
credentials:
  username: MY_USER
  password: MY_PASS
columns:
  path: p
  created_at: ca
  modified_at: ma
  checksum: cs
  ulid: ul
`
	if err := os.WriteFile(filepath.Join(dir, "sqlfs.yaml"), []byte(content), 0644); err != nil {
		t.Fatal(err)
	}

	cfg, err := Load(dir)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if cfg.SchemaFile != "custom.dbml" {
		t.Errorf("SchemaFile = %q", cfg.SchemaFile)
	}
	if cfg.Invalid != InvalidSilent {
		t.Errorf("Invalid = %q", cfg.Invalid)
	}
	if cfg.Port != 1234 {
		t.Errorf("Port = %d", cfg.Port)
	}
	if cfg.UsernameEnvVar != "MY_USER" {
		t.Errorf("UsernameEnvVar = %q", cfg.UsernameEnvVar)
	}
	if cfg.PasswordEnvVar != "MY_PASS" {
		t.Errorf("PasswordEnvVar = %q", cfg.PasswordEnvVar)
	}
	if cfg.StandardColumns.Path != "p" {
		t.Errorf("Path = %q", cfg.StandardColumns.Path)
	}
	if cfg.StandardColumns.ULID != "ul" {
		t.Errorf("ULID = %q", cfg.StandardColumns.ULID)
	}
}

func TestLoad_InvalidYAML(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "sqlfs.yaml"), []byte("key: [unclosed"), 0644); err != nil {
		t.Fatal(err)
	}

	_, err := Load(dir)
	if err == nil {
		t.Fatal("expected error for invalid YAML")
	}
}

func TestWithInvalid(t *testing.T) {
	cfg := Default()
	cfg2 := cfg.WithInvalid("warn")
	if cfg2.Invalid != InvalidWarn {
		t.Errorf("Invalid = %q, want warn", cfg2.Invalid)
	}
	// Original unchanged.
	if cfg.Invalid != InvalidFail {
		t.Errorf("original Invalid changed")
	}
	// Empty override is a no-op.
	cfg3 := cfg.WithInvalid("")
	if cfg3.Invalid != InvalidFail {
		t.Errorf("empty override changed Invalid")
	}
}

func TestWithPort(t *testing.T) {
	cfg := Default()
	cfg2 := cfg.WithPort(9999)
	if cfg2.Port != 9999 {
		t.Errorf("Port = %d, want 9999", cfg2.Port)
	}
	// Zero override is a no-op.
	cfg3 := cfg.WithPort(0)
	if cfg3.Port != 5432 {
		t.Errorf("zero override changed Port")
	}
}

func TestStandardColumnNames(t *testing.T) {
	cfg := Default()
	names := cfg.StandardColumnNames()
	for _, want := range []string{"__path__", "__created_at__", "__modified_at__", "__checksum__", "__ulid__"} {
		if _, ok := names[want]; !ok {
			t.Errorf("StandardColumnNames missing %q", want)
		}
	}
}
