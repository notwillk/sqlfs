package config

import (
	"os"
	"path/filepath"

	"gopkg.in/yaml.v3"
)

// InvalidBehavior controls how schema validation failures are handled.
type InvalidBehavior string

const (
	InvalidSilent InvalidBehavior = "silent"
	InvalidWarn   InvalidBehavior = "warn"
	InvalidFail   InvalidBehavior = "fail"
)

// StandardColumns holds the column names for the five injected standard columns.
type StandardColumns struct {
	Path       string `yaml:"path"`
	CreatedAt  string `yaml:"created_at"`
	ModifiedAt string `yaml:"modified_at"`
	Checksum   string `yaml:"checksum"`
	ULID       string `yaml:"ulid"`
}

// fileConfig is the raw YAML structure from sqlfs.yaml.
type fileConfig struct {
	Schema      string          `yaml:"schema"`
	Invalid     string          `yaml:"invalid"`
	Port        int             `yaml:"port"`
	Credentials struct {
		Username string `yaml:"username"`
		Password string `yaml:"password"`
	} `yaml:"credentials"`
	Columns StandardColumns `yaml:"columns"`
}

// Config is the fully merged, resolved configuration.
type Config struct {
	SchemaFile      string
	Invalid         InvalidBehavior
	Port            int
	UsernameEnvVar  string
	PasswordEnvVar  string
	StandardColumns StandardColumns
}

// Default returns a Config populated entirely with default values.
func Default() *Config {
	return &Config{
		SchemaFile: "schema.dbml",
		Invalid:    InvalidFail,
		Port:       5432,
		UsernameEnvVar: "SQLFS_USERNAME",
		PasswordEnvVar: "SQLFS_PASSWORD",
		StandardColumns: StandardColumns{
			Path:       "__path__",
			CreatedAt:  "__created_at__",
			ModifiedAt: "__modified_at__",
			Checksum:   "__checksum__",
			ULID:       "__ulid__",
		},
	}
}

// Load reads sqlfs.yaml from rootDir (if it exists) and merges it on top of defaults.
// If the file does not exist, defaults are returned with no error.
func Load(rootDir string) (*Config, error) {
	cfg := Default()

	cfgPath := filepath.Join(rootDir, "sqlfs.yaml")
	data, err := os.ReadFile(cfgPath)
	if os.IsNotExist(err) {
		return cfg, nil
	}
	if err != nil {
		return nil, err
	}

	var fc fileConfig
	if err := yaml.Unmarshal(data, &fc); err != nil {
		return nil, err
	}

	if fc.Schema != "" {
		cfg.SchemaFile = fc.Schema
	}
	if fc.Invalid != "" {
		cfg.Invalid = InvalidBehavior(fc.Invalid)
	}
	if fc.Port != 0 {
		cfg.Port = fc.Port
	}
	if fc.Credentials.Username != "" {
		cfg.UsernameEnvVar = fc.Credentials.Username
	}
	if fc.Credentials.Password != "" {
		cfg.PasswordEnvVar = fc.Credentials.Password
	}
	if fc.Columns.Path != "" {
		cfg.StandardColumns.Path = fc.Columns.Path
	}
	if fc.Columns.CreatedAt != "" {
		cfg.StandardColumns.CreatedAt = fc.Columns.CreatedAt
	}
	if fc.Columns.ModifiedAt != "" {
		cfg.StandardColumns.ModifiedAt = fc.Columns.ModifiedAt
	}
	if fc.Columns.Checksum != "" {
		cfg.StandardColumns.Checksum = fc.Columns.Checksum
	}
	if fc.Columns.ULID != "" {
		cfg.StandardColumns.ULID = fc.Columns.ULID
	}

	return cfg, nil
}

// WithInvalid returns a copy of cfg with the InvalidBehavior overridden if override is non-empty.
func (c *Config) WithInvalid(override string) *Config {
	if override == "" {
		return c
	}
	copy := *c
	copy.Invalid = InvalidBehavior(override)
	return &copy
}

// WithPort returns a copy of cfg with Port overridden if override > 0.
func (c *Config) WithPort(override int) *Config {
	if override <= 0 {
		return c
	}
	copy := *c
	copy.Port = override
	return &copy
}

// StandardColumnNames returns all five standard column names as a set for quick lookup.
func (c *Config) StandardColumnNames() map[string]struct{} {
	return map[string]struct{}{
		c.StandardColumns.Path:       {},
		c.StandardColumns.CreatedAt:  {},
		c.StandardColumns.ModifiedAt: {},
		c.StandardColumns.Checksum:   {},
		c.StandardColumns.ULID:       {},
	}
}
