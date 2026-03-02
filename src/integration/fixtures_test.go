// Package integration_test validates that the fixtures directory is internally
// consistent: for each fixture, the JSON schema generated from its schema.dbml
// accepts every entity file it contains.
package integration_test

import (
	"encoding/json"
	"io/fs"
	"os"
	"path/filepath"
	"testing"

	jsonvalidator "github.com/santhosh-tekuri/jsonschema/v6"

	"github.com/notwillk/sqlfs/internal/config"
	"github.com/notwillk/sqlfs/internal/dbml"
	"github.com/notwillk/sqlfs/internal/jsonschema"
	"github.com/notwillk/sqlfs/internal/loader"
)

// fixturesDir is relative to the integration test package directory (src/integration/).
const fixturesDir = "../../fixtures"

// TestFixtures_JSONSchema iterates every fixture, generates its JSON schema,
// and validates each entity file against the row schema for its table.
func TestFixtures_JSONSchema(t *testing.T) {
	entries, err := os.ReadDir(fixturesDir)
	if err != nil {
		t.Fatalf("reading fixtures dir: %v", err)
	}

	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		name := entry.Name()
		t.Run(name, func(t *testing.T) {
			validateFixture(t, filepath.Join(fixturesDir, name))
		})
	}
}

func validateFixture(t *testing.T, fixtureDir string) {
	t.Helper()

	// Load config and DBML schema.
	cfg, err := config.Load(fixtureDir)
	if err != nil {
		t.Fatalf("config.Load: %v", err)
	}
	dbmlSchema, err := dbml.ParseFile(filepath.Join(fixtureDir, cfg.SchemaFile))
	if err != nil {
		t.Fatalf("dbml.ParseFile: %v", err)
	}

	// Generate the JSON schema.
	schemaBytes, err := jsonschema.Generate(dbmlSchema, cfg)
	if err != nil {
		t.Fatalf("jsonschema.Generate: %v", err)
	}

	// Decode the schema to a Go value — v6 AddResource requires a decoded value, not an io.Reader.
	var schemaDoc any
	if err := json.Unmarshal(schemaBytes, &schemaDoc); err != nil {
		t.Fatalf("unmarshal generated schema: %v", err)
	}

	// Compile the generated schema into a validator.
	c := jsonvalidator.NewCompiler()
	if err := c.AddResource("schema.json", schemaDoc); err != nil {
		t.Fatalf("AddResource: %v", err)
	}

	// Cache compiled row schemas by table name.
	rowSchemas := make(map[string]*jsonvalidator.Schema)
	for _, tbl := range dbmlSchema.Tables {
		ref := "schema.json#/$defs/" + tbl.Name + "_row"
		sch, err := c.Compile(ref)
		if err != nil {
			t.Fatalf("compiling row schema for table %q: %v", tbl.Name, err)
		}
		rowSchemas[tbl.Name] = sch
	}

	// Walk all entity files.
	reg := loader.NewRegistry()
	err = filepath.WalkDir(fixtureDir, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}

		// Skip config and schema files.
		name := d.Name()
		if name == cfg.SchemaFile || name == "sqlfs.yaml" {
			return nil
		}
		if !reg.IsSupported(path) {
			return nil
		}

		// XML text content is always strings regardless of the declared column
		// type, so type-strict JSON schema validation does not apply.
		if filepath.Ext(name) == ".xml" {
			return nil
		}

		relPath, _ := filepath.Rel(fixtureDir, path)

		// Table name is the immediate parent directory.
		// Entity files must be one level deep: <table>/<entity>.<ext>
		tableDir := filepath.Dir(relPath)
		if tableDir == "." {
			// File sits directly in the fixture root — not an entity.
			return nil
		}
		tableName := filepath.Base(tableDir)

		t.Run(relPath, func(t *testing.T) {
			sch, ok := rowSchemas[tableName]
			if !ok {
				t.Errorf("parent dir %q does not match any table in schema", tableName)
				return
			}

			fr, err := reg.LoadFile(path, relPath)
			if err != nil {
				t.Errorf("LoadFile: %v", err)
				return
			}
			if len(fr.Records) == 0 {
				t.Error("no records loaded")
				return
			}

			// Round-trip fields through JSON to get a fully JSON-compatible
			// value (e.g. uint64 → float64) for the schema validator.
			raw, err := json.Marshal(fr.Records[0].Fields)
			if err != nil {
				t.Errorf("marshal fields: %v", err)
				return
			}
			var instance any
			if err := json.Unmarshal(raw, &instance); err != nil {
				t.Errorf("unmarshal fields: %v", err)
				return
			}

			if err := sch.Validate(instance); err != nil {
				t.Errorf("schema validation failed:\n%v", err)
			}
		})

		return nil
	})
	if err != nil {
		t.Fatalf("WalkDir: %v", err)
	}
}
