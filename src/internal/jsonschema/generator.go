package jsonschema

import (
	"encoding/json"
	"strings"

	"github.com/notwillk/sqlfs/internal/config"
	"github.com/notwillk/sqlfs/internal/dbml"
)

// Generate creates a JSON Schema document from a DBML schema.
// Standard columns are excluded from the output.
// Returns the JSON bytes.
func Generate(schema *dbml.Schema, cfg *config.Config) ([]byte, error) {
	stdCols := cfg.StandardColumnNames()
	defs := make(map[string]any)

	// Build a oneOf list pointing to each table's file schema.
	var oneOf []any
	for _, tbl := range schema.Tables {
		fileKey := tbl.Name + "_file"
		rowKey := tbl.Name + "_row"

		// File schema: top-level map where each key is a record.
		fileSchema := map[string]any{
			"type":                 "object",
			"title":                tbl.Name,
			"description":          "Matches files named " + tbl.Name + ".*",
			"additionalProperties": map[string]any{"$ref": "#/$defs/" + rowKey},
		}

		// Row schema: the columns.
		rowSchema := buildRowSchema(tbl, schema, stdCols)

		defs[fileKey] = fileSchema
		defs[rowKey] = rowSchema
		oneOf = append(oneOf, map[string]any{"$ref": "#/$defs/" + fileKey})
	}

	doc := map[string]any{
		"$schema":     "https://json-schema.org/draft/2020-12",
		"title":       "sqlfs data files",
		"description": "Validates data files for your sqlfs project",
		"$defs":       defs,
	}
	if len(oneOf) > 0 {
		doc["oneOf"] = oneOf
	}

	return json.MarshalIndent(doc, "", "  ")
}

// buildRowSchema constructs the JSON Schema for a single row in a table.
func buildRowSchema(tbl *dbml.Table, schema *dbml.Schema, stdCols map[string]struct{}) map[string]any {
	properties := make(map[string]any)
	var required []string

	for _, col := range tbl.Columns {
		// Exclude standard columns.
		if _, isStd := stdCols[col.Name]; isStd {
			continue
		}

		prop := columnSchema(col, schema)
		properties[col.Name] = prop

		// Required if not null and no default and not PK.
		if col.NotNull && col.Default == nil && !col.PK {
			required = append(required, col.Name)
		}
	}

	rowSchema := map[string]any{
		"type":                 "object",
		"properties":           properties,
		"additionalProperties": false,
	}
	if len(required) > 0 {
		rowSchema["required"] = required
	}
	if tbl.Note != "" {
		rowSchema["description"] = tbl.Note
	}

	return rowSchema
}

// columnSchema returns the JSON Schema for a single column.
func columnSchema(col *dbml.Column, schema *dbml.Schema) map[string]any {
	prop := make(map[string]any)

	// Check if the type references an enum.
	if en := schema.EnumByName(col.Type.Name); en != nil {
		prop["type"] = "string"
		vals := make([]string, len(en.Values))
		for i, v := range en.Values {
			vals[i] = v.Name
		}
		prop["enum"] = vals
		if col.Note != "" {
			prop["description"] = col.Note
		}
		return prop
	}

	jType, format := dbmlTypeToJSONSchema(col.Type.Name)
	if jType != "" {
		prop["type"] = jType
	}
	if format != "" {
		prop["format"] = format
	}

	if col.Note != "" {
		prop["description"] = col.Note
	}
	if col.Default != nil {
		prop["default"] = col.Default.Value
	}

	return prop
}

// dbmlTypeToJSONSchema maps a DBML type name to a JSON Schema type and optional format.
func dbmlTypeToJSONSchema(typeName string) (schemaType, format string) {
	switch strings.ToLower(typeName) {
	case "int", "integer", "int2", "int4", "int8", "bigint", "smallint",
		"tinyint", "mediumint", "serial", "bigserial", "smallserial":
		return "integer", ""
	case "float", "real", "double", "decimal", "numeric",
		"float4", "float8", "double precision", "money":
		return "number", ""
	case "bool", "boolean":
		return "boolean", ""
	case "date":
		return "string", "date"
	case "time", "timetz":
		return "string", "time"
	case "timestamp", "timestamptz", "datetime":
		return "string", "date-time"
	case "json", "jsonb":
		// Any value.
		return "", ""
	case "blob", "binary", "varbinary", "bytea":
		// Exclude or treat as string.
		return "string", ""
	default:
		// varchar, text, char, string, uuid, and any unknown types.
		return "string", ""
	}
}

// GenerateConfigSchema generates a JSON Schema for the sqlfs.yaml config file.
func GenerateConfigSchema() ([]byte, error) {
	doc := map[string]any{
		"$schema":     "https://json-schema.org/draft/2020-12",
		"title":       "sqlfs configuration",
		"description": "Configuration file for sqlfs (sqlfs.yaml)",
		"type":        "object",
		"properties": map[string]any{
			"schema": map[string]any{
				"type":        "string",
				"description": "Path to the DBML schema file (relative to root)",
				"default":     "schema.dbml",
			},
			"invalid": map[string]any{
				"type":        "string",
				"enum":        []string{"silent", "warn", "fail"},
				"description": "Behavior when a file fails schema validation",
				"default":     "fail",
			},
			"port": map[string]any{
				"type":        "integer",
				"description": "Port for the SQL server (serve command)",
				"default":     5432,
				"minimum":     1,
				"maximum":     65535,
			},
			"credentials": map[string]any{
				"type":        "object",
				"description": "Environment variable names for SQL server credentials",
				"properties": map[string]any{
					"username": map[string]any{
						"type":        "string",
						"description": "Environment variable name for the username",
						"default":     "SQLFS_USERNAME",
					},
					"password": map[string]any{
						"type":        "string",
						"description": "Environment variable name for the password",
						"default":     "SQLFS_PASSWORD",
					},
				},
				"additionalProperties": false,
			},
			"columns": map[string]any{
				"type":        "object",
				"description": "Custom names for the standard injected columns",
				"properties": map[string]any{
					"path":        columnNameProp("__path__"),
					"created_at":  columnNameProp("__created_at__"),
					"modified_at": columnNameProp("__modified_at__"),
					"checksum":    columnNameProp("__checksum__"),
					"ulid":        columnNameProp("__ulid__"),
				},
				"additionalProperties": false,
			},
		},
		"additionalProperties": false,
	}
	return json.MarshalIndent(doc, "", "  ")
}

func columnNameProp(defaultVal string) map[string]any {
	return map[string]any{
		"type":        "string",
		"description": "Column name override (default: " + defaultVal + ")",
		"default":     defaultVal,
	}
}
