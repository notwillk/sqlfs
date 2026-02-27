package schema

import (
	"fmt"
	"strings"

	"github.com/notwillk/sqlfs/internal/config"
	"github.com/notwillk/sqlfs/internal/dbml"
)

// Generator converts a DBML schema AST into SQLite DDL statements.
type Generator struct {
	Schema *dbml.Schema
	Config *config.Config
}

// New returns a new Generator.
func New(schema *dbml.Schema, cfg *config.Config) *Generator {
	return &Generator{Schema: schema, Config: cfg}
}

// DDL returns a slice of CREATE TABLE statements (one per table).
func (g *Generator) DDL() ([]string, error) {
	var stmts []string
	for _, t := range g.Schema.Tables {
		stmt, err := g.CreateTableSQL(t)
		if err != nil {
			return nil, err
		}
		stmts = append(stmts, stmt)
		// Index statements.
		for _, idx := range t.Indexes {
			if idx.PK {
				continue // handled in CREATE TABLE
			}
			idxSQL := g.createIndexSQL(t.Name, idx)
			if idxSQL != "" {
				stmts = append(stmts, idxSQL)
			}
		}
	}
	return stmts, nil
}

// CreateTableSQL returns the CREATE TABLE statement for a single table,
// appending the five standard columns after the user-defined columns.
func (g *Generator) CreateTableSQL(t *dbml.Table) (string, error) {
	sc := g.Config.StandardColumns

	var cols []string
	for _, col := range t.Columns {
		colSQL, err := g.columnDef(col)
		if err != nil {
			return "", err
		}
		cols = append(cols, "  "+colSQL)
	}

	// Standard columns.
	cols = append(cols, fmt.Sprintf("  %s TEXT", sqliteName(sc.Path)))
	cols = append(cols, fmt.Sprintf("  %s TEXT", sqliteName(sc.CreatedAt)))
	cols = append(cols, fmt.Sprintf("  %s TEXT", sqliteName(sc.ModifiedAt)))
	cols = append(cols, fmt.Sprintf("  %s TEXT", sqliteName(sc.Checksum)))
	cols = append(cols, fmt.Sprintf("  %s TEXT", sqliteName(sc.ULID)))

	tableName := sqliteName(t.Name)
	return fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (\n%s\n)", tableName, strings.Join(cols, ",\n")), nil
}

func (g *Generator) columnDef(col *dbml.Column) (string, error) {
	affinity := DBMLTypeToSQLite(col.Type)
	name := sqliteName(col.Name)

	var parts []string
	parts = append(parts, name+" "+affinity)

	if col.PK && col.Increment && strings.ToLower(affinity) == "integer" {
		parts = append(parts, "PRIMARY KEY AUTOINCREMENT")
	} else if col.PK {
		parts = append(parts, "PRIMARY KEY")
	}

	if col.NotNull && !col.PK {
		parts = append(parts, "NOT NULL")
	}
	if col.Unique && !col.PK {
		parts = append(parts, "UNIQUE")
	}
	if col.Default != nil {
		def, err := defaultSQL(col.Default)
		if err != nil {
			return "", err
		}
		parts = append(parts, "DEFAULT "+def)
	}

	return strings.Join(parts, " "), nil
}

func defaultSQL(dv *dbml.DefaultValue) (string, error) {
	switch dv.Kind {
	case dbml.DefaultString:
		return "'" + strings.ReplaceAll(dv.Value, "'", "''") + "'", nil
	case dbml.DefaultNumber:
		return dv.Value, nil
	case dbml.DefaultBool:
		if dv.Value == "true" {
			return "1", nil
		}
		return "0", nil
	case dbml.DefaultNull:
		return "NULL", nil
	case dbml.DefaultExpr:
		return "(" + dv.Value + ")", nil
	}
	return "", fmt.Errorf("unknown default kind %d", dv.Kind)
}

func (g *Generator) createIndexSQL(tableName string, idx *dbml.Index) string {
	if len(idx.Columns) == 0 {
		return ""
	}
	unique := ""
	if idx.Unique {
		unique = "UNIQUE "
	}
	name := idx.Name
	if name == "" {
		name = fmt.Sprintf("idx_%s_%s", tableName, strings.Join(idx.Columns, "_"))
	}
	cols := make([]string, len(idx.Columns))
	for i, c := range idx.Columns {
		cols[i] = sqliteName(c)
	}
	return fmt.Sprintf("CREATE %sINDEX IF NOT EXISTS %s ON %s (%s)",
		unique, sqliteName(name), sqliteName(tableName), strings.Join(cols, ", "))
}

// DBMLTypeToSQLite maps a DBML column type to a SQLite affinity type.
func DBMLTypeToSQLite(ct dbml.ColumnType) string {
	switch strings.ToLower(ct.Name) {
	case "int", "integer", "int2", "int4", "int8", "bigint", "smallint", "tinyint",
		"mediumint", "serial", "bigserial", "smallserial":
		return "INTEGER"
	case "float", "real", "double", "decimal", "numeric", "float4", "float8",
		"double precision", "money":
		return "REAL"
	case "bool", "boolean":
		return "INTEGER" // SQLite has no boolean type
	case "blob", "binary", "varbinary", "bytea":
		return "BLOB"
	default:
		// varchar, text, char, string, uuid, date, time, timestamp, json, jsonb, enum refs, etc.
		return "TEXT"
	}
}

// sqliteName quotes an identifier if necessary.
func sqliteName(name string) string {
	// SQLite uses double-quotes for identifiers.
	// We quote all names to avoid conflicts with reserved words.
	return `"` + strings.ReplaceAll(name, `"`, `""`) + `"`
}
