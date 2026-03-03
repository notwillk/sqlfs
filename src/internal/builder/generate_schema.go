package builder

import (
	"fmt"
	"sort"
	"strings"

	"github.com/notwillk/sqlfs/internal/config"
	"github.com/notwillk/sqlfs/internal/loader"
)

// GenerateSchemaOptions configures a schema generation run.
type GenerateSchemaOptions struct {
	RootDir string
	Config  *config.Config
}

// GenerateSchema inspects entity files in RootDir and returns a DBML schema
// string representing the discovered table and column structure.
// All columns are typed as varchar; standard columns (__pk__ etc.) are omitted
// since they are added automatically at build time.
func GenerateSchema(opts GenerateSchemaOptions) (string, error) {
	cfg := opts.Config
	if cfg == nil {
		var err error
		cfg, err = config.Load(opts.RootDir)
		if err != nil {
			return "", fmt.Errorf("loading config: %w", err)
		}
	}

	reg := loader.NewRegistry()
	tables, _, err := discoverTables(opts.RootDir, cfg, reg)
	if err != nil {
		return "", fmt.Errorf("discovering schema: %w", err)
	}

	if len(tables) == 0 {
		return "", fmt.Errorf("no entity files found in %q", opts.RootDir)
	}

	return renderDBML(tables, cfg), nil
}

// DiscoverColumnMap runs the discovery pass and returns a map of
// tableName → []columnName (in discovery order, standard columns excluded).
// Used by external commands (e.g. json-schema) that need the inferred structure.
func DiscoverColumnMap(rootDir string, cfg *config.Config) (map[string][]string, error) {
	if cfg == nil {
		var err error
		cfg, err = config.Load(rootDir)
		if err != nil {
			return nil, fmt.Errorf("loading config: %w", err)
		}
	}

	reg := loader.NewRegistry()
	tables, _, err := discoverTables(rootDir, cfg, reg)
	if err != nil {
		return nil, fmt.Errorf("discovering schema: %w", err)
	}

	stdCols := cfg.StandardColumnNames()
	out := make(map[string][]string, len(tables))
	for name, tbl := range tables {
		var cols []string
		for _, col := range tbl.columns {
			if _, isStd := stdCols[col]; !isStd {
				cols = append(cols, col)
			}
		}
		out[name] = cols
	}
	return out, nil
}

// renderDBML serialises the discovered tables as a DBML string.
func renderDBML(tables map[string]*discoveredTable, cfg *config.Config) string {
	stdCols := cfg.StandardColumnNames()

	// Stable output: sort table names alphabetically.
	names := make([]string, 0, len(tables))
	for n := range tables {
		names = append(names, n)
	}
	sort.Strings(names)

	var sb strings.Builder
	for i, name := range names {
		tbl := tables[name]
		if i > 0 {
			sb.WriteString("\n")
		}
		fmt.Fprintf(&sb, "Table %s {\n", name)
		for _, col := range tbl.columns {
			if _, isStd := stdCols[col]; isStd {
				continue // standard columns are managed automatically
			}
			fmt.Fprintf(&sb, "  %s varchar\n", col)
		}
		sb.WriteString("}\n")
	}
	return sb.String()
}
