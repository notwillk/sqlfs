package builder

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"log"
	"math/rand"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/oklog/ulid/v2"

	"github.com/notwillk/sqlfs/internal/config"
	"github.com/notwillk/sqlfs/internal/dbml"
	"github.com/notwillk/sqlfs/internal/loader"
	"github.com/notwillk/sqlfs/internal/schema"
	"github.com/notwillk/sqlfs/internal/sqlite"
	"github.com/notwillk/sqlfs/internal/validator"
)

// Options configures a build run.
type Options struct {
	RootDir    string
	OutputFile string
	Config     *config.Config
}

// Result holds the outcome of a build.
type Result struct {
	TablesBuilt  int
	RecordsTotal int
	Warnings     []validator.ValidationError
	Duration     time.Duration
}

// Build executes the full build pipeline.
// If schema.dbml exists it is used for DDL and validation (DBML mode).
// If schema.dbml does not exist the schema is inferred from the entity files
// (schema-less mode) and all user columns are stored as TEXT.
func Build(ctx context.Context, opts Options) (*Result, error) {
	start := time.Now()

	cfg := opts.Config
	if cfg == nil {
		var err error
		cfg, err = config.Load(opts.RootDir)
		if err != nil {
			return nil, fmt.Errorf("loading config: %w", err)
		}
	}

	schemaPath := filepath.Join(opts.RootDir, cfg.SchemaFile)
	if _, err := os.Stat(schemaPath); errors.Is(err, os.ErrNotExist) {
		return buildSchemaless(ctx, opts, cfg, start)
	}
	return buildWithDBML(ctx, opts, cfg, start)
}

// ---------------------------------------------------------------------------
// DBML mode
// ---------------------------------------------------------------------------

func buildWithDBML(ctx context.Context, opts Options, cfg *config.Config, start time.Time) (*Result, error) {
	result := &Result{}

	schemaPath := filepath.Join(opts.RootDir, cfg.SchemaFile)
	dbmlSchema, err := dbml.ParseFile(schemaPath)
	if err != nil {
		return nil, fmt.Errorf("parsing schema %q: %w", schemaPath, err)
	}

	gen := schema.New(dbmlSchema, cfg)
	ddl, err := gen.DDL()
	if err != nil {
		return nil, fmt.Errorf("generating DDL: %w", err)
	}

	db, err := sqlite.OpenMemory()
	if err != nil {
		return nil, fmt.Errorf("opening database: %w", err)
	}
	defer db.Close()

	if err := db.ExecDDL(ddl); err != nil {
		return nil, fmt.Errorf("applying DDL: %w", err)
	}

	reg := loader.NewRegistry()
	val := validator.New(dbmlSchema, cfg)
	tablesSeen := make(map[string]struct{})

	if err := filepath.WalkDir(opts.RootDir, func(path string, d fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if d.IsDir() && strings.HasPrefix(d.Name(), ".") {
			return filepath.SkipDir
		}
		if d.IsDir() {
			return nil
		}

		name := d.Name()
		if name == cfg.SchemaFile || name == "sqlfs.yaml" {
			return nil
		}
		if !reg.IsSupported(path) {
			return nil
		}

		relPath, err := filepath.Rel(opts.RootDir, path)
		if err != nil {
			return err
		}

		entityType := loader.EntityType(relPath)
		if entityType == "" {
			log.Printf("warning: skipping %q: no entity type in filename (expected name.entity-type.ext)", relPath)
			return nil
		}

		fr, err := reg.LoadFile(path, relPath)
		if err != nil {
			return fmt.Errorf("loading %q: %w", relPath, err)
		}
		if len(fr.Records) == 0 {
			return nil
		}

		fr.EntityType = entityType

		// For validation, use only the scalar fields of the primary record.
		// Array/object fields will be expanded into child tables — they are not
		// directly validated against the DBML schema.
		flatFR := scalarFileRecord(fr)
		valid, warns, err := val.Validate(flatFR)
		if err != nil {
			return fmt.Errorf("validating %q: %w", relPath, err)
		}
		result.Warnings = append(result.Warnings, warns...)

		if len(valid) == 0 {
			return nil
		}

		// Expand and insert.
		pk := loader.EntityPK(relPath)
		expanded := expandEntity(entityType, pk, fr, fr.Records[0].Fields, nil)
		for _, exp := range expanded {
			if err := insertExpandedRecord(db, exp, cfg); err != nil {
				return fmt.Errorf("inserting from %q: %w", relPath, err)
			}
			result.RecordsTotal++
		}
		tablesSeen[entityType] = struct{}{}
		return nil
	}); err != nil {
		return nil, err
	}

	result.TablesBuilt = len(tablesSeen)

	if err := os.MkdirAll(filepath.Dir(opts.OutputFile), 0755); err != nil {
		return nil, fmt.Errorf("creating output directory: %w", err)
	}
	if err := db.SaveTo(opts.OutputFile); err != nil {
		return nil, fmt.Errorf("saving database: %w", err)
	}

	result.Duration = time.Since(start)
	return result, nil
}

// scalarFileRecord returns a copy of fr whose Records contain only scalar fields.
// Array and object fields are stripped so validation only checks flat columns.
func scalarFileRecord(fr *loader.FileRecord) *loader.FileRecord {
	flat := &loader.FileRecord{
		EntityType: fr.EntityType,
		FilePath:   fr.FilePath,
		Records:    make([]loader.Record, 0, len(fr.Records)),
		ModTime:    fr.ModTime,
		CreatedAt:  fr.CreatedAt,
		Checksum:   fr.Checksum,
	}
	for _, rec := range fr.Records {
		scalar := loader.Record{Key: rec.Key, Fields: make(map[string]any)}
		for k, v := range rec.Fields {
			switch v.(type) {
			case []any, map[string]any:
				// Skip — will be expanded into child tables.
			default:
				scalar.Fields[k] = v
			}
		}
		flat.Records = append(flat.Records, scalar)
	}
	return flat
}

// ---------------------------------------------------------------------------
// Schema-less mode
// ---------------------------------------------------------------------------

// discoveredTable tracks columns found for one table during the scan pass.
type discoveredTable struct {
	name    string
	columns []string
	seen    map[string]struct{}
}

func newDiscoveredTable(name string) *discoveredTable {
	return &discoveredTable{name: name, seen: make(map[string]struct{})}
}

func (t *discoveredTable) addColumn(col string) {
	if _, ok := t.seen[col]; !ok {
		t.seen[col] = struct{}{}
		t.columns = append(t.columns, col)
	}
}

// discoverTables walks rootDir and collects the table/column structure from
// entity files. Returns the table map and a pk→entityType index.
func discoverTables(rootDir string, cfg *config.Config, reg *loader.Registry) (map[string]*discoveredTable, map[string]string, error) {
	tables := make(map[string]*discoveredTable)
	pathIndex := make(map[string]string)

	err := filepath.WalkDir(rootDir, func(path string, d fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if d.IsDir() && strings.HasPrefix(d.Name(), ".") {
			return filepath.SkipDir
		}
		if d.IsDir() {
			return nil
		}
		name := d.Name()
		if name == cfg.SchemaFile || name == "sqlfs.yaml" {
			return nil
		}
		if !reg.IsSupported(path) {
			return nil
		}

		relPath, err := filepath.Rel(rootDir, path)
		if err != nil {
			return err
		}

		entityType := loader.EntityType(relPath)
		if entityType == "" {
			return nil
		}

		pk := loader.EntityPK(relPath)
		pathIndex[pk] = entityType

		fr, err := reg.LoadFile(path, relPath)
		if err != nil {
			return nil // skip on error in discovery
		}
		if len(fr.Records) > 0 {
			discoverColumns(entityType, fr.Records[0].Fields, tables, pathIndex)
		}
		return nil
	})
	return tables, pathIndex, err
}

func buildSchemaless(ctx context.Context, opts Options, cfg *config.Config, start time.Time) (*Result, error) {
	result := &Result{}
	reg := loader.NewRegistry()

	// --- Discovery pass ---
	tables, pathIndex, err := discoverTables(opts.RootDir, cfg, reg)
	if err != nil {
		return nil, err
	}

	// --- DDL generation ---
	sc := cfg.StandardColumns
	var ddl []string
	for _, tbl := range tables {
		var cols []string
		cols = append(cols, fmt.Sprintf(`  %s TEXT PRIMARY KEY`, sqliteQuote(sc.PK)))
		cols = append(cols, fmt.Sprintf(`  %s TEXT`, sqliteQuote(sc.Path)))
		cols = append(cols, fmt.Sprintf(`  %s TEXT`, sqliteQuote(sc.CreatedAt)))
		cols = append(cols, fmt.Sprintf(`  %s TEXT`, sqliteQuote(sc.ModifiedAt)))
		cols = append(cols, fmt.Sprintf(`  %s TEXT`, sqliteQuote(sc.Checksum)))
		cols = append(cols, fmt.Sprintf(`  %s TEXT`, sqliteQuote(sc.ULID)))
		for _, col := range tbl.columns {
			cols = append(cols, fmt.Sprintf(`  %s TEXT`, sqliteQuote(col)))
		}
		ddl = append(ddl, fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (\n%s\n)",
			sqliteQuote(tbl.name), strings.Join(cols, ",\n")))
	}

	db, err := sqlite.OpenMemory()
	if err != nil {
		return nil, fmt.Errorf("opening database: %w", err)
	}
	defer db.Close()

	if err := db.ExecDDL(ddl); err != nil {
		return nil, fmt.Errorf("applying DDL: %w", err)
	}

	// --- Insert pass ---
	tablesSeen := make(map[string]struct{})

	if err := filepath.WalkDir(opts.RootDir, func(path string, d fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if d.IsDir() && strings.HasPrefix(d.Name(), ".") {
			return filepath.SkipDir
		}
		if d.IsDir() {
			return nil
		}
		name := d.Name()
		if name == cfg.SchemaFile || name == "sqlfs.yaml" {
			return nil
		}
		if !reg.IsSupported(path) {
			return nil
		}

		relPath, err := filepath.Rel(opts.RootDir, path)
		if err != nil {
			return err
		}

		entityType := loader.EntityType(relPath)
		if entityType == "" {
			return nil
		}

		fr, err := reg.LoadFile(path, relPath)
		if err != nil {
			return fmt.Errorf("loading %q: %w", relPath, err)
		}
		if len(fr.Records) == 0 {
			return nil
		}

		pk := loader.EntityPK(relPath)
		expanded := expandEntity(entityType, pk, fr, fr.Records[0].Fields, pathIndex)
		for _, exp := range expanded {
			if err := insertExpandedRecord(db, exp, cfg); err != nil {
				log.Printf("warning: insert error for table %s pk %s: %v", exp.TableName, exp.PK, err)
			} else {
				result.RecordsTotal++
				tablesSeen[exp.TableName] = struct{}{}
			}
		}
		return nil
	}); err != nil {
		return nil, err
	}

	result.TablesBuilt = len(tablesSeen)

	if err := os.MkdirAll(filepath.Dir(opts.OutputFile), 0755); err != nil {
		return nil, fmt.Errorf("creating output directory: %w", err)
	}
	if err := db.SaveTo(opts.OutputFile); err != nil {
		return nil, fmt.Errorf("saving database: %w", err)
	}

	result.Duration = time.Since(start)
	return result, nil
}

// discoverColumns walks a field map and records column names for each table.
func discoverColumns(entityType string, fields map[string]any, tables map[string]*discoveredTable, pathIndex map[string]string) {
	tbl, ok := tables[entityType]
	if !ok {
		tbl = newDiscoveredTable(entityType)
		tables[entityType] = tbl
	}

	for key, val := range fields {
		switch v := val.(type) {
		case []any:
			childType := entityType + "_" + key
			parentFKCol := entityType + "_pk"
			childTbl, ok := tables[childType]
			if !ok {
				childTbl = newDiscoveredTable(childType)
				tables[childType] = childTbl
			}
			childTbl.addColumn(parentFKCol)
			discoverArrayColumns(childType, v, tables, pathIndex)
		default:
			_ = v
			tbl.addColumn(key)
		}
	}
}

func discoverArrayColumns(childType string, elems []any, tables map[string]*discoveredTable, pathIndex map[string]string) {
	tbl := tables[childType]
	for _, elem := range elems {
		switch e := elem.(type) {
		case map[string]any:
			discoverColumns(childType, e, tables, pathIndex)
		case loader.EntityRef:
			refEntityType := pathIndex[e.Path]
			if refEntityType == "" {
				tbl.addColumn("ref_pk")
			} else {
				tbl.addColumn(refEntityType + "_pk")
			}
		default:
			tbl.addColumn("value")
		}
	}
}

// ---------------------------------------------------------------------------
// Entity expansion
// ---------------------------------------------------------------------------

// expandEntity shreds an entity's raw fields into a primary ExpandedRecord plus
// child ExpandedRecords for each nested array field.
func expandEntity(entityType, pk string, fr *loader.FileRecord, fields map[string]any, pathIndex map[string]string) []*loader.ExpandedRecord {
	primary := &loader.ExpandedRecord{
		TableName:  entityType,
		PK:         pk,
		SourcePath: fr.FilePath,
		ModTime:    fr.ModTime,
		CreatedAt:  fr.CreatedAt,
		Checksum:   fr.Checksum,
		Fields:     make(map[string]any),
	}

	var all []*loader.ExpandedRecord
	all = append(all, primary)

	for key, val := range fields {
		switch v := val.(type) {
		case []any:
			children := expandArray(entityType, pk, key, fr, v, pathIndex)
			all = append(all, children...)
		case loader.EntityRef:
			primary.Fields[key] = v.Path
		default:
			primary.Fields[key] = flattenScalar(val)
		}
	}

	return all
}

// expandArray creates child ExpandedRecords from one array field.
func expandArray(parentType, parentPK, arrayKey string, fr *loader.FileRecord, elems []any, pathIndex map[string]string) []*loader.ExpandedRecord {
	childTable := parentType + "_" + arrayKey
	parentFKCol := parentType + "_pk"
	var all []*loader.ExpandedRecord

	for i, elem := range elems {
		childPK := parentPK + "#" + childTable + "-" + strconv.Itoa(i)

		switch e := elem.(type) {
		case map[string]any:
			// Nested object → recursively expand with FK injected.
			childFields := make(map[string]any, len(e)+1)
			childFields[parentFKCol] = parentPK
			for k, v := range e {
				childFields[k] = v
			}
			childFR := &loader.FileRecord{
				EntityType: childTable,
				FilePath:   fr.FilePath,
				ModTime:    fr.ModTime,
				CreatedAt:  fr.CreatedAt,
				Checksum:   fr.Checksum,
			}
			children := expandEntity(childTable, childPK, childFR, childFields, pathIndex)
			all = append(all, children...)

		case loader.EntityRef:
			// Reference → join record.
			refEntityType := ""
			if pathIndex != nil {
				refEntityType = pathIndex[e.Path]
			}
			refFKCol := "ref_pk"
			if refEntityType != "" {
				refFKCol = refEntityType + "_pk"
			}
			join := &loader.ExpandedRecord{
				TableName:  childTable,
				PK:         childPK,
				SourcePath: fr.FilePath,
				ModTime:    fr.ModTime,
				CreatedAt:  fr.CreatedAt,
				Checksum:   fr.Checksum,
				Fields: map[string]any{
					parentFKCol: parentPK,
					refFKCol:    e.Path,
				},
			}
			all = append(all, join)

		default:
			// Scalar → value record.
			rec := &loader.ExpandedRecord{
				TableName:  childTable,
				PK:         childPK,
				SourcePath: fr.FilePath,
				ModTime:    fr.ModTime,
				CreatedAt:  fr.CreatedAt,
				Checksum:   fr.Checksum,
				Fields: map[string]any{
					parentFKCol: parentPK,
					"value":     flattenScalar(elem),
				},
			}
			all = append(all, rec)
		}
	}

	return all
}

// flattenScalar converts a value to a scalar suitable for SQLite.
// Nested structures (maps, slices) are JSON-encoded as strings.
func flattenScalar(v any) any {
	switch v.(type) {
	case string, int, int8, int16, int32, int64,
		uint, uint8, uint16, uint32, uint64,
		float32, float64, bool, nil:
		return v
	default:
		b, err := json.Marshal(v)
		if err != nil {
			return fmt.Sprintf("%v", v)
		}
		return string(b)
	}
}

// insertExpandedRecord inserts one expanded record into SQLite.
func insertExpandedRecord(db *sqlite.DB, rec *loader.ExpandedRecord, cfg *config.Config) error {
	sc := cfg.StandardColumns

	cols := make([]string, 0, len(rec.Fields)+6)
	vals := make([]any, 0, len(rec.Fields)+6)

	for col, val := range rec.Fields {
		if ref, ok := val.(loader.EntityRef); ok {
			val = ref.Path
		}
		cols = append(cols, col)
		vals = append(vals, val)
	}

	entropy := rand.New(rand.NewSource(time.Now().UnixNano()))
	id := ulid.MustNew(ulid.Timestamp(rec.CreatedAt), entropy)

	cols = append(cols, sc.PK, sc.Path, sc.CreatedAt, sc.ModifiedAt, sc.Checksum, sc.ULID)
	vals = append(vals,
		rec.PK,
		rec.SourcePath+"#"+rec.PK,
		rec.CreatedAt.UTC().Format(time.RFC3339),
		rec.ModTime.UTC().Format(time.RFC3339),
		rec.Checksum,
		id.String(),
	)

	if err := db.InsertRecord(rec.TableName, cols, vals); err != nil {
		log.Printf("warning: insert error for table %s pk %s: %v", rec.TableName, rec.PK, err)
		return err
	}
	return nil
}

func sqliteQuote(name string) string {
	return `"` + strings.ReplaceAll(name, `"`, `""`) + `"`
}
