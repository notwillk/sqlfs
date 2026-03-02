package builder

import (
	"context"
	"fmt"
	"io/fs"
	"log"
	"math/rand"
	"os"
	"path/filepath"
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

// Build executes the full build pipeline:
// 1. Parse schema.dbml
// 2. Create SQLite schema
// 3. Walk root directory, load each file, duck-type its table, validate, insert
// 4. Write output database
func Build(ctx context.Context, opts Options) (*Result, error) {
	start := time.Now()
	result := &Result{}

	// Load config if not provided.
	cfg := opts.Config
	if cfg == nil {
		var err error
		cfg, err = config.Load(opts.RootDir)
		if err != nil {
			return nil, fmt.Errorf("loading config: %w", err)
		}
	}

	// Parse schema.dbml.
	schemaPath := filepath.Join(opts.RootDir, cfg.SchemaFile)
	dbmlSchema, err := dbml.ParseFile(schemaPath)
	if err != nil {
		return nil, fmt.Errorf("parsing schema %q: %w", schemaPath, err)
	}

	// Generate and execute DDL.
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

	// Set up file registry and validator.
	reg := loader.NewRegistry()
	val := validator.New(dbmlSchema, cfg)
	stdCols := cfg.StandardColumnNames()

	// Track tables that have been populated.
	tablesSeen := make(map[string]struct{})

	// Walk the root directory.
	if err := filepath.WalkDir(opts.RootDir, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}

		// Skip hidden directories.
		if d.IsDir() && strings.HasPrefix(d.Name(), ".") {
			return filepath.SkipDir
		}
		if d.IsDir() {
			return nil
		}

		// Skip special files.
		name := d.Name()
		if name == cfg.SchemaFile || name == "sqlfs.yaml" {
			return nil
		}

		// Skip unsupported extensions.
		if !reg.IsSupported(path) {
			return nil
		}

		relPath, err := filepath.Rel(opts.RootDir, path)
		if err != nil {
			return err
		}

		// Load file.
		fr, err := reg.LoadFile(path, relPath)
		if err != nil {
			return fmt.Errorf("loading %q: %w", relPath, err)
		}

		if len(fr.Records) == 0 {
			return nil
		}

		// Duck-type: determine which schema table this entity belongs to.
		matched := matchTable(dbmlSchema.Tables, fr.Records[0].Fields, stdCols)
		if matched == nil {
			log.Printf("warning: skipping %q: fields do not match any schema table", relPath)
			return nil
		}
		fr.TableName = matched.Name

		// Validate records.
		valid, warns, err := val.Validate(fr)
		if err != nil {
			return fmt.Errorf("validating %q: %w", relPath, err)
		}
		result.Warnings = append(result.Warnings, warns...)

		// Insert valid records.
		for _, rec := range valid {
			if err := insertRecord(db, fr, rec, cfg); err != nil {
				return fmt.Errorf("inserting record %q#%s: %w", relPath, rec.Key, err)
			}
			result.RecordsTotal++
		}

		if len(valid) > 0 {
			tablesSeen[fr.TableName] = struct{}{}
		}

		return nil
	}); err != nil {
		return nil, err
	}

	result.TablesBuilt = len(tablesSeen)

	// Ensure output directory exists.
	if err := os.MkdirAll(filepath.Dir(opts.OutputFile), 0755); err != nil {
		return nil, fmt.Errorf("creating output directory: %w", err)
	}

	// Write output.
	if err := db.SaveTo(opts.OutputFile); err != nil {
		return nil, fmt.Errorf("saving database: %w", err)
	}

	result.Duration = time.Since(start)
	return result, nil
}

// matchTable finds the schema table whose columns best match the given fields.
// Standard column names are excluded from matching.
// Returns nil if no table has any matching columns.
func matchTable(tables []*dbml.Table, fields map[string]any, stdCols map[string]struct{}) *dbml.Table {
	fieldSet := make(map[string]struct{}, len(fields))
	for k := range fields {
		if _, isStd := stdCols[strings.ToLower(k)]; !isStd {
			fieldSet[strings.ToLower(k)] = struct{}{}
		}
	}

	var best *dbml.Table
	bestScore := -1

	for _, t := range tables {
		score := 0
		for _, col := range t.Columns {
			if _, ok := fieldSet[strings.ToLower(col.Name)]; ok {
				score++
			}
		}
		// Prefer higher score; break ties alphabetically for determinism.
		if score > bestScore || (score == bestScore && best != nil && t.Name < best.Name) {
			bestScore = score
			best = t
		}
	}

	if bestScore <= 0 {
		return nil
	}
	return best
}

// insertRecord inserts a single record into the SQLite database.
func insertRecord(db *sqlite.DB, fr *loader.FileRecord, rec loader.Record, cfg *config.Config) error {
	sc := cfg.StandardColumns

	cols := make([]string, 0, len(rec.Fields)+5)
	vals := make([]any, 0, len(rec.Fields)+5)

	for col, val := range rec.Fields {
		cols = append(cols, col)
		vals = append(vals, val)
	}

	// Standard columns.
	path := fr.FilePath + "#" + rec.Key
	createdAt := fr.CreatedAt
	modifiedAt := fr.ModTime

	// Generate ULID with the file's created-at time as the timestamp component.
	entropy := rand.New(rand.NewSource(time.Now().UnixNano()))
	id := ulid.MustNew(ulid.Timestamp(createdAt), entropy)

	cols = append(cols,
		sc.Path, sc.CreatedAt, sc.ModifiedAt, sc.Checksum, sc.ULID,
	)
	vals = append(vals,
		path,
		createdAt.UTC().Format(time.RFC3339),
		modifiedAt.UTC().Format(time.RFC3339),
		fr.Checksum,
		id.String(),
	)

	if err := db.InsertRecord(fr.TableName, cols, vals); err != nil {
		log.Printf("warning: insert error for %s#%s: %v", fr.FilePath, rec.Key, err)
		return err
	}
	return nil
}
