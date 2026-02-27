package validator

import (
	"fmt"
	"strings"

	"github.com/notwillk/sqlfs/internal/config"
	"github.com/notwillk/sqlfs/internal/dbml"
	"github.com/notwillk/sqlfs/internal/loader"
)

// ValidationError describes a single field-level schema violation.
type ValidationError struct {
	FilePath  string
	RecordKey string
	Field     string
	Message   string
}

func (e ValidationError) Error() string {
	return fmt.Sprintf("%s#%s: field %q: %s", e.FilePath, e.RecordKey, e.Field, e.Message)
}

// Validator checks FileRecords against a DBML schema.
type Validator struct {
	Schema *dbml.Schema
	Config *config.Config
}

// New returns a new Validator.
func New(schema *dbml.Schema, cfg *config.Config) *Validator {
	return &Validator{Schema: schema, Config: cfg}
}

// Validate validates and filters records in a FileRecord against the DBML schema.
// Returns (validRecords, warnings, fatalError).
//
//   - fail mode:   returns error on the first violation
//   - warn mode:   collects all violations as warnings, returns valid records
//   - silent mode: drops invalid records silently, returns valid records
func (v *Validator) Validate(fr *loader.FileRecord) ([]loader.Record, []ValidationError, error) {
	table := v.Schema.TableByName(fr.TableName)
	if table == nil {
		// No schema for this table: pass all records through without validation.
		return fr.Records, nil, nil
	}

	stdCols := v.Config.StandardColumnNames()
	var valid []loader.Record
	var warnings []ValidationError

	for _, rec := range fr.Records {
		errs := v.validateRecord(rec, table, stdCols, fr.FilePath)
		if len(errs) == 0 {
			valid = append(valid, rec)
			continue
		}

		switch v.Config.Invalid {
		case config.InvalidFail:
			return nil, nil, errs[0]
		case config.InvalidWarn:
			warnings = append(warnings, errs...)
			valid = append(valid, rec) // still include the record
		case config.InvalidSilent:
			// Drop the record silently.
		}
	}

	return valid, warnings, nil
}

// validateRecord checks a single record against the table's column constraints.
func (v *Validator) validateRecord(rec loader.Record, table *dbml.Table, stdCols map[string]struct{}, filePath string) []ValidationError {
	var errs []ValidationError

	// Check required columns (not null, no default).
	for _, col := range table.Columns {
		if _, isStd := stdCols[col.Name]; isStd {
			continue
		}
		if col.NotNull && col.Default == nil && !col.PK {
			if _, exists := rec.Fields[col.Name]; !exists {
				errs = append(errs, ValidationError{
					FilePath:  filePath,
					RecordKey: rec.Key,
					Field:     col.Name,
					Message:   "required field is missing",
				})
			}
		}
	}

	// Check enum constraints.
	for _, col := range table.Columns {
		val, exists := rec.Fields[col.Name]
		if !exists || val == nil {
			continue
		}
		en := v.Schema.EnumByName(col.Type.Name)
		if en == nil {
			continue
		}
		valStr := fmt.Sprintf("%v", val)
		if !enumContains(en, valStr) {
			errs = append(errs, ValidationError{
				FilePath:  filePath,
				RecordKey: rec.Key,
				Field:     col.Name,
				Message:   fmt.Sprintf("value %q is not a valid enum value for %q", valStr, col.Type.Name),
			})
		}
	}

	// Check for unknown fields (fields not in schema and not standard columns).
	colSet := make(map[string]struct{}, len(table.Columns))
	for _, col := range table.Columns {
		colSet[strings.ToLower(col.Name)] = struct{}{}
	}
	for _, std := range []string{
		v.Config.StandardColumns.Path,
		v.Config.StandardColumns.CreatedAt,
		v.Config.StandardColumns.ModifiedAt,
		v.Config.StandardColumns.Checksum,
		v.Config.StandardColumns.ULID,
	} {
		colSet[strings.ToLower(std)] = struct{}{}
	}

	for field := range rec.Fields {
		if _, known := colSet[strings.ToLower(field)]; !known {
			errs = append(errs, ValidationError{
				FilePath:  filePath,
				RecordKey: rec.Key,
				Field:     field,
				Message:   fmt.Sprintf("unknown field %q not in schema", field),
			})
		}
	}

	return errs
}

func enumContains(en *dbml.Enum, val string) bool {
	for _, ev := range en.Values {
		if ev.Name == val {
			return true
		}
	}
	return false
}
