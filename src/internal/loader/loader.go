package loader

import (
	"crypto/md5"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// EntityRef is a reference to another entity, expressed as a relative path
// without file extension (e.g. "recipes/celeriac-veloute").
// Used in YAML anchor syntax: "- &recipes/celeriac-veloute".
// Its value resolves to the __pk__ of the target entity, which equals the path itself.
type EntityRef struct {
	Path string
}

// Record is a single row loaded from a source file.
// Key is the filename stem (without entity type); Fields maps column names to values.
// Fields may contain nested []any or map[string]any for arrays/objects — the builder
// expands these into child ExpandedRecords.
type Record struct {
	Key    string
	Fields map[string]any
}

// FileRecord is the result of loading one static file.
// EntityType is the second-to-last dot segment of the filename (e.g. "recipe" from
// "celeriac-veloute.recipe.yaml"). It is left empty for files without an entity type.
// The builder uses EntityType as the target table name.
type FileRecord struct {
	EntityType string
	FilePath   string // relative path from root
	Records    []Record
	ModTime    time.Time
	CreatedAt  time.Time
	Checksum   string // hex MD5 of raw file bytes
}

// ExpandedRecord is a flattened row ready for insertion, produced by the builder
// when it shreds nested entity structures into related tables.
type ExpandedRecord struct {
	TableName  string
	PK         string         // __pk__ value
	SourcePath string         // original file relpath (for __path__, __created_at__, etc.)
	ModTime    time.Time      // for __modified_at__
	CreatedAt  time.Time      // for __created_at__
	Checksum   string         // for __checksum__
	Fields     map[string]any // scalar fields and resolved EntityRef values only
}

// Loader can parse files of specific extension(s).
type Loader interface {
	// Extensions returns the file extensions this loader handles (lowercase, with dot).
	Extensions() []string
	// Load parses a file and returns its single record.
	Load(absPath, relPath string) (*FileRecord, error)
}

// Registry holds all registered loaders and dispatches by file extension.
type Registry struct {
	loaders map[string]Loader
}

// NewRegistry returns a Registry pre-populated with all built-in loaders.
func NewRegistry() *Registry {
	r := &Registry{loaders: make(map[string]Loader)}
	r.Register(&YAMLLoader{})
	r.Register(&TOMLLoader{})
	r.Register(&HJSONLoader{})
	r.Register(&XMLLoader{})
	r.Register(&PlistLoader{})
	return r
}

// Register adds a Loader for its declared extensions.
func (r *Registry) Register(l Loader) {
	for _, ext := range l.Extensions() {
		r.loaders[strings.ToLower(ext)] = l
	}
}

// SupportedExtensions returns all registered extensions.
func (r *Registry) SupportedExtensions() []string {
	exts := make([]string, 0, len(r.loaders))
	for ext := range r.loaders {
		exts = append(exts, ext)
	}
	return exts
}

// IsSupported reports whether the file has a supported extension.
func (r *Registry) IsSupported(path string) bool {
	ext := strings.ToLower(filepath.Ext(path))
	_, ok := r.loaders[ext]
	return ok
}

// LoadFile dispatches to the appropriate loader based on file extension.
func (r *Registry) LoadFile(absPath, relPath string) (*FileRecord, error) {
	ext := strings.ToLower(filepath.Ext(absPath))
	l, ok := r.loaders[ext]
	if !ok {
		return nil, fmt.Errorf("no loader for extension %q", ext)
	}
	return l.Load(absPath, relPath)
}

// EntityType extracts the entity type from a relative file path.
// The entity type is the second-to-last dot-separated segment of the filename.
// Examples:
//
//	"recipes/celeriac-veloute.recipe.yaml" → "recipe"
//	"users/alice.users.yaml"               → "users"
//	"alice.yaml"                           → "" (no entity type)
func EntityType(relPath string) string {
	base := filepath.Base(relPath)
	ext := filepath.Ext(base)
	stem := strings.TrimSuffix(base, ext)
	lastDot := strings.LastIndex(stem, ".")
	if lastDot < 0 {
		return ""
	}
	return stem[lastDot+1:]
}

// EntityKey extracts the record key from a relative file path.
// It strips both the file extension and the entity type suffix (if present).
// Examples:
//
//	"recipes/celeriac-veloute.recipe.yaml" → "celeriac-veloute"
//	"users/alice.users.yaml"               → "alice"
//	"alice.yaml"                           → "alice"
func EntityKey(relPath string) string {
	base := filepath.Base(relPath)
	ext := filepath.Ext(base)
	stem := strings.TrimSuffix(base, ext)
	lastDot := strings.LastIndex(stem, ".")
	if lastDot < 0 {
		return stem
	}
	return stem[:lastDot]
}

// EntityPK computes the deterministic __pk__ value for an entity file.
// It is the relative path with the file extension stripped, then the entity type
// suffix stripped.
// Examples:
//
//	"recipes/celeriac-veloute.recipe.yaml" → "recipes/celeriac-veloute"
//	"users/alice.users.yaml"               → "users/alice"
func EntityPK(relPath string) string {
	// Strip file extension.
	ext := filepath.Ext(relPath)
	noExt := strings.TrimSuffix(relPath, ext)
	// Strip entity type (second extension).
	ext2 := filepath.Ext(noExt)
	if ext2 != "" {
		return strings.TrimSuffix(noExt, ext2)
	}
	return noExt
}

// flattenValue converts nested structures to JSON strings for flat storage.
// Scalars (string, int, float, bool, nil) are returned as-is.
// uint64 is included because plist unmarshals integers as uint64.
// EntityRef is returned as-is (resolved by the builder).
func flattenValue(v any) any {
	switch v.(type) {
	case string, int, int8, int16, int32, int64,
		uint, uint8, uint16, uint32, uint64,
		float32, float64, bool, nil, EntityRef:
		return v
	default:
		b, err := json.Marshal(v)
		if err != nil {
			return fmt.Sprintf("%v", v)
		}
		return string(b)
	}
}

// rawBytesChecksum computes the MD5 hex of a byte slice.
func rawBytesChecksum(data []byte) string {
	h := md5.New()
	h.Write(data)
	return fmt.Sprintf("%x", h.Sum(nil))
}

// readFile reads a file and returns its bytes plus metadata.
// EntityType is left empty; the loader sets it after parsing.
func readFile(absPath, relPath string) ([]byte, *FileRecord, error) {
	data, err := os.ReadFile(absPath)
	if err != nil {
		return nil, nil, err
	}

	info, err := os.Stat(absPath)
	if err != nil {
		return nil, nil, err
	}

	fr := &FileRecord{
		FilePath:  relPath,
		ModTime:   info.ModTime(),
		CreatedAt: fileCreatedAt(info, absPath),
		Checksum:  rawBytesChecksum(data),
	}
	return data, fr, nil
}

// buildRecord creates a single Record from a field map, flattening nested structures.
// Used by non-YAML loaders (TOML, HJSON, XML, plist) which do not support EntityRefs.
func buildRecord(key string, m map[string]any) Record {
	fields := make(map[string]any, len(m))
	for k, v := range m {
		fields[k] = flattenValue(v)
	}
	return Record{Key: key, Fields: fields}
}
