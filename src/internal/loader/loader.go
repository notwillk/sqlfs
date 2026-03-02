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

// Record is a single row loaded from a source file.
// Key is the filename stem; Fields maps column names to values.
type Record struct {
	Key    string
	Fields map[string]any
}

// FileRecord is the result of loading one static file.
// TableName is left empty by loaders; the builder sets it via duck-typing.
type FileRecord struct {
	TableName string
	FilePath  string // relative path from root
	Records   []Record
	ModTime   time.Time
	CreatedAt time.Time
	Checksum  string // hex MD5 of raw file bytes
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

// recordKey derives the record key from a file's relative path (filename stem).
func recordKey(relPath string) string {
	base := filepath.Base(relPath)
	ext := filepath.Ext(base)
	return strings.TrimSuffix(base, ext)
}

// flattenValue converts nested structures to JSON strings for flat storage.
// Scalars (string, int, float, bool, nil) are returned as-is.
func flattenValue(v any) any {
	switch v.(type) {
	case string, int, int64, float64, bool, nil:
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
// TableName is left empty; the builder sets it via duck-typing.
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

// buildRecord creates a single Record from a flat field map.
// key is the filename stem; all map values are flattened to scalar-or-JSON-string.
func buildRecord(key string, m map[string]any) Record {
	fields := make(map[string]any, len(m))
	for k, v := range m {
		fields[k] = flattenValue(v)
	}
	return Record{Key: key, Fields: fields}
}
