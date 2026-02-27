package loader

import (
	"github.com/BurntSushi/toml"
)

// TOMLLoader loads .toml files.
type TOMLLoader struct{}

func (TOMLLoader) Extensions() []string { return []string{".toml"} }

func (TOMLLoader) Load(absPath, relPath string) (*FileRecord, error) {
	data, fr, err := readFile(absPath, relPath)
	if err != nil {
		return nil, err
	}

	var raw map[string]any
	if err := toml.Unmarshal(data, &raw); err != nil {
		return nil, err
	}

	// TOML Unmarshal returns int64/float64 etc — convert to consistent types.
	fr.Records = buildRecords(normaliseMap(raw))
	return fr, nil
}

// normaliseMap converts TOML-specific types to standard Go types.
func normaliseMap(m map[string]any) map[string]any {
	result := make(map[string]any, len(m))
	for k, v := range m {
		result[k] = normaliseValue(v)
	}
	return result
}

func normaliseValue(v any) any {
	switch val := v.(type) {
	case map[string]any:
		return normaliseMap(val)
	case []any:
		out := make([]any, len(val))
		for i, elem := range val {
			out[i] = normaliseValue(elem)
		}
		return out
	default:
		return v
	}
}
