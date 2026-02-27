package loader

import (
	"encoding/json"

	hjson "github.com/hjson/hjson-go/v4"
)

// HJSONLoader loads .json, .jsonc, and .json5 files.
// It uses hjson-go which supports comments and trailing commas.
type HJSONLoader struct{}

func (HJSONLoader) Extensions() []string { return []string{".json", ".jsonc", ".json5"} }

func (HJSONLoader) Load(absPath, relPath string) (*FileRecord, error) {
	data, fr, err := readFile(absPath, relPath)
	if err != nil {
		return nil, err
	}

	// Parse HJSON to a generic map.
	var raw any
	if err := hjson.Unmarshal(data, &raw); err != nil {
		// Fall back to standard JSON if hjson fails.
		if err2 := json.Unmarshal(data, &raw); err2 != nil {
			return nil, err
		}
	}

	m, ok := raw.(map[string]any)
	if !ok {
		// If not a map, wrap in a single record.
		m = map[string]any{"root": raw}
	}

	fr.Records = buildRecords(m)
	return fr, nil
}
