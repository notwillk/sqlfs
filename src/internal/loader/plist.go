package loader

import (
	"howett.net/plist"
)

// PlistLoader loads .plist files (Apple Property List format).
type PlistLoader struct{}

func (PlistLoader) Extensions() []string { return []string{".plist"} }

func (PlistLoader) Load(absPath, relPath string) (*FileRecord, error) {
	data, fr, err := readFile(absPath, relPath)
	if err != nil {
		return nil, err
	}

	var raw any
	if _, err := plist.Unmarshal(data, &raw); err != nil {
		return nil, err
	}

	m, ok := raw.(map[string]any)
	if !ok {
		m = map[string]any{"value": raw}
	}

	fr.Records = []Record{buildRecord(recordKey(relPath), m)}
	return fr, nil
}
