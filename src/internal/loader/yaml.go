package loader

import (
	"gopkg.in/yaml.v3"
)

// YAMLLoader loads .yaml and .yml files.
type YAMLLoader struct{}

func (YAMLLoader) Extensions() []string { return []string{".yaml", ".yml"} }

func (YAMLLoader) Load(absPath, relPath string) (*FileRecord, error) {
	data, fr, err := readFile(absPath, relPath)
	if err != nil {
		return nil, err
	}

	var raw map[string]any
	if err := yaml.Unmarshal(data, &raw); err != nil {
		return nil, err
	}

	fr.Records = buildRecords(raw)
	return fr, nil
}
