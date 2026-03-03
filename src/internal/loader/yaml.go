package loader

import (
	"strings"

	"gopkg.in/yaml.v3"
)

// YAMLLoader parses YAML/YML files using the yaml.Node API so that entity
// references can be detected and converted to EntityRef values.
type YAMLLoader struct{}

func (YAMLLoader) Extensions() []string { return []string{".yaml", ".yml"} }

func (YAMLLoader) Load(absPath, relPath string) (*FileRecord, error) {
	data, fr, err := readFile(absPath, relPath)
	if err != nil {
		return nil, err
	}

	var doc yaml.Node
	if err := yaml.Unmarshal(data, &doc); err != nil {
		return nil, err
	}

	// Empty file or null document.
	if doc.Kind == 0 || len(doc.Content) == 0 {
		fr.EntityType = EntityType(relPath)
		fr.Records = []Record{{Key: EntityKey(relPath), Fields: map[string]any{}}}
		return fr, nil
	}

	root := doc.Content[0]
	raw := nodeToValue(root)

	var fields map[string]any
	if m, ok := raw.(map[string]any); ok {
		fields = m
	} else {
		fields = map[string]any{}
	}

	fr.EntityType = EntityType(relPath)
	fr.Records = []Record{{Key: EntityKey(relPath), Fields: fields}}
	return fr, nil
}

// nodeToValue converts a yaml.Node to a Go value.
//
// Entity references are expressed as quoted YAML strings starting with "&"
// and containing "/" to distinguish file paths from plain ampersand values:
//
//	courses:
//	  - "&recipes/celeriac-veloute"
//
// The yaml.v3 library rejects "/" in bare YAML anchor names, so we use the
// quoted string convention instead.
func nodeToValue(n *yaml.Node) any {
	switch n.Kind {
	case yaml.ScalarNode:
		return scalarValue(n)

	case yaml.SequenceNode:
		out := make([]any, 0, len(n.Content))
		for _, child := range n.Content {
			out = append(out, nodeToValue(child))
		}
		return out

	case yaml.MappingNode:
		m := make(map[string]any, len(n.Content)/2)
		for i := 0; i+1 < len(n.Content); i += 2 {
			key := n.Content[i].Value
			val := nodeToValue(n.Content[i+1])
			m[key] = val
		}
		return m

	case yaml.AliasNode:
		if n.Alias != nil {
			return nodeToValue(n.Alias)
		}
		return nil

	default:
		return nil
	}
}

// scalarValue converts a YAML scalar node to the appropriate Go type.
// Strings starting with "&" and containing "/" are treated as EntityRefs.
func scalarValue(n *yaml.Node) any {
	switch n.Tag {
	case "!!null":
		return nil
	case "!!bool":
		return n.Value == "true"
	case "!!int":
		var i int64
		if err := n.Decode(&i); err == nil {
			return i
		}
		return n.Value
	case "!!float":
		var f float64
		if err := n.Decode(&f); err == nil {
			return f
		}
		return n.Value
	default:
		// Detect entity references: strings like "&recipes/celeriac-veloute".
		if strings.HasPrefix(n.Value, "&") && strings.Contains(n.Value, "/") {
			return EntityRef{Path: n.Value[1:]}
		}
		return n.Value
	}
}
