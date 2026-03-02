package loader

import (
	"encoding/xml"
	"io"
	"strings"
)

// XMLLoader loads .xml files.
// It parses XML into a nested map where element names are keys and
// text content / child elements become values.
type XMLLoader struct{}

func (XMLLoader) Extensions() []string { return []string{".xml"} }

func (XMLLoader) Load(absPath, relPath string) (*FileRecord, error) {
	data, fr, err := readFile(absPath, relPath)
	if err != nil {
		return nil, err
	}

	m, err := parseXML(data)
	if err != nil {
		return nil, err
	}

	fr.Records = []Record{buildRecord(recordKey(relPath), m)}
	return fr, nil
}

// parseXML parses XML bytes into a map[string]any suitable for buildRecord.
// The root element is unwrapped; its children become the field map.
func parseXML(data []byte) (map[string]any, error) {
	decoder := xml.NewDecoder(strings.NewReader(string(data)))
	// Skip the root element and parse its children as top-level keys.
	root, err := xmlDecodeElement(decoder)
	if err != nil {
		return nil, err
	}
	// The root's children become the top-level map.
	if m, ok := root.(map[string]any); ok {
		return m, nil
	}
	return map[string]any{"root": root}, nil
}

// xmlDecodeElement reads and returns the next element from the decoder.
func xmlDecodeElement(decoder *xml.Decoder) (any, error) {
	// Seek to next start element.
	for {
		tok, err := decoder.Token()
		if err == io.EOF {
			return nil, nil
		}
		if err != nil {
			return nil, err
		}
		switch t := tok.(type) {
		case xml.StartElement:
			return xmlReadElement(decoder, t)
		}
	}
}

func xmlReadElement(decoder *xml.Decoder, start xml.StartElement) (any, error) {
	children := make(map[string]any)

	// Include attributes.
	for _, attr := range start.Attr {
		children[attr.Name.Local] = attr.Value
	}

	var textContent strings.Builder

	for {
		tok, err := decoder.Token()
		if err != nil {
			return nil, err
		}
		switch t := tok.(type) {
		case xml.StartElement:
			child, err := xmlReadElement(decoder, t)
			if err != nil {
				return nil, err
			}
			name := t.Name.Local
			if existing, ok := children[name]; ok {
				// Multiple children with same name → make a slice.
				switch ex := existing.(type) {
				case []any:
					children[name] = append(ex, child)
				default:
					children[name] = []any{ex, child}
				}
			} else {
				children[name] = child
			}
		case xml.CharData:
			textContent.Write(t)
		case xml.EndElement:
			text := strings.TrimSpace(textContent.String())
			if len(children) == 0 {
				// Leaf element — return its text content.
				return text, nil
			}
			if text != "" {
				children["#text"] = text
			}
			return children, nil
		}
	}
}
