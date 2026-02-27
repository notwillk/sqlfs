package dbml

import (
	"fmt"
	"os"
	"strconv"
	"strings"
)

// ParseError is returned when the DBML source is syntactically invalid.
type ParseError struct {
	Pos     Position
	Message string
}

func (e *ParseError) Error() string {
	return fmt.Sprintf("dbml parse error at %d:%d: %s", e.Pos.Line, e.Pos.Column, e.Message)
}

// Parse parses DBML source bytes and returns the Schema AST.
func Parse(src []byte) (*Schema, error) {
	lex, err := NewLexer(src)
	if err != nil {
		return nil, err
	}
	p := &parser{lex: lex}
	return p.parseSchema()
}

// ParseFile reads a .dbml file and parses it.
func ParseFile(path string) (*Schema, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	return Parse(data)
}

type parser struct {
	lex *Lexer
}

func (p *parser) peek() Token    { return p.lex.Peek(0) }
func (p *parser) peekAt(n int) Token { return p.lex.Peek(n) }
func (p *parser) next() Token    { return p.lex.Next() }

func (p *parser) expect(kind TokenKind) (Token, error) {
	t := p.next()
	if t.Kind != kind {
		return t, &ParseError{t.Pos, fmt.Sprintf("expected token kind %d, got %q (%d)", kind, t.Value, t.Kind)}
	}
	return t, nil
}

func (p *parser) parseError(t Token, msg string) error {
	return &ParseError{t.Pos, msg}
}

// parseSchema reads the top-level sequence of Table/Enum/Ref/Project declarations.
func (p *parser) parseSchema() (*Schema, error) {
	schema := &Schema{}
	for {
		t := p.peek()
		if t.Kind == TokEOF {
			break
		}
		if t.Kind != TokIdent {
			return nil, p.parseError(t, fmt.Sprintf("expected keyword, got %q", t.Value))
		}
		switch strings.ToLower(t.Value) {
		case "table":
			p.next()
			tbl, err := p.parseTable()
			if err != nil {
				return nil, err
			}
			schema.Tables = append(schema.Tables, tbl)
		case "enum":
			p.next()
			en, err := p.parseEnum()
			if err != nil {
				return nil, err
			}
			schema.Enums = append(schema.Enums, en)
		case "ref":
			p.next()
			ref, err := p.parseRef()
			if err != nil {
				return nil, err
			}
			schema.Refs = append(schema.Refs, ref)
		case "project":
			p.next()
			proj, err := p.parseProject()
			if err != nil {
				return nil, err
			}
			schema.Project = proj
		case "tablegroup":
			// Skip TableGroup blocks (not used for our purposes).
			p.next()
			if err := p.skipBlock(); err != nil {
				return nil, err
			}
		default:
			return nil, p.parseError(t, fmt.Sprintf("unknown keyword %q", t.Value))
		}
	}
	return schema, nil
}

// parseTable parses: Name ['as' Alias] [TableSettings] '{' columns... '}'
func (p *parser) parseTable() (*Table, error) {
	tbl := &Table{}

	// Table name (possibly schema-qualified).
	name, err := p.parseQualifiedName()
	if err != nil {
		return nil, err
	}
	tbl.Name = name

	// Optional alias.
	if p.peek().Kind == TokIdent && strings.ToLower(p.peek().Value) == "as" {
		p.next()
		alias, err := p.expectIdent()
		if err != nil {
			return nil, err
		}
		tbl.Alias = alias
	}

	// Optional table-level settings [...]
	if p.peek().Kind == TokLBracket {
		if err := p.skipBracketBlock(); err != nil {
			return nil, err
		}
	}

	if _, err := p.expect(TokLBrace); err != nil {
		return nil, err
	}

	for p.peek().Kind != TokRBrace && p.peek().Kind != TokEOF {
		t := p.peek()
		if t.Kind == TokIdent {
			switch strings.ToLower(t.Value) {
			case "indexes":
				p.next()
				idxs, err := p.parseIndexes()
				if err != nil {
					return nil, err
				}
				tbl.Indexes = append(tbl.Indexes, idxs...)
				continue
			case "note":
				p.next()
				note, err := p.parseNoteValue()
				if err != nil {
					return nil, err
				}
				tbl.Note = note
				continue
			}
		}
		col, err := p.parseColumn()
		if err != nil {
			return nil, err
		}
		tbl.Columns = append(tbl.Columns, col)
	}

	if _, err := p.expect(TokRBrace); err != nil {
		return nil, err
	}
	return tbl, nil
}

// parseColumn parses a single column definition.
func (p *parser) parseColumn() (*Column, error) {
	col := &Column{}

	name, err := p.expectIdent()
	if err != nil {
		return nil, err
	}
	col.Name = name

	// Column type (optional — some columns might just have settings).
	if p.peek().Kind == TokIdent {
		ct, err := p.parseColumnType()
		if err != nil {
			return nil, err
		}
		col.Type = ct
	}

	// Optional column settings [...]
	if p.peek().Kind == TokLBracket {
		if err := p.parseColumnSettings(col); err != nil {
			return nil, err
		}
	}

	return col, nil
}

// parseColumnType parses a type name with optional args: varchar(255), int, etc.
func (p *parser) parseColumnType() (ColumnType, error) {
	ct := ColumnType{}
	name, err := p.expectIdent()
	if err != nil {
		return ct, err
	}
	ct.Name = strings.ToLower(name)

	// Optional (args).
	if p.peek().Kind == TokLParen {
		p.next()
		for p.peek().Kind != TokRParen && p.peek().Kind != TokEOF {
			t := p.next()
			if t.Kind == TokNumber {
				n, _ := strconv.Atoi(t.Value)
				ct.Args = append(ct.Args, n)
			}
			if p.peek().Kind == TokComma {
				p.next()
			}
		}
		if _, err := p.expect(TokRParen); err != nil {
			return ct, err
		}
	}

	return ct, nil
}

// parseColumnSettings parses [...] column settings.
func (p *parser) parseColumnSettings(col *Column) error {
	p.next() // consume [
	for p.peek().Kind != TokRBracket && p.peek().Kind != TokEOF {
		t := p.peek()
		if t.Kind != TokIdent && t.Kind != TokString {
			return p.parseError(t, fmt.Sprintf("expected setting keyword, got %q", t.Value))
		}
		setting := strings.ToLower(t.Value)
		switch setting {
		case "pk", "primary":
			p.next()
			if strings.ToLower(p.peek().Value) == "key" {
				p.next()
			}
			col.PK = true
		case "not":
			p.next()
			if strings.ToLower(p.peek().Value) == "null" {
				p.next()
			}
			col.NotNull = true
		case "null":
			p.next()
			// explicitly nullable (default), no action needed
		case "unique":
			p.next()
			col.Unique = true
		case "increment":
			p.next()
			col.Increment = true
		case "note":
			p.next()
			if _, err := p.expect(TokColon); err != nil {
				return err
			}
			note, err := p.expectString()
			if err != nil {
				return err
			}
			col.Note = note
		case "default":
			p.next()
			if _, err := p.expect(TokColon); err != nil {
				return err
			}
			dv, err := p.parseDefaultValue()
			if err != nil {
				return err
			}
			col.Default = dv
		case "ref":
			p.next()
			if _, err := p.expect(TokColon); err != nil {
				return err
			}
			ref, err := p.parseInlineRef()
			if err != nil {
				return err
			}
			col.Refs = append(col.Refs, ref)
		default:
			// Unknown setting — skip until comma or ]
			p.next()
			for p.peek().Kind != TokComma && p.peek().Kind != TokRBracket && p.peek().Kind != TokEOF {
				p.next()
			}
		}
		if p.peek().Kind == TokComma {
			p.next()
		}
	}
	if _, err := p.expect(TokRBracket); err != nil {
		return err
	}
	return nil
}

func (p *parser) parseDefaultValue() (*DefaultValue, error) {
	t := p.peek()
	switch t.Kind {
	case TokString:
		p.next()
		return &DefaultValue{Kind: DefaultString, Value: t.Value}, nil
	case TokNumber:
		p.next()
		return &DefaultValue{Kind: DefaultNumber, Value: t.Value}, nil
	case TokBacktick:
		p.next()
		return &DefaultValue{Kind: DefaultExpr, Value: t.Value}, nil
	case TokIdent:
		p.next()
		switch strings.ToLower(t.Value) {
		case "true":
			return &DefaultValue{Kind: DefaultBool, Value: "true"}, nil
		case "false":
			return &DefaultValue{Kind: DefaultBool, Value: "false"}, nil
		case "null":
			return &DefaultValue{Kind: DefaultNull, Value: "null"}, nil
		default:
			// Could be an identifier like a function name.
			return &DefaultValue{Kind: DefaultExpr, Value: t.Value}, nil
		}
	default:
		return nil, p.parseError(t, fmt.Sprintf("unexpected default value token %q", t.Value))
	}
}

func (p *parser) parseInlineRef() (*InlineRef, error) {
	rel, err := p.parseRelation()
	if err != nil {
		return nil, err
	}
	endpoint, err := p.parseRefEndpoint()
	if err != nil {
		return nil, err
	}
	return &InlineRef{Relation: rel, To: endpoint}, nil
}

func (p *parser) parseRelation() (RefRelation, error) {
	t := p.next()
	switch t.Kind {
	case TokRAngle:
		return ManyToOne, nil
	case TokLAngle:
		return OneToMany, nil
	case TokDash:
		return OneToOne, nil
	case TokIdent:
		if t.Value == "<>" {
			return ManyToMany, nil
		}
	}
	return 0, p.parseError(t, fmt.Sprintf("expected relation symbol (< > - <>), got %q", t.Value))
}

func (p *parser) parseRefEndpoint() (RefEndpoint, error) {
	ep := RefEndpoint{}
	// Could be: table.col or schema.table.col
	parts := []string{}
	name, err := p.expectIdent()
	if err != nil {
		return ep, err
	}
	parts = append(parts, name)
	for p.peek().Kind == TokDot {
		p.next()
		part, err := p.expectIdent()
		if err != nil {
			return ep, err
		}
		parts = append(parts, part)
	}
	switch len(parts) {
	case 2:
		ep.Table = parts[0]
		ep.Column = parts[1]
	case 3:
		ep.Schema = parts[0]
		ep.Table = parts[1]
		ep.Column = parts[2]
	default:
		return ep, fmt.Errorf("invalid ref endpoint: %v", parts)
	}
	return ep, nil
}

func (p *parser) parseIndexes() ([]*Index, error) {
	if _, err := p.expect(TokLBrace); err != nil {
		return nil, err
	}
	var idxs []*Index
	for p.peek().Kind != TokRBrace && p.peek().Kind != TokEOF {
		idx, err := p.parseIndex()
		if err != nil {
			return nil, err
		}
		idxs = append(idxs, idx)
	}
	if _, err := p.expect(TokRBrace); err != nil {
		return nil, err
	}
	return idxs, nil
}

func (p *parser) parseIndex() (*Index, error) {
	idx := &Index{}
	t := p.peek()

	if t.Kind == TokBacktick {
		p.next()
		idx.Columns = []string{t.Value}
		idx.IsExpr = true
	} else if t.Kind == TokLParen {
		// Composite index: (col1, col2)
		p.next()
		for p.peek().Kind != TokRParen && p.peek().Kind != TokEOF {
			col, err := p.expectIdent()
			if err != nil {
				return nil, err
			}
			idx.Columns = append(idx.Columns, col)
			if p.peek().Kind == TokComma {
				p.next()
			}
		}
		if _, err := p.expect(TokRParen); err != nil {
			return nil, err
		}
	} else {
		col, err := p.expectIdent()
		if err != nil {
			return nil, err
		}
		idx.Columns = []string{col}
	}

	// Optional index settings [...]
	if p.peek().Kind == TokLBracket {
		p.next()
		for p.peek().Kind != TokRBracket && p.peek().Kind != TokEOF {
			t := p.peek()
			if t.Kind != TokIdent {
				p.next()
				continue
			}
			switch strings.ToLower(t.Value) {
			case "pk":
				p.next()
				idx.PK = true
			case "unique":
				p.next()
				idx.Unique = true
			case "name":
				p.next()
				p.next() // :
				nm, err := p.expectString()
				if err != nil {
					return nil, err
				}
				idx.Name = nm
			case "type":
				p.next()
				p.next() // :
				tp, err := p.expectIdent()
				if err != nil {
					return nil, err
				}
				idx.Type = tp
			default:
				p.next()
			}
			if p.peek().Kind == TokComma {
				p.next()
			}
		}
		if _, err := p.expect(TokRBracket); err != nil {
			return nil, err
		}
	}

	return idx, nil
}

func (p *parser) parseEnum() (*Enum, error) {
	en := &Enum{}
	name, err := p.parseQualifiedName()
	if err != nil {
		return nil, err
	}
	en.Name = name

	if _, err := p.expect(TokLBrace); err != nil {
		return nil, err
	}
	for p.peek().Kind != TokRBrace && p.peek().Kind != TokEOF {
		t := p.peek()
		if t.Kind == TokIdent && strings.ToLower(t.Value) == "note" {
			p.next()
			if _, err := p.parseNoteValue(); err != nil {
				return nil, err
			}
			continue
		}
		var valName string
		if t.Kind == TokString {
			p.next()
			valName = t.Value
		} else if t.Kind == TokIdent {
			p.next()
			valName = t.Value
		} else {
			return nil, p.parseError(t, fmt.Sprintf("expected enum value, got %q", t.Value))
		}
		ev := &EnumValue{Name: valName}
		if p.peek().Kind == TokLBracket {
			p.next()
			for p.peek().Kind != TokRBracket && p.peek().Kind != TokEOF {
				st := p.peek()
				if st.Kind == TokIdent && strings.ToLower(st.Value) == "note" {
					p.next()
					if _, err := p.expect(TokColon); err != nil {
						return nil, err
					}
					note, err := p.expectString()
					if err != nil {
						return nil, err
					}
					ev.Note = note
				} else {
					p.next()
				}
				if p.peek().Kind == TokComma {
					p.next()
				}
			}
			if _, err := p.expect(TokRBracket); err != nil {
				return nil, err
			}
		}
		en.Values = append(en.Values, ev)
	}
	if _, err := p.expect(TokRBrace); err != nil {
		return nil, err
	}
	return en, nil
}

func (p *parser) parseRef() (*Ref, error) {
	ref := &Ref{}

	// Optional ref name.
	if p.peek().Kind == TokIdent {
		// Could be name or direct endpoint — look ahead for colon.
		if p.peekAt(1).Kind == TokColon {
			ref.Name = p.next().Value
		}
	}

	if _, err := p.expect(TokColon); err != nil {
		return nil, err
	}

	from, err := p.parseRefEndpoint()
	if err != nil {
		return nil, err
	}
	ref.From = from

	rel, err := p.parseRelation()
	if err != nil {
		return nil, err
	}
	ref.Relation = rel

	to, err := p.parseRefEndpoint()
	if err != nil {
		return nil, err
	}
	ref.To = to

	// Optional ref settings [delete: ..., update: ...]
	if p.peek().Kind == TokLBracket {
		p.next()
		for p.peek().Kind != TokRBracket && p.peek().Kind != TokEOF {
			t := p.peek()
			if t.Kind == TokIdent {
				switch strings.ToLower(t.Value) {
				case "delete":
					p.next()
					if _, err := p.expect(TokColon); err != nil {
						return nil, err
					}
					ref.OnDelete = p.readRefAction()
				case "update":
					p.next()
					if _, err := p.expect(TokColon); err != nil {
						return nil, err
					}
					ref.OnUpdate = p.readRefAction()
				default:
					p.next()
				}
			} else {
				p.next()
			}
			if p.peek().Kind == TokComma {
				p.next()
			}
		}
		if _, err := p.expect(TokRBracket); err != nil {
			return nil, err
		}
	}

	return ref, nil
}

func (p *parser) readRefAction() string {
	var parts []string
	for {
		t := p.peek()
		if t.Kind != TokIdent {
			break
		}
		// Ref actions can be multi-word: "no action", "set null", "set default"
		lower := strings.ToLower(t.Value)
		if lower == "cascade" || lower == "restrict" {
			p.next()
			parts = append(parts, lower)
			break
		}
		if lower == "no" || lower == "set" {
			p.next()
			parts = append(parts, lower)
			next := p.peek()
			if next.Kind == TokIdent {
				p.next()
				parts = append(parts, strings.ToLower(next.Value))
			}
			break
		}
		p.next()
		parts = append(parts, lower)
		break
	}
	return strings.Join(parts, " ")
}

func (p *parser) parseProject() (*Project, error) {
	proj := &Project{}
	nameT, err := p.expectIdent()
	if err != nil {
		return nil, err
	}
	proj.Name = nameT

	if _, err := p.expect(TokLBrace); err != nil {
		return nil, err
	}
	for p.peek().Kind != TokRBrace && p.peek().Kind != TokEOF {
		t := p.peek()
		if t.Kind != TokIdent {
			p.next()
			continue
		}
		switch strings.ToLower(t.Value) {
		case "database_type":
			p.next()
			if _, err := p.expect(TokColon); err != nil {
				return nil, err
			}
			val, err := p.expectString()
			if err != nil {
				return nil, err
			}
			proj.DatabaseType = val
		case "note":
			p.next()
			val, err := p.parseNoteValue()
			if err != nil {
				return nil, err
			}
			proj.Note = val
		default:
			p.next()
			// skip value
			for p.peek().Kind != TokIdent && p.peek().Kind != TokRBrace && p.peek().Kind != TokEOF {
				p.next()
			}
		}
	}
	if _, err := p.expect(TokRBrace); err != nil {
		return nil, err
	}
	return proj, nil
}

func (p *parser) parseNoteValue() (string, error) {
	t := p.peek()
	if t.Kind == TokColon {
		p.next()
		return p.expectString()
	}
	if t.Kind == TokLBrace {
		p.next()
		val, err := p.expectString()
		if err != nil {
			return "", err
		}
		if _, err := p.expect(TokRBrace); err != nil {
			return "", err
		}
		return val, nil
	}
	return "", p.parseError(t, "expected ':' or '{' after Note")
}

// parseQualifiedName reads an optionally schema-qualified name: name or schema.name
func (p *parser) parseQualifiedName() (string, error) {
	name, err := p.expectIdent()
	if err != nil {
		return "", err
	}
	if p.peek().Kind == TokDot {
		p.next()
		sub, err := p.expectIdent()
		if err != nil {
			return "", err
		}
		return name + "." + sub, nil
	}
	return name, nil
}

func (p *parser) expectIdent() (string, error) {
	t := p.next()
	if t.Kind != TokIdent {
		return "", p.parseError(t, fmt.Sprintf("expected identifier, got %q", t.Value))
	}
	return t.Value, nil
}

func (p *parser) expectString() (string, error) {
	t := p.next()
	if t.Kind != TokString && t.Kind != TokIdent {
		return "", p.parseError(t, fmt.Sprintf("expected string, got %q (%d)", t.Value, t.Kind))
	}
	return t.Value, nil
}

func (p *parser) skipBlock() error {
	// Skip an optional name then a { ... } block.
	if p.peek().Kind == TokIdent {
		p.next()
	}
	if p.peek().Kind != TokLBrace {
		return nil
	}
	depth := 0
	for p.peek().Kind != TokEOF {
		t := p.next()
		if t.Kind == TokLBrace {
			depth++
		} else if t.Kind == TokRBrace {
			depth--
			if depth == 0 {
				return nil
			}
		}
	}
	return fmt.Errorf("unclosed block")
}

func (p *parser) skipBracketBlock() error {
	depth := 0
	for p.peek().Kind != TokEOF {
		t := p.next()
		if t.Kind == TokLBracket {
			depth++
		} else if t.Kind == TokRBracket {
			depth--
			if depth == 0 {
				return nil
			}
		}
	}
	return fmt.Errorf("unclosed bracket block")
}
