package dbml

// Schema is the top-level result of parsing a DBML file.
type Schema struct {
	Tables  []*Table
	Enums   []*Enum
	Refs    []*Ref
	Project *Project
}

// TableByName returns the table with the given name, or nil.
func (s *Schema) TableByName(name string) *Table {
	for _, t := range s.Tables {
		if t.Name == name {
			return t
		}
	}
	return nil
}

// EnumByName returns the enum with the given name, or nil.
func (s *Schema) EnumByName(name string) *Enum {
	for _, e := range s.Enums {
		if e.Name == name {
			return e
		}
	}
	return nil
}

// Project holds optional project metadata from DBML.
type Project struct {
	Name         string
	DatabaseType string
	Note         string
}

// Table represents a DBML Table definition.
type Table struct {
	Name    string
	Alias   string
	Note    string
	Columns []*Column
	Indexes []*Index
}

// ColumnByName returns the column with the given name, or nil.
func (t *Table) ColumnByName(name string) *Column {
	for _, c := range t.Columns {
		if c.Name == name {
			return c
		}
	}
	return nil
}

// Column represents a column within a Table.
type Column struct {
	Name      string
	Type      ColumnType
	PK        bool
	NotNull   bool
	Unique    bool
	Increment bool
	Default   *DefaultValue
	Note      string
	Refs      []*InlineRef
}

// ColumnType is the parsed column type, e.g. varchar(255).
type ColumnType struct {
	Name string // e.g. "varchar", "int"
	Args []int  // e.g. [255] for varchar(255)
}

// DefaultKind identifies the kind of a column default value.
type DefaultKind int

const (
	DefaultString DefaultKind = iota
	DefaultNumber
	DefaultBool
	DefaultNull
	DefaultExpr // backtick expression
)

// DefaultValue is the parsed default clause for a column.
type DefaultValue struct {
	Kind  DefaultKind
	Value string
}

// InlineRef is a reference defined inline on a column using [ref: > table.col].
type InlineRef struct {
	Relation RefRelation
	To       RefEndpoint
}

// Index represents an index defined in the `indexes` block of a table.
type Index struct {
	Columns []string
	IsExpr  bool   // true when the column is a backtick expression
	Unique  bool
	PK      bool
	Name    string
	Type    string // e.g. "btree", "hash"
}

// Enum represents a DBML enum definition.
type Enum struct {
	Name   string
	Values []*EnumValue
}

// EnumValue is one value in an Enum.
type EnumValue struct {
	Name string
	Note string
}

// Ref is a standalone relationship declaration (Ref: table.col > table.col).
type Ref struct {
	Name     string
	From     RefEndpoint
	To       RefEndpoint
	Relation RefRelation
	OnDelete string
	OnUpdate string
}

// RefEndpoint identifies one side of a relationship (table.column).
type RefEndpoint struct {
	Schema string // optional schema qualifier
	Table  string
	Column string
}

// RefRelation describes the cardinality of a relationship.
type RefRelation int

const (
	ManyToOne  RefRelation = iota // >
	OneToMany                     // <
	OneToOne                      // -
	ManyToMany                    // <>
)

// Position is a source location used in error messages.
type Position struct {
	Line   int
	Column int
}
