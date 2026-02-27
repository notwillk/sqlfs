package sqlite

import (
	"database/sql"
	"fmt"
	"io"
	"os"
	"strings"

	_ "modernc.org/sqlite" // register the sqlite driver
)

const driverName = "sqlite"

// DB wraps a *sql.DB backed by modernc.org/sqlite.
type DB struct {
	db   *sql.DB
	path string
}

// Open opens or creates a SQLite database at the given file path.
func Open(path string) (*DB, error) {
	db, err := sql.Open(driverName, path)
	if err != nil {
		return nil, err
	}
	// SQLite is single-writer; limit to one connection.
	db.SetMaxOpenConns(1)
	if err := db.Ping(); err != nil {
		db.Close()
		return nil, err
	}
	return &DB{db: db, path: path}, nil
}

// OpenMemory opens an in-memory SQLite database (useful for testing).
func OpenMemory() (*DB, error) {
	return Open(":memory:")
}

// OpenReadOnly opens an existing SQLite database in read-only mode.
func OpenReadOnly(path string) (*DB, error) {
	uri := fmt.Sprintf("file:%s?mode=ro", path)
	db, err := sql.Open(driverName, uri)
	if err != nil {
		return nil, err
	}
	if err := db.Ping(); err != nil {
		db.Close()
		return nil, err
	}
	return &DB{db: db, path: path}, nil
}

// ExecDDL executes a slice of DDL statements.
func (d *DB) ExecDDL(statements []string) error {
	for _, stmt := range statements {
		if strings.TrimSpace(stmt) == "" {
			continue
		}
		if _, err := d.db.Exec(stmt); err != nil {
			return fmt.Errorf("DDL error in %q: %w", stmt, err)
		}
	}
	return nil
}

// Exec executes a single SQL statement.
func (d *DB) Exec(query string, args ...any) error {
	_, err := d.db.Exec(query, args...)
	return err
}

// InsertRecord inserts a single row into a table.
// cols and values must be the same length.
func (d *DB) InsertRecord(table string, cols []string, values []any) error {
	if len(cols) == 0 {
		return nil
	}
	quotedTable := quoteName(table)
	quotedCols := make([]string, len(cols))
	placeholders := make([]string, len(cols))
	for i, col := range cols {
		quotedCols[i] = quoteName(col)
		placeholders[i] = "?"
	}
	query := fmt.Sprintf(
		"INSERT INTO %s (%s) VALUES (%s)",
		quotedTable,
		strings.Join(quotedCols, ", "),
		strings.Join(placeholders, ", "),
	)
	_, err := d.db.Exec(query, values...)
	return err
}

// Query executes a SQL query and returns the rows.
func (d *DB) Query(query string, args ...any) (*sql.Rows, error) {
	return d.db.Query(query, args...)
}

// DB returns the underlying *sql.DB for use by drivers that need it directly.
func (d *DB) DB() *sql.DB {
	return d.db
}

// Close closes the database.
func (d *DB) Close() error {
	return d.db.Close()
}

// SaveTo copies the in-memory or current DB to a file.
// For file-based DBs this is a file copy; for in-memory it uses the backup API.
func (d *DB) SaveTo(path string) error {
	if d.path == ":memory:" {
		return d.saveMemoryTo(path)
	}
	// The DB is already at d.path; just copy the file.
	return copyFile(d.path, path)
}

func (d *DB) saveMemoryTo(path string) error {
	// Use VACUUM INTO to write an in-memory DB to a file.
	_, err := d.db.Exec(fmt.Sprintf("VACUUM INTO %q", path))
	return err
}

func copyFile(src, dst string) error {
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()

	out, err := os.Create(dst)
	if err != nil {
		return err
	}
	defer out.Close()

	_, err = io.Copy(out, in)
	return err
}

// quoteName quotes a SQLite identifier.
func quoteName(name string) string {
	return `"` + strings.ReplaceAll(name, `"`, `""`) + `"`
}
