package pgserver

import (
	"database/sql"
	"fmt"
	"strings"

	"github.com/jackc/pgproto3/v2"
)

// pgTypeOID maps Go SQL column scan types to approximate PostgreSQL OIDs.
// We use TEXT (25) as the safe default for anything not explicitly mapped.
const (
	oidInt8    = 20
	oidFloat8  = 701
	oidBool    = 16
	oidText    = 25
	oidBytea   = 17
	oidUnknown = 705
)

// executeQuery runs a SQL statement against the database and writes results
// back to the client via the pgproto3 backend.
func executeQuery(backend *pgproto3.Backend, db *sql.DB, query string) error {
	query = strings.TrimSpace(query)

	rows, err := db.Query(query)
	if err != nil {
		return sendQueryError(backend, err)
	}
	defer rows.Close()

	cols, err := rows.ColumnTypes()
	if err != nil {
		return sendQueryError(backend, err)
	}

	// Build RowDescription.
	fields := make([]pgproto3.FieldDescription, len(cols))
	for i, col := range cols {
		fields[i] = pgproto3.FieldDescription{
			Name:                 []byte(col.Name()),
			TableOID:             0,
			TableAttributeNumber: 0,
			DataTypeOID:          goTypeToOID(col.DatabaseTypeName()),
			DataTypeSize:         -1,
			TypeModifier:         -1,
			Format:               0, // text format
		}
	}
	if err := backend.Send(&pgproto3.RowDescription{Fields: fields}); err != nil {
		return fmt.Errorf("send RowDescription: %w", err)
	}

	// Stream data rows.
	rowCount := 0
	scanDest := make([]any, len(cols))
	scanPtrs := make([]any, len(cols))
	for i := range scanDest {
		scanPtrs[i] = &scanDest[i]
	}

	for rows.Next() {
		if err := rows.Scan(scanPtrs...); err != nil {
			return sendQueryError(backend, err)
		}

		vals := make([][]byte, len(cols))
		for i, v := range scanDest {
			if v == nil {
				vals[i] = nil
			} else {
				vals[i] = []byte(fmt.Sprintf("%v", v))
			}
		}
		if err := backend.Send(&pgproto3.DataRow{Values: vals}); err != nil {
			return fmt.Errorf("send DataRow: %w", err)
		}
		rowCount++
	}
	if err := rows.Err(); err != nil {
		return sendQueryError(backend, err)
	}

	tag := fmt.Sprintf("SELECT %d", rowCount)
	if err := backend.Send(&pgproto3.CommandComplete{CommandTag: []byte(tag)}); err != nil {
		return fmt.Errorf("send CommandComplete: %w", err)
	}
	return nil
}

func goTypeToOID(dbTypeName string) uint32 {
	switch strings.ToUpper(dbTypeName) {
	case "INTEGER", "INT", "INT2", "INT4", "INT8", "BIGINT", "SMALLINT":
		return oidInt8
	case "REAL", "FLOAT", "FLOAT4", "FLOAT8", "DOUBLE", "NUMERIC", "DECIMAL":
		return oidFloat8
	case "BOOL", "BOOLEAN":
		return oidBool
	case "BLOB":
		return oidBytea
	default:
		return oidText
	}
}

func sendQueryError(backend *pgproto3.Backend, err error) error {
	backend.Send(&pgproto3.ErrorResponse{ //nolint:errcheck
		Severity: "ERROR",
		Code:     "42601", // syntax_error
		Message:  err.Error(),
	})
	return err
}
