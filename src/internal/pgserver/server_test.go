package pgserver

import (
	"context"
	"database/sql"
	"fmt"
	"net"
	"testing"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
)

// findFreePort returns an available TCP port.
func findFreePort(t *testing.T) int {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("find free port: %v", err)
	}
	port := ln.Addr().(*net.TCPAddr).Port
	ln.Close()
	return port
}

func startTestServer(t *testing.T, opts Options) (*Server, int) {
	t.Helper()
	if opts.Port == 0 {
		opts.Port = findFreePort(t)
	}
	srv, err := New(opts)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(func() {
		cancel()
		srv.Close()
	})
	go func() {
		srv.Serve(ctx) //nolint:errcheck
	}()
	// Give server time to start listening.
	time.Sleep(50 * time.Millisecond)
	return srv, opts.Port
}

func connectPG(t *testing.T, port int, user, pass string) *sql.DB {
	t.Helper()
	// prefer_simple_protocol=true forces pgx to use Simple Query protocol
	// instead of extended query (Parse/Bind/Execute), which our server supports.
	dsn := fmt.Sprintf(
		"host=127.0.0.1 port=%d user=%s password=%s dbname=postgres sslmode=disable prefer_simple_protocol=true",
		port, user, pass,
	)
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("sql.Open: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	return db
}

func TestServer_NoAuth_Select(t *testing.T) {
	// Start server with no auth.
	_, port := startTestServer(t, Options{
		Port:   0,
		DBPath: ":memory:",
	})

	db := connectPG(t, port, "any", "any")

	rows, err := db.Query("SELECT 1 as n")
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	defer rows.Close()
	if !rows.Next() {
		t.Fatal("expected row")
	}
	var n int
	if err := rows.Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Errorf("n = %d, want 1", n)
	}
}

func TestServer_Auth_Valid(t *testing.T) {
	_, port := startTestServer(t, Options{
		Port:     0,
		DBPath:   ":memory:",
		Username: "testuser",
		Password: "testpass",
	})

	db := connectPG(t, port, "testuser", "testpass")
	if err := db.Ping(); err != nil {
		t.Fatalf("ping with valid credentials: %v", err)
	}
}

func TestServer_Auth_InvalidPassword(t *testing.T) {
	_, port := startTestServer(t, Options{
		Port:     0,
		DBPath:   ":memory:",
		Username: "testuser",
		Password: "testpass",
	})

	db := connectPG(t, port, "testuser", "wrongpassword")
	err := db.Ping()
	if err == nil {
		t.Fatal("expected error with wrong password")
	}
}

func TestServer_QuerySQLiteData(t *testing.T) {
	// Create a temp SQLite DB with some data.
	tmpDir := t.TempDir()
	dbPath := tmpDir + "/test.db"

	// Use internal sqlite package to create DB.
	setupDB, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatal(err)
	}
	setupDB.Exec("CREATE TABLE users (id INTEGER, name TEXT)")
	setupDB.Exec("INSERT INTO users VALUES (1, 'Alice')")
	setupDB.Exec("INSERT INTO users VALUES (2, 'Bob')")
	setupDB.Close()

	_, port := startTestServer(t, Options{
		Port:   0,
		DBPath: dbPath,
	})

	db := connectPG(t, port, "any", "any")
	rows, err := db.Query("SELECT id, name FROM users ORDER BY id")
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	defer rows.Close()

	type row struct{ id int; name string }
	var results []row
	for rows.Next() {
		var r row
		rows.Scan(&r.id, &r.name)
		results = append(results, r)
	}
	if len(results) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(results))
	}
	if results[0].name != "Alice" {
		t.Errorf("row[0].name = %q, want Alice", results[0].name)
	}
	if results[1].name != "Bob" {
		t.Errorf("row[1].name = %q, want Bob", results[1].name)
	}
}

func TestServer_Reload(t *testing.T) {
	tmpDir := t.TempDir()

	// Initial DB.
	db1Path := tmpDir + "/db1.db"
	d1, _ := sql.Open("sqlite", db1Path)
	d1.Exec("CREATE TABLE t (val TEXT)")
	d1.Exec("INSERT INTO t VALUES ('original')")
	d1.Close()

	srv, port := startTestServer(t, Options{
		Port:   0,
		DBPath: db1Path,
	})

	client := connectPG(t, port, "", "")
	rows, err := client.Query("SELECT val FROM t")
	if err != nil {
		t.Fatalf("query before reload: %v", err)
	}
	rows.Next()
	var val string
	rows.Scan(&val)
	rows.Close()
	if val != "original" {
		t.Errorf("before reload: val = %q, want original", val)
	}

	// Create a new DB with different data.
	db2Path := tmpDir + "/db2.db"
	d2, _ := sql.Open("sqlite", db2Path)
	d2.Exec("CREATE TABLE t (val TEXT)")
	d2.Exec("INSERT INTO t VALUES ('reloaded')")
	d2.Close()

	if err := srv.Reload(db2Path); err != nil {
		t.Fatalf("Reload: %v", err)
	}

	rows2, err2 := client.Query("SELECT val FROM t")
	if err2 != nil {
		t.Fatalf("query after reload: %v", err2)
	}
	rows2.Next()
	var val2 string
	rows2.Scan(&val2)
	rows2.Close()
	if val2 != "reloaded" {
		t.Errorf("after reload: val = %q, want reloaded", val2)
	}
}
