package pgserver

import (
	"context"
	"database/sql"
	"fmt"
	"net"
	"strings"
	"sync"

	"github.com/jackc/pgproto3/v2"
	_ "modernc.org/sqlite"
)

// Options configures the PostgreSQL wire protocol server.
type Options struct {
	Port     int
	DBPath   string
	Username string // empty = no auth required
	Password string
}

// Server is a read-only PostgreSQL wire protocol server backed by SQLite.
type Server struct {
	opts     Options
	mu       sync.RWMutex
	db       *sql.DB
	listener net.Listener
}

// New creates a new Server. Call Serve to start accepting connections.
func New(opts Options) (*Server, error) {
	db, err := openSQLite(opts.DBPath)
	if err != nil {
		return nil, fmt.Errorf("opening database: %w", err)
	}
	return &Server{opts: opts, db: db}, nil
}

// Serve starts the server and blocks until ctx is cancelled.
func (s *Server) Serve(ctx context.Context) error {
	addr := fmt.Sprintf("0.0.0.0:%d", s.opts.Port)
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("listen %s: %w", addr, err)
	}
	s.listener = ln
	defer ln.Close()

	// Close the listener when context is cancelled.
	go func() {
		<-ctx.Done()
		ln.Close()
	}()

	for {
		conn, err := ln.Accept()
		if err != nil {
			if ctx.Err() != nil {
				return nil // normal shutdown
			}
			return fmt.Errorf("accept: %w", err)
		}
		go s.handleConn(conn)
	}
}

// Addr returns the address the server is listening on (after Serve is called via a goroutine).
func (s *Server) Addr() net.Addr {
	if s.listener == nil {
		return nil
	}
	return s.listener.Addr()
}

// Reload atomically swaps the underlying SQLite database.
func (s *Server) Reload(dbPath string) error {
	newDB, err := openSQLite(dbPath)
	if err != nil {
		return fmt.Errorf("opening new database: %w", err)
	}
	s.mu.Lock()
	old := s.db
	s.db = newDB
	s.mu.Unlock()
	if old != nil {
		old.Close()
	}
	return nil
}

// Close shuts down the server.
func (s *Server) Close() error {
	if s.listener != nil {
		s.listener.Close()
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.db != nil {
		return s.db.Close()
	}
	return nil
}

func (s *Server) handleConn(conn net.Conn) {
	defer conn.Close()

	backend := pgproto3.NewBackend(pgproto3.NewChunkReader(conn), conn)

	// Read startup message (handles SSL negotiation internally via pgproto3).
	startupMsg, err := backend.ReceiveStartupMessage()
	if err != nil {
		return
	}

	switch startupMsg.(type) {
	case *pgproto3.StartupMessage:
		// Normal connection.
	case *pgproto3.SSLRequest:
		// Decline SSL.
		conn.Write([]byte{'N'}) //nolint:errcheck
		// Re-read after SSL rejection.
		startupMsg, err = backend.ReceiveStartupMessage()
		if err != nil {
			return
		}
		if _, ok := startupMsg.(*pgproto3.StartupMessage); !ok {
			return
		}
	default:
		return
	}

	// Authenticate.
	if err := handleAuth(backend, s.opts.Username, s.opts.Password); err != nil {
		return
	}

	// Send server parameter statuses and ReadyForQuery.
	params := [][2]string{
		{"server_version", "14.0"},
		{"client_encoding", "UTF8"},
		{"server_encoding", "UTF8"},
		{"DateStyle", "ISO, MDY"},
		{"integer_datetimes", "on"},
	}
	for _, kv := range params {
		if err := backend.Send(&pgproto3.ParameterStatus{Name: kv[0], Value: kv[1]}); err != nil {
			return
		}
	}
	if err := backend.Send(&pgproto3.BackendKeyData{ProcessID: 1, SecretKey: 0}); err != nil {
		return
	}
	if err := backend.Send(&pgproto3.ReadyForQuery{TxStatus: 'I'}); err != nil {
		return
	}

	// connState tracks state for the extended query protocol.
	type connState struct {
		preparedQuery string // last Parse'd query
	}
	state := &connState{}

	// Query loop — handles both simple and extended query protocols.
	for {
		msg, err := backend.Receive()
		if err != nil {
			return
		}

		switch m := msg.(type) {
		// ── Simple Query Protocol ────────────────────────────────────────
		case *pgproto3.Query:
			query := strings.TrimSpace(m.String)
			if query == "" || query == ";" {
				backend.Send(&pgproto3.EmptyQueryResponse{})          //nolint:errcheck
				backend.Send(&pgproto3.ReadyForQuery{TxStatus: 'I'}) //nolint:errcheck
				continue
			}
			s.mu.RLock()
			db := s.db
			s.mu.RUnlock()
			executeQuery(backend, db, query) //nolint:errcheck
			backend.Send(&pgproto3.ReadyForQuery{TxStatus: 'I'}) //nolint:errcheck

		// ── Extended Query Protocol ──────────────────────────────────────
		case *pgproto3.Parse:
			// Store the query for later execution.
			state.preparedQuery = strings.TrimSpace(m.Query)
			backend.Send(&pgproto3.ParseComplete{}) //nolint:errcheck

		case *pgproto3.Bind:
			// We ignore parameters; just acknowledge.
			backend.Send(&pgproto3.BindComplete{}) //nolint:errcheck

		case *pgproto3.Describe:
			// Send back an empty ParameterDescription (no parameters).
			backend.Send(&pgproto3.ParameterDescription{ParameterOIDs: nil}) //nolint:errcheck
			// We'll send the real RowDescription in Execute.
			// If describing a statement, send NoData for now.
			if m.ObjectType == 'S' {
				backend.Send(&pgproto3.NoData{}) //nolint:errcheck
			}

		case *pgproto3.Execute:
			query := state.preparedQuery
			if query == "" || query == ";" {
				backend.Send(&pgproto3.EmptyQueryResponse{}) //nolint:errcheck
			} else {
				s.mu.RLock()
				db := s.db
				s.mu.RUnlock()
				executeQuery(backend, db, query) //nolint:errcheck
			}

		case *pgproto3.Sync:
			// End of extended query cycle — send ReadyForQuery.
			backend.Send(&pgproto3.ReadyForQuery{TxStatus: 'I'}) //nolint:errcheck

		case *pgproto3.Terminate:
			return

		default:
			// Unknown message type — send error and stay ready.
			backend.Send(&pgproto3.ErrorResponse{ //nolint:errcheck
				Severity: "ERROR",
				Code:     "0A000",
				Message:  fmt.Sprintf("unsupported message type %T", msg),
			})
			backend.Send(&pgproto3.ReadyForQuery{TxStatus: 'I'}) //nolint:errcheck
		}
	}
}

func openSQLite(path string) (*sql.DB, error) {
	if path == "" || path == ":memory:" {
		return sql.Open("sqlite", ":memory:")
	}
	// Open read-only for serving.
	uri := fmt.Sprintf("file:%s?mode=ro", path)
	db, err := sql.Open("sqlite", uri)
	if err != nil {
		return nil, err
	}
	if err := db.Ping(); err != nil {
		db.Close()
		return nil, err
	}
	return db, nil
}
