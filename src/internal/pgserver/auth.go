package pgserver

import (
	"fmt"

	"github.com/jackc/pgproto3/v2"
)

// authResult is the outcome of authentication for a connection.
type authResult struct {
	ok  bool
	err error
}

// handleAuth performs authentication with the client.
// If username is empty, all connections are accepted without credentials.
func handleAuth(backend *pgproto3.Backend, username, password string) error {
	if username == "" {
		// No auth required.
		if err := backend.Send(&pgproto3.AuthenticationOk{}); err != nil {
			return fmt.Errorf("send AuthOk: %w", err)
		}
		return nil
	}

	// Request cleartext password.
	if err := backend.Send(&pgproto3.AuthenticationCleartextPassword{}); err != nil {
		return fmt.Errorf("send AuthCleartextPassword: %w", err)
	}

	msg, err := backend.Receive()
	if err != nil {
		return fmt.Errorf("receive password: %w", err)
	}

	pw, ok := msg.(*pgproto3.PasswordMessage)
	if !ok {
		return sendAuthError(backend, "expected password message")
	}

	if pw.Password != password {
		return sendAuthError(backend, "password authentication failed")
	}

	if err := backend.Send(&pgproto3.AuthenticationOk{}); err != nil {
		return fmt.Errorf("send AuthOk: %w", err)
	}
	return nil
}

func sendAuthError(backend *pgproto3.Backend, msg string) error {
	backend.Send(&pgproto3.ErrorResponse{ //nolint:errcheck
		Severity: "FATAL",
		Code:     "28P01", // invalid_password
		Message:  msg,
	})
	return fmt.Errorf("auth failed: %s", msg)
}
