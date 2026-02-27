package dbml

import (
	"fmt"
	"strings"
	"unicode"
	"unicode/utf8"
)

// TokenKind identifies the kind of a lexer token.
type TokenKind int

const (
	TokIdent       TokenKind = iota // bare word identifier
	TokString                       // 'hello' or '''multi-line'''
	TokNumber                       // 123 or 123.456
	TokBacktick                     // `expr`
	TokLBrace                       // {
	TokRBrace                       // }
	TokLBracket                     // [
	TokRBracket                     // ]
	TokLParen                       // (
	TokRParen                       // )
	TokColon                        // :
	TokComma                        // ,
	TokDot                          // .
	TokLAngle                       // <
	TokRAngle                       // >
	TokDash                         // -
	TokTilde                        // ~
	TokEOF
	TokIllegal
)

// Token is a single lexical unit.
type Token struct {
	Kind    TokenKind
	Value   string
	Pos     Position
}

func (t Token) String() string {
	return fmt.Sprintf("Token(%d, %q, %d:%d)", t.Kind, t.Value, t.Pos.Line, t.Pos.Column)
}

// Lexer tokenises DBML source text.
type Lexer struct {
	src    []byte
	pos    int  // current byte position
	line   int
	col    int
	tokens []Token
	tpos   int // current position in tokens slice
}

// NewLexer creates a Lexer and eagerly tokenises all of src.
func NewLexer(src []byte) (*Lexer, error) {
	l := &Lexer{src: src, line: 1, col: 1}
	if err := l.tokenise(); err != nil {
		return nil, err
	}
	return l, nil
}

func (l *Lexer) tokenise() error {
	for {
		l.skipWhitespaceAndComments()
		if l.pos >= len(l.src) {
			l.tokens = append(l.tokens, Token{Kind: TokEOF, Pos: l.curPos()})
			return nil
		}

		pos := l.curPos()
		ch := l.src[l.pos]

		switch {
		case ch == '{':
			l.tokens = append(l.tokens, Token{TokLBrace, "{", pos})
			l.advance()
		case ch == '}':
			l.tokens = append(l.tokens, Token{TokRBrace, "}", pos})
			l.advance()
		case ch == '[':
			l.tokens = append(l.tokens, Token{TokLBracket, "[", pos})
			l.advance()
		case ch == ']':
			l.tokens = append(l.tokens, Token{TokRBracket, "]", pos})
			l.advance()
		case ch == '(':
			l.tokens = append(l.tokens, Token{TokLParen, "(", pos})
			l.advance()
		case ch == ')':
			l.tokens = append(l.tokens, Token{TokRParen, ")", pos})
			l.advance()
		case ch == ':':
			l.tokens = append(l.tokens, Token{TokColon, ":", pos})
			l.advance()
		case ch == ',':
			l.tokens = append(l.tokens, Token{TokComma, ",", pos})
			l.advance()
		case ch == '.':
			l.tokens = append(l.tokens, Token{TokDot, ".", pos})
			l.advance()
		case ch == '<':
			if l.peek(1) == '>' {
				l.tokens = append(l.tokens, Token{TokIdent, "<>", pos})
				l.advance()
				l.advance()
			} else {
				l.tokens = append(l.tokens, Token{TokLAngle, "<", pos})
				l.advance()
			}
		case ch == '>':
			l.tokens = append(l.tokens, Token{TokRAngle, ">", pos})
			l.advance()
		case ch == '-':
			l.tokens = append(l.tokens, Token{TokDash, "-", pos})
			l.advance()
		case ch == '~':
			l.tokens = append(l.tokens, Token{TokTilde, "~", pos})
			l.advance()
		case ch == '"':
			s, err := l.readQuotedIdent()
			if err != nil {
				return err
			}
			l.tokens = append(l.tokens, Token{TokIdent, s, pos})
		case ch == '\'':
			s, err := l.readString()
			if err != nil {
				return err
			}
			l.tokens = append(l.tokens, Token{TokString, s, pos})
		case ch == '`':
			s, err := l.readBacktick()
			if err != nil {
				return err
			}
			l.tokens = append(l.tokens, Token{TokBacktick, s, pos})
		case ch == '#':
			// Skip hex colour (e.g. #FF0000) or treat as comment-like ident.
			l.advance()
			var sb strings.Builder
			for l.pos < len(l.src) && isHexOrAlnum(l.src[l.pos]) {
				sb.WriteByte(l.src[l.pos])
				l.advance()
			}
			l.tokens = append(l.tokens, Token{TokIdent, "#" + sb.String(), pos})
		case isDigit(ch) || (ch == '-' && l.pos+1 < len(l.src) && isDigit(l.src[l.pos+1])):
			s := l.readNumber()
			l.tokens = append(l.tokens, Token{TokNumber, s, pos})
		case isIdentStart(ch):
			s := l.readIdent()
			l.tokens = append(l.tokens, Token{TokIdent, s, pos})
		default:
			return fmt.Errorf("unexpected character %q at %d:%d", string(ch), pos.Line, pos.Column)
		}
	}
}

func (l *Lexer) skipWhitespaceAndComments() {
	for l.pos < len(l.src) {
		ch := l.src[l.pos]
		if ch == ' ' || ch == '\t' || ch == '\r' || ch == '\n' {
			l.advance()
			continue
		}
		// Single-line comment //
		if ch == '/' && l.peek(1) == '/' {
			for l.pos < len(l.src) && l.src[l.pos] != '\n' {
				l.advance()
			}
			continue
		}
		// Block comment /* ... */
		if ch == '/' && l.peek(1) == '*' {
			l.advance()
			l.advance()
			for l.pos < len(l.src) {
				if l.src[l.pos] == '*' && l.peek(1) == '/' {
					l.advance()
					l.advance()
					break
				}
				l.advance()
			}
			continue
		}
		break
	}
}

func (l *Lexer) readQuotedIdent() (string, error) {
	l.advance() // consume opening "
	var sb strings.Builder
	for l.pos < len(l.src) {
		ch := l.src[l.pos]
		if ch == '"' {
			l.advance()
			return sb.String(), nil
		}
		if ch == '\\' && l.pos+1 < len(l.src) {
			l.advance()
			sb.WriteByte(l.src[l.pos])
			l.advance()
			continue
		}
		sb.WriteByte(ch)
		l.advance()
	}
	return "", fmt.Errorf("unterminated quoted identifier")
}

func (l *Lexer) readString() (string, error) {
	// Check for triple-quoted string '''...'''
	if l.pos+2 < len(l.src) && l.src[l.pos+1] == '\'' && l.src[l.pos+2] == '\'' {
		l.advance()
		l.advance()
		l.advance()
		return l.readTripleQuotedString()
	}
	l.advance() // consume opening '
	var sb strings.Builder
	for l.pos < len(l.src) {
		ch := l.src[l.pos]
		if ch == '\'' {
			l.advance()
			return sb.String(), nil
		}
		if ch == '\\' && l.pos+1 < len(l.src) {
			l.advance()
			sb.WriteByte(l.src[l.pos])
			l.advance()
			continue
		}
		sb.WriteByte(ch)
		l.advance()
	}
	return "", fmt.Errorf("unterminated string literal")
}

func (l *Lexer) readTripleQuotedString() (string, error) {
	var sb strings.Builder
	for l.pos < len(l.src) {
		if l.src[l.pos] == '\'' && l.peek(1) == '\'' && l.peek2(2) == '\'' {
			l.advance()
			l.advance()
			l.advance()
			return sb.String(), nil
		}
		sb.WriteByte(l.src[l.pos])
		l.advance()
	}
	return "", fmt.Errorf("unterminated triple-quoted string")
}

func (l *Lexer) readBacktick() (string, error) {
	l.advance() // consume opening `
	var sb strings.Builder
	for l.pos < len(l.src) {
		ch := l.src[l.pos]
		if ch == '`' {
			l.advance()
			return sb.String(), nil
		}
		sb.WriteByte(ch)
		l.advance()
	}
	return "", fmt.Errorf("unterminated backtick expression")
}

func (l *Lexer) readNumber() string {
	var sb strings.Builder
	if l.pos < len(l.src) && l.src[l.pos] == '-' {
		sb.WriteByte('-')
		l.advance()
	}
	for l.pos < len(l.src) && (isDigit(l.src[l.pos]) || l.src[l.pos] == '.') {
		sb.WriteByte(l.src[l.pos])
		l.advance()
	}
	return sb.String()
}

func (l *Lexer) readIdent() string {
	var sb strings.Builder
	for l.pos < len(l.src) {
		r, size := utf8.DecodeRune(l.src[l.pos:])
		if r == utf8.RuneError || (!unicode.IsLetter(r) && !unicode.IsDigit(r) && r != '_') {
			break
		}
		sb.WriteRune(r)
		l.pos += size
		l.col++
	}
	return sb.String()
}

func (l *Lexer) advance() {
	if l.pos >= len(l.src) {
		return
	}
	if l.src[l.pos] == '\n' {
		l.line++
		l.col = 1
	} else {
		l.col++
	}
	l.pos++
}

func (l *Lexer) peek(offset int) byte {
	p := l.pos + offset
	if p >= len(l.src) {
		return 0
	}
	return l.src[p]
}

func (l *Lexer) peek2(offset int) byte {
	return l.peek(offset)
}

func (l *Lexer) curPos() Position {
	return Position{Line: l.line, Column: l.col}
}

// Next returns the next token.
func (l *Lexer) Next() Token {
	if l.tpos >= len(l.tokens) {
		return Token{Kind: TokEOF}
	}
	t := l.tokens[l.tpos]
	l.tpos++
	return t
}

// Peek returns the token at offset ahead of current position (0 = current).
func (l *Lexer) Peek(offset int) Token {
	i := l.tpos + offset
	if i >= len(l.tokens) {
		return Token{Kind: TokEOF}
	}
	return l.tokens[i]
}

func isIdentStart(ch byte) bool {
	return (ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z') || ch == '_'
}

func isDigit(ch byte) bool {
	return ch >= '0' && ch <= '9'
}

func isHexOrAlnum(ch byte) bool {
	return isDigit(ch) || (ch >= 'a' && ch <= 'f') || (ch >= 'A' && ch <= 'F') ||
		(ch >= 'g' && ch <= 'z') || (ch >= 'G' && ch <= 'Z') || ch == '_'
}
