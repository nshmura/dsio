package gql

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"strings"
)

// TokenType represents a lexical token.
type TokenType int

var (
	micsChars = map[rune]TokenType{
		'*': ASTERISK,
		',': COMMA,
		'+': PLUS,
		'=': EQUAL,
		'<': LEFT_BRACKETS,
		'>': RIGHT_BRACKETS,
		'(': LEFT_ROUND,
		')': RIGHT_ROUND,
	}

	keywords = map[string]TokenType{
		"SELECT":     SELECT,
		"DISTINCT":   DISTINCT,
		"ON":         ON,
		"FROM":       FROM,
		"WHERE":      WHERE,
		"ASC":        ASC,
		"DESC":       DESC,
		"ORDER":      ORDER,
		"BY":         BY,
		"LIMIT":      LIMIT,
		"FIRST":      FIRST,
		"OFFSET":     OFFSET,
		"AND":        AND,
		"IS":         IS,
		"NULL":       NULL,
		"CONTAINS":   CONTAINS,
		"HAS":        HAS,
		"ANCESTOR":   ANCESTOR,
		"DESCENDANT": DESCENDANT,
		"IN":         IN,
		"KEY":        KEY,
		"PROJECT":    PROJECT,
		"NAMESPACE":  NAMESPACE,
		"BLOB":       BLOB,
		"DATETIME":   DATETIME,
		"TRUE":       TRUE,
		"FALSE":      FALSE,
	}
)

// Scanner represents a lexical scanner.
type Scanner struct {
	r   *bufio.Reader
	buf []rune
}

// NewScanner returns a new instance of Scanner.
func NewScanner(r io.Reader) *Scanner {
	return &Scanner{
		r: bufio.NewReader(r),
	}
}

// read reads the next rune from the bufferred reader.
// Returns the rune(0) if an error occurs (or io.EOF is returned).
func (s *Scanner) read() rune {
	ch, _, err := s.r.ReadRune()
	if err != nil {
		return eof
	}
	s.buf = append(s.buf, ch)
	return ch
}

// unread places the previously read rune back on the reader.
func (s *Scanner) unread() {
	_ = s.r.UnreadRune()
	s.buf = s.buf[0 : len(s.buf)-1]
}

func (s *Scanner) Consumed() string {
	return string(s.buf)
}

// Scan scans the next non-whitespace token.
func (s *Scanner) Scan() (tok TokenType, lit string) {
	tok, lit = s.ScanIncludeWihtespace()
	if tok == WS {
		tok, lit = s.ScanIncludeWihtespace()
	}
	return
}

// ScanIncludeWihtespace returns the next token and literal value.
func (s *Scanner) ScanIncludeWihtespace() (tok TokenType, lit string) {
	// Read the next rune.
	ch := s.read()

	// If we see whitespace then consume all contiguous whitespace.
	// If we see a letter then consume as an ident or reserved word.
	if isWhitespace(ch) {
		s.unread()
		return s.scanWhitespace()

	} else if isDigit(ch) || ch == '-' {
		s.unread()
		return s.scanDecimalDigits()

	} else if ch == '+' {
		if next := s.read(); isDigit(next) {
			s.unread()
			s.unread()
			return s.scanDecimalDigits()
		}

	} else if isLetter(ch) {
		s.unread()
		return s.scanName()

	} else if isQuote(ch) {
		s.unread()
		return s.scanQuotedString(STRING)

	} else if isBackquote(ch) {
		s.unread()
		return s.scanQuotedString(NAME)

	} else if ch == '@' {
		return s.scanBindingSite()
	}

	// Otherwise read the individual character.
	if t, ok := micsChars[ch]; ok {
		return t, string(ch)
	}
	if ch == eof {
		return EOF, ""
	}

	return ILLEGAL, string(ch)
}

// scanWhitespace consumes the current rune and all contiguous whitespace.
func (s *Scanner) scanWhitespace() (tok TokenType, lit string) {
	var buf bytes.Buffer
	buf.WriteRune(s.read())

	for {
		if ch := s.read(); ch == eof {
			break
		} else if !isWhitespace(ch) {
			s.unread()
			break
		} else {
			buf.WriteRune(ch)
		}
	}

	return WS, buf.String()
}

// scanName consumes the current rune and all contiguous name runes.
func (s *Scanner) scanName() (tok TokenType, lit string) {
	var buf bytes.Buffer
	buf.WriteRune(s.read())

	for {
		if ch := s.read(); ch == eof {
			break
		} else if !isLetter(ch) && !isDigit(ch) && ch != '_' && ch != '.' {
			s.unread()
			break
		} else {
			_, _ = buf.WriteRune(ch)
		}
	}

	if t, ok := keywords[strings.ToUpper(buf.String())]; ok {
		return t, buf.String()
	}

	return NAME, buf.String()
}

// scanBindingSite consumes the current rune and all contiguous binding-site runes.
func (s *Scanner) scanBindingSite() (tok TokenType, lit string) {
	tok, lit = s.scanName()
	return BINDING_SITE, lit
}

// scanDecimalDigits consumes the current rune and all contiguous number runes.
func (s *Scanner) scanDecimalDigits() (tok TokenType, lit string) {
	var buf bytes.Buffer
	buf.WriteRune(s.read())

	for {
		ch := s.read()
		if ch == eof {
			break
		} else if isNumber(ch) {
			buf.WriteRune(ch)

		} else if ch == '.' {
			s.unread()
			return s.scanDouble(buf)

		} else {
			s.unread()
			break
		}
	}

	return INTEGER, buf.String()
}

// scanDouble consumes the current rune and all contiguous double number runes.
func (s *Scanner) scanDouble(buf bytes.Buffer) (tok TokenType, lit string) {
	buf.WriteRune(s.read())

	for {
		ch := s.read()
		if ch == eof {
			break

		} else if isNumber(ch) {
			buf.WriteRune(ch)

		} else if ch == 'E' || ch == 'e' {
			s.unread()
			return s.scanDoubleExp(buf)

		} else if ch == '.' {
			buf.WriteRune(ch)
			return ILLEGAL, buf.String()

		} else {
			s.unread()
			break
		}
	}

	return DOUBLE, buf.String()
}

func (s *Scanner) scanDoubleExp(buf bytes.Buffer) (tok TokenType, lit string) {
	buf.WriteRune(s.read())

	next := s.read()

	if isNumber(next) {
		buf.WriteRune(next)

	} else if next == '-' {
		next2 := s.read()
		buf.WriteRune(next)

		if isNumber(next2) {
			buf.WriteRune(next2)

		} else {
			buf.WriteRune(next2)
			return ILLEGAL, buf.String()
		}
	} else {
		buf.WriteRune(next)
		return ILLEGAL, buf.String()
	}

	for {
		if ch := s.read(); !isNumber(ch) {
			s.unread()
			break
		} else {
			buf.WriteRune(ch)
		}
	}

	return DOUBLE, buf.String()
}

// scanString consumes the current rune and all contiguous string runes.
func (s *Scanner) scanQuotedString(expected TokenType) (tok TokenType, lit string) {

	start := s.read()

	var buf bytes.Buffer

	for {
		ch := s.read()
		if ch == eof {
			return ILLEGAL, fmt.Sprintf("%c%v", start, buf.String())

		} else if ch == newline {
			return ILLEGAL, fmt.Sprintf("%c%v", start, buf.String())

		} else if ch == start {
			next := s.read()

			if next == start {
				_, _ = buf.WriteRune(ch)
				_, _ = buf.WriteRune(next)

			} else {
				s.unread()
				break
			}

		} else {
			_, _ = buf.WriteRune(ch)
		}
	}

	return expected, buf.String()
}
