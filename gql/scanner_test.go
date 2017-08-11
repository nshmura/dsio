package gql

import (
	"bytes"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestScannerSpecialTokens(t *testing.T) {
	q := " "
	s := NewScanner(strings.NewReader(q))

	tok, lit := s.ScanIncludeWihtespace()
	assert.Equal(t, WS, int(tok))
	assert.Equal(t, " ", lit)

	tok, lit = s.ScanIncludeWihtespace()
	assert.Equal(t, EOF, int(tok))
	assert.Equal(t, "", lit)
}

func TestScannerLiteralsOK(t *testing.T) {
	q := "name 'string' true false 12 -23 1.23 -12.3 1.23e10 1.23e-29"
	s := NewScanner(strings.NewReader(q))

	tok, lit := scan(s)
	assert.Equal(t, NAME, tok)
	assert.Equal(t, "name", lit)

	tok, lit = scan(s)
	assert.Equal(t, STRING, tok)
	assert.Equal(t, "string", lit)

	tok, lit = scan(s)
	assert.Equal(t, TRUE, tok)
	assert.Equal(t, "true", lit)

	tok, lit = scan(s)
	assert.Equal(t, FALSE, tok)
	assert.Equal(t, "false", lit)

	tok, lit = scan(s)
	assert.Equal(t, INTEGER, tok)
	assert.Equal(t, "12", lit)

	tok, lit = scan(s)
	assert.Equal(t, INTEGER, tok)
	assert.Equal(t, "-23", lit)

	tok, lit = scan(s)
	assert.Equal(t, DOUBLE, tok)
	assert.Equal(t, "1.23", lit)

	tok, lit = scan(s)
	assert.Equal(t, DOUBLE, tok)
	assert.Equal(t, "-12.3", lit)

	tok, lit = scan(s)
	assert.Equal(t, DOUBLE, tok)
	assert.Equal(t, "1.23e10", lit)

	tok, lit = scan(s)
	assert.Equal(t, DOUBLE, tok)
	assert.Equal(t, "1.23e-29", lit)
}

func TestScannerNameNG(t *testing.T) {
	q := " 'name "
	s := NewScanner(strings.NewReader(q))

	tok, _ := scan(s)
	assert.Equal(t, ILLEGAL, tok)
}

func TestScannerStringNG(t *testing.T) {
	q := " 'a "
	s := NewScanner(strings.NewReader(q))

	tok, _ := scan(s)
	assert.Equal(t, ILLEGAL, tok)
}

func TestScannerDoubleNG(t *testing.T) {
	q := " 1.2e "
	s := NewScanner(strings.NewReader(q))

	tok, _ := scan(s)
	assert.Equal(t, ILLEGAL, tok)
}

func TestScannerMiscOK(t *testing.T) {
	q := "* + , = < > ( )"
	s := NewScanner(strings.NewReader(q))

	tok, lit := scan(s)
	assert.Equal(t, ASTERISK, tok)
	assert.Equal(t, "*", lit)

	tok, lit = scan(s)
	assert.Equal(t, PLUS, tok)
	assert.Equal(t, "+", lit)

	tok, lit = scan(s)
	assert.Equal(t, COMMA, tok)
	assert.Equal(t, ",", lit)

	tok, lit = scan(s)
	assert.Equal(t, EQUAL, tok)
	assert.Equal(t, "=", lit)

	tok, lit = scan(s)
	assert.Equal(t, LEFT_BRACKETS, tok)
	assert.Equal(t, "<", lit)

	tok, lit = scan(s)
	assert.Equal(t, RIGHT_BRACKETS, tok)
	assert.Equal(t, ">", lit)

	tok, lit = scan(s)
	assert.Equal(t, LEFT_ROUND, tok)
	assert.Equal(t, "(", lit)

	tok, lit = scan(s)
	assert.Equal(t, RIGHT_ROUND, tok)
	assert.Equal(t, ")", lit)
}

func TestScannerKeywordsOK(t *testing.T) {

	var buf bytes.Buffer
	var klist []string
	var tlist []int
	for k, tok := range keywords {
		buf.WriteString(k)
		buf.WriteString(" ")
		klist = append(klist, k)
		tlist = append(tlist, int(tok))
	}

	q := buf.String()
	s := NewScanner(strings.NewReader(q))

	for i := range klist {
		tok, lit := scan(s)
		assert.Equal(t, tlist[i], tok)
		assert.Equal(t, klist[i], lit)
	}
}

func TestScannerGQL(t *testing.T) {
	q := "SELECT * FROM Book LIMIT 1"
	s := NewScanner(strings.NewReader(q))

	tok, _ := scan(s)
	assert.Equal(t, SELECT, tok)

	tok, _ = scan(s)
	assert.Equal(t, ASTERISK, tok)

	tok, _ = scan(s)
	assert.Equal(t, FROM, tok)

	tok, _ = scan(s)
	assert.Equal(t, NAME, tok)

	tok, _ = scan(s)
	assert.Equal(t, LIMIT, tok)

	tok, _ = scan(s)
	assert.Equal(t, INTEGER, tok)
}

func scan(s *Scanner) (int, string) {
	tok, lit := s.Scan()
	return int(tok), lit
}
