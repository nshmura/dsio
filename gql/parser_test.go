package gql

import (
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func qry(t *testing.T, query string) SelectExpr {

	l := new(Lexer)
	l.Scanner = NewScanner(strings.NewReader(query))
	err := l.Parse()
	if err != nil {
		t.Fatalf("%v", err)
	}

	q, ok := l.Result.(SelectExpr)
	if !ok {
		t.Fatalf("can't convert %v to SelectExpr", l.Result)
	}

	return q
}

func TestField(t *testing.T) {
	q := qry(t, "SELECT *")
	assert.True(t, q.Field.Asterisk)
	assert.False(t, q.Field.Distinct)
	assert.Equal(t, 0, len(q.Field.Field))
	assert.Equal(t, 0, len(q.Field.DistinctOnField))

	q = qry(t, "SELECT DISTINCT abc, def")
	assert.True(t, q.Field.Distinct)
	assert.Equal(t, "abc", q.Field.Field[0])
	assert.Equal(t, "def", q.Field.Field[1])
	assert.False(t, q.Field.Asterisk)
	assert.Equal(t, 0, len(q.Field.DistinctOnField))

	q = qry(t, "SELECT DISTINCT ON (abc, def) *")
	assert.True(t, q.Field.Asterisk)
	assert.False(t, q.Field.Distinct)
	assert.Equal(t, 0, len(q.Field.Field))
	assert.Equal(t, "abc", q.Field.DistinctOnField[0])
	assert.Equal(t, "def", q.Field.DistinctOnField[1])

	q = qry(t, "SELECT DISTINCT ON (abc, def) uvw, xyz")
	assert.False(t, q.Field.Asterisk)
	assert.False(t, q.Field.Distinct)
	assert.Equal(t, "abc", q.Field.DistinctOnField[0])
	assert.Equal(t, "def", q.Field.DistinctOnField[1])
	assert.Equal(t, "uvw", q.Field.Field[0])
	assert.Equal(t, "xyz", q.Field.Field[1])
}

func TestFrom(t *testing.T) {
	q := qry(t, "SELECT * FROM abc")
	assert.Equal(t, "abc", q.From.Kind.Name)
}

func TestWhereForward(t *testing.T) {
	q := qry(t, "SELECT * FROM Book WHERE a = 1 And b < 'abc' AND c >= true")
	cond, ok := q.Where[0].(ForwardConditionExpr)
	assert.True(t, ok)
	assert.Equal(t, "a", cond.PropertyName)
	assert.Equal(t, OP_EQUALS, cond.Comparator)
	assert.Equal(t, int64(1), cond.Value.V.(int64))

	cond, ok = q.Where[1].(ForwardConditionExpr)
	assert.True(t, ok)
	assert.Equal(t, "b", cond.PropertyName)
	assert.Equal(t, OP_LESS, cond.Comparator)
	assert.Equal(t, "abc", cond.Value.V.(string))

	cond, ok = q.Where[2].(ForwardConditionExpr)
	assert.True(t, ok)
	assert.Equal(t, "c", cond.PropertyName)
	assert.Equal(t, OP_GREATER_EQUALS, cond.Comparator)
	assert.Equal(t, true, cond.Value.V.(bool))

	q = qry(t, "SELECT * FROM Book WHERE abc CONTAINS 'def' AND ust HAS ANCESTOR 'xyz'")
	cond, ok = q.Where[0].(ForwardConditionExpr)
	assert.True(t, ok)
	assert.Equal(t, "abc", cond.PropertyName)
	assert.Equal(t, OP_CONTAINS, cond.Comparator)
	assert.Equal(t, "def", cond.Value.V.(string))

	cond, ok = q.Where[1].(ForwardConditionExpr)
	assert.True(t, ok)
	assert.Equal(t, "ust", cond.PropertyName)
	assert.Equal(t, OP_HAS_ANCESTOR, cond.Comparator)
	assert.Equal(t, "xyz", cond.Value.V.(string))
}

func TestWhereBackward(t *testing.T) {
	q := qry(t, "SELECT * FROM Book WHERE 1 = a And 'abc' < b AND true >= c")

	cond, ok := q.Where[0].(BackwardConditionExpr)
	assert.True(t, ok)
	assert.Equal(t, "a", cond.PropertyName)
	assert.Equal(t, OP_EQUALS, cond.Comparator)
	assert.Equal(t, int64(1), cond.Value.V.(int64))

	cond, ok = q.Where[1].(BackwardConditionExpr)
	assert.True(t, ok)
	assert.Equal(t, "b", cond.PropertyName)
	assert.Equal(t, OP_LESS, cond.Comparator)
	assert.Equal(t, "abc", cond.Value.V.(string))

	cond, ok = q.Where[2].(BackwardConditionExpr)
	assert.True(t, ok)
	assert.Equal(t, "c", cond.PropertyName)
	assert.Equal(t, OP_GREATER_EQUALS, cond.Comparator)
	assert.Equal(t, true, cond.Value.V.(bool))

	q = qry(t, "SELECT * FROM Book WHERE 'def' IN abc AND 'xyz' HAS DESCENDANT ust")
	cond, ok = q.Where[0].(BackwardConditionExpr)
	assert.True(t, ok)
	assert.Equal(t, "abc", cond.PropertyName)
	assert.Equal(t, OP_IN, cond.Comparator)
	assert.Equal(t, "def", cond.Value.V.(string))

	cond, ok = q.Where[1].(BackwardConditionExpr)
	assert.True(t, ok)
	assert.Equal(t, "ust", cond.PropertyName)
	assert.Equal(t, OP_HAS_DESCENDANT, cond.Comparator)
	assert.Equal(t, "xyz", cond.Value.V.(string))
}

func TestWhereKey(t *testing.T) {
	q := qry(t, "SELECT * FROM Book WHERE a = KEY(PROJECT('sample-123'), NAMESPACE('sampe-space'),Auther,'Huxley',Book,1234)")
	cond, ok := q.Where[0].(ForwardConditionExpr)
	assert.True(t, ok)
	assert.Equal(t, "a", cond.PropertyName)
	assert.Equal(t, OP_EQUALS, cond.Comparator)
	assert.Equal(t, TYPE_KEY, cond.Value.Type)

	keyLiteral := cond.Value.V.(KeyLiteralExpr)
	assert.Equal(t, "sample-123", keyLiteral.Project)
	assert.Equal(t, "sampe-space", keyLiteral.Namespace)
	assert.Equal(t, "Auther", keyLiteral.KeyPath[0].Kind)
	assert.Equal(t, "Huxley", keyLiteral.KeyPath[0].Name)
	assert.Equal(t, "Book", keyLiteral.KeyPath[1].Kind)
	assert.Equal(t, int64(1234), keyLiteral.KeyPath[1].ID)
}

func TestWhereBlob(t *testing.T) {
	q := qry(t, "SELECT * FROM Book WHERE a = BLOB('abcd')")
	cond, ok := q.Where[0].(ForwardConditionExpr)
	assert.True(t, ok)
	assert.Equal(t, "a", cond.PropertyName)
	assert.Equal(t, OP_EQUALS, cond.Comparator)
	assert.Equal(t, TYPE_BLOB, cond.Value.Type)
	assert.Equal(t, "abcd", cond.Value.V.(BlobLiteralExpr).Blob)
}

func TestWhereDatetime(t *testing.T) {
	q := qry(t, "SELECT * FROM Book WHERE a = DATETIME('2013-09-29T09:30:20-08:00')")
	cond, ok := q.Where[0].(ForwardConditionExpr)
	assert.True(t, ok)
	assert.Equal(t, "a", cond.PropertyName)
	assert.Equal(t, OP_EQUALS, cond.Comparator)
	assert.Equal(t, TYPE_DATETIME, cond.Value.Type)
	assert.Equal(t, "2013-09-29T09:30:20-08:00", cond.Value.V.(time.Time).Format(time.RFC3339))
}

func TestOrder(t *testing.T) {
	q := qry(t, "SELECT * ORDER BY abc, def ASC, ghi DESC")
	assert.Equal(t, "abc", q.Order[0].PropertyName)
	assert.Equal(t, SORT_NONE, q.Order[0].Sort)
	assert.Equal(t, "def", q.Order[1].PropertyName)
	assert.Equal(t, SORT_ASC, q.Order[1].Sort)
	assert.Equal(t, "ghi", q.Order[2].PropertyName)
	assert.Equal(t, SORT_DESC, q.Order[2].Sort)
}

func TestLimit(t *testing.T) {
	q := qry(t, "SELECT * LIMIT 123")
	assert.Equal(t, 123, q.Limit.Number)

	q = qry(t, "SELECT * LIMIT @limit")
	assert.Equal(t, "limit", q.Limit.Cursor)

	q = qry(t, "SELECT * LIMIT FIRST(@limit, 123)")
	assert.Equal(t, "limit", q.Limit.Cursor)
	assert.Equal(t, 123, q.Limit.Number)
}

func TestOffsetNumber(t *testing.T) {
	q := qry(t, "SELECT * FROM Book OFFSET 1")
	assert.Equal(t, 1, q.Offset.Number)
}

func TestOffsetCursor(t *testing.T) {
	q := qry(t, "SELECT * FROM Book OFFSET @startCursor + 12")

	assert.Equal(t, "startCursor", q.Offset.Cursor)
	assert.Equal(t, 12, q.Offset.Number)
}

func TestSyntaxErr(t *testing.T) {
	query := "SELECT limit"

	l := new(Lexer)
	l.Scanner = NewScanner(strings.NewReader(query))
	err := l.Parse()
	assert.NotNil(t, err)
	t.Log(err)
}

func TestScannerErr(t *testing.T) {
	query := "SELECT 'abc"

	l := new(Lexer)
	l.Scanner = NewScanner(strings.NewReader(query))
	err := l.Parse()
	assert.NotNil(t, err)
	t.Log(err)
}
