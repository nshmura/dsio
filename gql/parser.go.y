%{
package gql

import (
    "fmt"
    "strconv"
    "time"

	"cloud.google.com/go/datastore"
)

type Token struct {
    token   int
    literal string
}

type Expression interface{}

type SelectExpr struct {
    Field FieldExpr
    From *FromExpr
    Where []ConditionExpr
    Order []OrderExpr
    Limit *LimitExpr
    Offset *OffsetExpr
}

type FieldExpr struct {
    Distinct         bool
    DistinctOnField []string
    Field           []string
    Asterisk         bool
}

type FromExpr struct {
    Kind *KindExpr
}

type ConditionExpr interface{
    GetPropertyName() string
    GetValue() ValueExpr
    GetComparator() ComparatorExpr
}

type IsNullConditionExpr struct {
    PropertyName string
}

func (c IsNullConditionExpr) GetPropertyName() string {
    return c.PropertyName
}

func (c IsNullConditionExpr) GetValue() ValueExpr {
    return ValueExpr{}
}

func (c IsNullConditionExpr) GetComparator() ComparatorExpr {
    return OP_IS_NULL
}

type ForwardConditionExpr struct {
    PropertyName string
    Comparator ComparatorExpr
    Value ValueExpr
}

func (c ForwardConditionExpr) GetPropertyName() string {
    return c.PropertyName
}

func (c ForwardConditionExpr) GetValue() ValueExpr {
    return c.Value
}

func (c ForwardConditionExpr) GetComparator() ComparatorExpr {
    return c.Comparator
}

type BackwardConditionExpr struct {
    Value ValueExpr
    Comparator ComparatorExpr
    PropertyName string
}

func (c BackwardConditionExpr) GetPropertyName() string {
    return c.PropertyName
}

func (c BackwardConditionExpr) GetValue() ValueExpr {
    return c.Value
}

func (c BackwardConditionExpr) GetComparator() ComparatorExpr {
    return c.Comparator
}

type OrderExpr struct {
    PropertyName string
    Sort SortType
}

type LimitExpr struct {
    Cursor string
    Number int
}

type OffsetExpr struct {
    Cursor string
    Number int
}

type ResultPositionExpr struct {
    Number int
    BindingSite string
}

type ValueExpr struct {
    Type ValueType
    V interface{}
}

type KeyLiteralExpr struct {
    Project string
    Namespace string
    KeyPath []KeyPathElementExpr
}

func (k KeyLiteralExpr) ToDatastoreKey(defaultNamespace string) *datastore.Key {
	namespace := defaultNamespace
	if k.Namespace != "" {
		namespace = k.Namespace
	}
	return datastoreKeyOf(namespace, k.KeyPath) // what to do with project ID? Go API doesn't have a field for it
}

func datastoreKeyOf(namespace string, keyPath []KeyPathElementExpr) (k *datastore.Key) {
    if len(keyPath) == 0 {
    	return nil
    }
    parent := datastoreKeyOf(namespace, keyPath[:len(keyPath) - 1])
    i := keyPath[len(keyPath) - 1]
    if i.Name != "" {
        k = datastore.NameKey(i.Kind, i.Name, parent)
    } else {
        k = datastore.IDKey(i.Kind, i.ID, parent)
    }
    if namespace != "" {
        k.Namespace = namespace
    }
    return
}

type BlobLiteralExpr struct {
    Blob string
}

type DatetimeLiteralExpr struct {
    Datetime time.Time
}

type KeyPathElementExpr struct {
    Kind string
    ID   int64
    Name string
}

type KindExpr struct {
    Name string
}

type PropertyNameExpr struct {
    Name string
}

type ComparatorExpr int

func (c ComparatorExpr) String() string {
    switch c {
    case OP_IS_NULL:
        return "IS NULL"
    case OP_CONTAINS:
        return "CONTAINS"
    case OP_HAS_ANCESTOR:
        return "HAS ANCESTOR"
    case OP_IN:
        return "IN"
    case OP_HAS_DESCENDANT:
        return "HAS DESCENDANT"
    case OP_EQUALS:
        return "="
    case OP_LESS:
        return "<"
    case OP_LESS_EQUALS:
        return "<="
    case OP_GREATER:
        return ">"
    case OP_GREATER_EQUALS:
        return ">="
    }
    return ""
}

const (
    OP_IS_NULL ComparatorExpr = 1 << iota
    OP_CONTAINS
    OP_HAS_ANCESTOR
    OP_IN
    OP_HAS_DESCENDANT
    OP_EQUALS           // =
    OP_LESS             // <
    OP_LESS_EQUALS      // <=
    OP_GREATER          // >
    OP_GREATER_EQUALS   // >=
)

type ValueType int

const (
    TYPE_BINDING_SITE ValueType = 1 << iota
    TYPE_KEY
    TYPE_BLOB
    TYPE_DATETIME
    TYPE_STRING
    TYPE_INTEGER
    TYPE_DOUBLE
    TYPE_BOOL
    TYPE_NULL
)

type SortType int

const (
    SORT_NONE SortType = iota
    SORT_ASC
    SORT_DESC
)

%}

%union{
    token Token
    expr  Expression
}

%type<expr> query
%type<expr> select
%type<expr> Field
%type<expr> property_names
%type<expr> opt_from
%type<expr> opt_where
%type<expr> opt_limit
%type<expr> opt_order
%type<expr> orders
%type<expr> opt_asc_desc
%type<expr> opt_offset
%type<expr> compound_condition
%type<expr> condition
%type<expr> forward_comparator
%type<expr> backward_comparator
%type<expr> either_comparator
%type<expr> result_position
%type<expr> synthetic_literal
%type<expr> opt_project
%type<expr> opt_namespace
%type<expr> key_path_elements
%type<expr> key_path_element
%type<expr> kind
%type<expr> property_name
%type<expr> value

/* Special tokens */
%token<token> ILLEGAL
%token<token> EOF
%token<token> WS

/* Literals */
%token<token> NAME         // Field, table_name
%token<token> BINDING_SITE // binding-site
%token<token> STRING       // string literal
%token<token> INTEGER      // integer literal
%token<token> DOUBLE       // double literal

/* Misc characters */
%token<token> ASTERISK       // *
%token<token> PLUS           // +
%token<token> COMMA          // ,
%token<token> EQUAL          // =
%token<token> LEFT_BRACKETS  // <
%token<token> RIGHT_BRACKETS // >
%token<token> LEFT_ROUND     // (
%token<token> RIGHT_ROUND    // )

/* Keywords */
%token<token> SELECT
%token<token> DISTINCT
%token<token> ON
%token<token> FROM
%token<token> WHERE
%token<token> ASC
%token<token> DESC
%token<token> ORDER
%token<token> BY
%token<token> LIMIT
%token<token> FIRST
%token<token> OFFSET
%token<token> AND
%token<token> IS
%token<token> NULL
%token<token> CONTAINS
%token<token> HAS
%token<token> ANCESTOR
%token<token> DESCENDANT
%token<token> IN
%token<token> KEY
%token<token> PROJECT
%token<token> NAMESPACE
%token<token> BLOB
%token<token> DATETIME
%token<token> TRUE       // true literal
%token<token> FALSE      // false literal

%left PLUS

%%

query
    : select
    {
        $$ = $1
        yylex.(*Lexer).Result = $$
    }

select
    : SELECT Field opt_from opt_where opt_order opt_limit opt_offset
    {
        fieldExpr := $2
        fromExpr := $3
        whereExpr := $4
        orderExpr := $5
        limitExpr := $6
        offsetExpr := $7

        var from *FromExpr
        if fromExpr != nil {
            from = fromExpr.(*FromExpr)
        }

        var limit *LimitExpr
        if limitExpr != nil {
            limit = limitExpr.(*LimitExpr)
        }

        var offset *OffsetExpr
        if offsetExpr != nil {
            offset = offsetExpr.(*OffsetExpr)
        }

        $$ = SelectExpr{
            Field: fieldExpr.(FieldExpr),
            From: from,
            Where: whereExpr.([]ConditionExpr),
            Order: orderExpr.([]OrderExpr),
            Limit: limit,
            Offset: offset,
        }
    }

Field
    : ASTERISK
    {
        $$ = FieldExpr{Asterisk: true}
    }
    | property_names
    {
        $$ = FieldExpr{Field: $1.([]string)}
    }
    | DISTINCT property_names
    {
        $$ = FieldExpr{
            Distinct: true,
            Field: $2.([]string),
        }
    }
    | DISTINCT ON LEFT_ROUND property_names RIGHT_ROUND ASTERISK
    {
        $$ = FieldExpr{
            DistinctOnField: $4.([]string),
            Asterisk: true,
        }
    }
    | DISTINCT ON LEFT_ROUND property_names RIGHT_ROUND property_names
    {
        $$ = FieldExpr{
            DistinctOnField: $4.([]string),
            Field: $6.([]string),
        }
    }

property_names
    : property_name
    {
        $$ = []string{ $1.(string) }
    }
    | property_names COMMA property_name
    {
        $$ = append($1.([]string), $3.(string))
    }

opt_from
    :
    {
        $$ = nil
    }
    | FROM kind
    {
        kind := $2.(KindExpr)
        $$ = &FromExpr{Kind: &kind}
    }

opt_where
    :
    {
        $$ = make([]ConditionExpr, 0)
    }
    | WHERE compound_condition
    {
        $$ = $2.([]ConditionExpr)
    }

compound_condition
    : condition
    {
        $$ = []ConditionExpr{ $1.(ConditionExpr) }
    }
    | compound_condition AND condition
    {
        $$ = append($1.([]ConditionExpr), $3.(ConditionExpr))
    }

condition
    : property_name IS NULL
    {
        $$ = IsNullConditionExpr{
            PropertyName: $1.(string),
        }
    }
    | property_name forward_comparator value
    {
        $$ = ForwardConditionExpr{
            PropertyName: $1.(string),
            Comparator: $2.(ComparatorExpr),
            Value: $3.(ValueExpr),
        }
    }
    | value backward_comparator property_name
    {
        $$ = BackwardConditionExpr{
            Value: $1.(ValueExpr),
            Comparator: $2.(ComparatorExpr),
            PropertyName: $3.(string),
        }
    }

forward_comparator
    : either_comparator
    {
        $$ = $1
    }
    | CONTAINS
    {
        $$ = OP_CONTAINS
    }
    | HAS ANCESTOR
    {
        $$ = OP_HAS_ANCESTOR
    }

backward_comparator
    : either_comparator
    {
        $$ = $1
    }
    | IN
    {
        $$ = OP_IN
    }
    | HAS DESCENDANT
    {
        $$ = OP_HAS_DESCENDANT
    }

either_comparator
    : EQUAL
    {
        $$ = OP_EQUALS
    }
    | LEFT_BRACKETS
    {
        $$ = OP_LESS
    }
    | LEFT_BRACKETS EQUAL
    {
        $$ = OP_LESS_EQUALS
    }
    | RIGHT_BRACKETS
    {
        $$ = OP_GREATER
    }
    | RIGHT_BRACKETS EQUAL
    {
        $$ = OP_GREATER_EQUALS
    }

opt_order
    :
    {
        $$ = []OrderExpr{}
    }
    | ORDER BY orders
    {
        $$ = $3.([]OrderExpr)
    }

orders
    : property_name opt_asc_desc
    {
        $$ = []OrderExpr{
            OrderExpr{PropertyName: $1.(string), Sort: $2.(SortType)},
        }
    }
    | orders COMMA property_name opt_asc_desc
    {
        o := OrderExpr{PropertyName: $3.(string), Sort: $4.(SortType)}
        $$ = append($1.([]OrderExpr), o)
    }

opt_asc_desc
    :
    {
        $$ = SORT_NONE
    }
    | ASC
    {
        $$ = SORT_ASC
    }
    | DESC
    {
        $$ = SORT_DESC
    }

opt_limit
    :
    {
        $$ = nil
    }
    | LIMIT result_position
    {
        $$ = &LimitExpr{
            Cursor: $2.(ResultPositionExpr).BindingSite,
            Number: $2.(ResultPositionExpr).Number,
        }
    }
    | LIMIT FIRST LEFT_ROUND result_position COMMA result_position RIGHT_ROUND
    {
        $$ = &LimitExpr{
            Cursor: $4.(ResultPositionExpr).BindingSite,
            Number: $6.(ResultPositionExpr).Number,
        }
    }

opt_offset
    :
    {
        $$ = nil
    }
    | OFFSET result_position
    {
        if $2.(ResultPositionExpr).BindingSite != "" {
            $$ = &OffsetExpr{Cursor: $2.(ResultPositionExpr).BindingSite}
        } else {
            $$ = &OffsetExpr{Number: $2.(ResultPositionExpr).Number}
        }
    }
    | OFFSET result_position PLUS result_position
    {
        $$ = &OffsetExpr{
            Cursor: $2.(ResultPositionExpr).BindingSite,
            Number: $4.(ResultPositionExpr).Number,
        }
    }

result_position
    : BINDING_SITE
    {
        $$ = ResultPositionExpr{BindingSite: $1.literal}
    }
    | INTEGER
    {
        number, err := strconv.Atoi($1.literal)
        if err != nil {
            panic(fmt.Sprintf("can't convert %v to integer", $1.literal))
        }
        $$ = ResultPositionExpr{Number: number}
    }

value
    : BINDING_SITE
    {
        $$ = ValueExpr{Type:TYPE_BINDING_SITE, V:$1.literal }
    }
    | synthetic_literal
    {
        switch t := $1.(type) {
        case KeyLiteralExpr:
            $$ = ValueExpr{Type:TYPE_KEY, V:$1 }
        case BlobLiteralExpr:
            $$ = ValueExpr{Type:TYPE_BLOB, V:$1 }
        case DatetimeLiteralExpr:
            $$ = ValueExpr{Type:TYPE_DATETIME, V:t.Datetime }
        default:
            panic(fmt.Sprintf("unkown synthetic_literal:%v", $1))
        }
    }
    | STRING
    {
        $$ = ValueExpr{Type:TYPE_STRING, V:$1.literal }
    }
    | INTEGER
    {
        number, err := strconv.ParseInt($1.literal, 10, 64)
        if err != nil {
            panic(fmt.Sprintf("can't convert %v to integer", $1.literal))
        }
        $$ = ValueExpr{Type:TYPE_INTEGER, V:number }
    }
    | DOUBLE
    {
        double, err := strconv.ParseFloat($1.literal, 64)
        if err != nil {
            panic(fmt.Sprintf("can't convert %v to double", $1.literal))
        }
        $$ = ValueExpr{Type:TYPE_DOUBLE, V:double }
    }
    | TRUE
    {
        $$ = ValueExpr{Type:TYPE_BOOL, V:true }
    }
    | FALSE
    {
        $$ = ValueExpr{Type:TYPE_BOOL, V:false }
    }
    | NULL
    {
        $$ = ValueExpr{Type:TYPE_NULL, V:nil }
    }

synthetic_literal
    : KEY LEFT_ROUND opt_project opt_namespace key_path_elements RIGHT_ROUND
    {
        $$ = KeyLiteralExpr{
            Project: $3.(string),
            Namespace: $4.(string),
            KeyPath: $5.([]KeyPathElementExpr),
        }
    }
    | BLOB LEFT_ROUND STRING RIGHT_ROUND
    {
        $$ = BlobLiteralExpr {Blob: $3.literal}
    }
    | DATETIME LEFT_ROUND STRING RIGHT_ROUND
    {
        t, err := time.Parse(time.RFC3339 , $3.literal)
        if err != nil {
            panic(fmt.Sprintf("can't convert %v to datime", $3.literal))
        }
        $$ = DatetimeLiteralExpr {Datetime: t}
    }

opt_project
    :
    {
        $$ = ""
    }
    | PROJECT LEFT_ROUND STRING RIGHT_ROUND COMMA
    {
        $$ = $3.literal
    }

opt_namespace
    :
    {
        $$ = ""
    }
    | NAMESPACE LEFT_ROUND STRING RIGHT_ROUND COMMA
    {
        $$ = $3.literal
    }

key_path_elements
    : key_path_element
    {
        $$ = []KeyPathElementExpr{ $1.(KeyPathElementExpr) }
    }
    | key_path_elements COMMA key_path_element
    {
        $$ = append($1.([]KeyPathElementExpr), $3.(KeyPathElementExpr))
    }

key_path_element
    : kind COMMA INTEGER
    {
        number, err := strconv.ParseInt($3.literal, 10, 64)
        if err != nil {
            panic(fmt.Sprintf("can't convert %v to integer", $3.literal))
        }
        $$ = KeyPathElementExpr{Kind: $1.(KindExpr).Name, ID:number}
    }
    | kind COMMA STRING
    {
        $$ = KeyPathElementExpr{Kind: $1.(KindExpr).Name, Name:$3.literal}
    }

kind
    : NAME
    {
        $$ = KindExpr{Name: $1.literal}
    }

property_name
    : NAME
    {
        $$ = $1.literal
    }
%%

type Lexer struct {
    Scanner *Scanner
    Result Expression
    parserErr error
    scannerErr error
}

func (l *Lexer) Lex(lval *yySymType) int {
    token, literal := l.Scanner.Scan()

    if token == EOF {
        return 0

    } else if token == ILLEGAL {
        l.scannerErr = fmt.Errorf("invalid token: %v", literal)
        return 0
    }

    lval.token = Token{token: int(token), literal: literal}
    return int(token)
}

func (l *Lexer) Error(e string) {
    if l.scannerErr == nil {
        l.parserErr = fmt.Errorf("%v: %v\n", e, l.Scanner.Consumed())
    }
}

func (l *Lexer) Parse() error {
    yyParse(l)

    if l.parserErr != nil {
        return l.parserErr
    } else {
        return l.scannerErr
    }
}
