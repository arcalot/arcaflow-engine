package expressions

import (
	"regexp"
	"strings"
	"text/scanner"
)

type TokenID string

const (
	IdentifierToken   TokenID = "identifier" // Any valid object name
	StringLiteral     TokenID = "string"     // "string example"
	IntLiteral        TokenID = "int"
	MapDelimiterStart TokenID = "map-delimiter-start" // the [ in map["key"]
	MapDelimiterEnd   TokenID = "map-delimiter-end"   // the ] in map["key"]
	ExpressionStart   TokenID = "expression-start"
	ExpressionEnd     TokenID = "expression-end"
	DotObjectAccess   TokenID = "object-access" // The . in a.b (dot notation)
	RootAccess        TokenID = "root-access"
	CurrentNodeAccess TokenID = "current-node-access"
	Equals            TokenID = "equals-sign"
	Selector          TokenID = "selector"
	Filter            TokenID = "filter"
	Negation          TokenID = "negation-sign"
	Wildcard          TokenID = "wildcard"
	UnknownToken      TokenID = "error"
)

// The token parsed from the expression the tokenizer was initialized with.
// The line number and column is relative to the beginning of the expression.
// If part of a greater file, it's recommended that you offset those values to
// get the line and column within the file to prevent confusion.
type TokenValue struct {
	Value    string
	Token_id TokenID
	Filename string
	Line     int
	Column   int
}

// Used for reading tokens of an expression.
type Tokenizer struct {
	s      scanner.Scanner
	reader *strings.Reader
}

type TokenPattern struct {
	TokenID TokenID
	*regexp.Regexp
}

var tokenPatterns = []TokenPattern{
	{IntLiteral, regexp.MustCompile(`^0$|^[1-9]\d*$`)},   // Note: numbers that start with 0 are identifiers.
	{IdentifierToken, regexp.MustCompile(`^\w+$`)},       // Any valid object name
	{StringLiteral, regexp.MustCompile(`^".*"$|^'.*'$`)}, // "string example"
	{MapDelimiterStart, regexp.MustCompile(`^\[$`)},      // the [ in map["key"]
	{MapDelimiterEnd, regexp.MustCompile(`^\]$`)},        // the ] in map["key"]
	{ExpressionStart, regexp.MustCompile(`^\($`)},        // (
	{ExpressionEnd, regexp.MustCompile(`^\)$`)},          // )
	{DotObjectAccess, regexp.MustCompile(`^\.$`)},        // .
	{RootAccess, regexp.MustCompile(`^\$$`)},             // $
	{CurrentNodeAccess, regexp.MustCompile(`^@$`)},       // @
	{Equals, regexp.MustCompile(`^=$`)},                  // =
	{Selector, regexp.MustCompile(`^\:$`)},               // :
	{Filter, regexp.MustCompile(`^\?$`)},                 // ?
	{Negation, regexp.MustCompile(`^\-$`)},               // -
	{Wildcard, regexp.MustCompile(`^\*$`)},               // *
}

func InitTokenizer(expression string, source_name string) *Tokenizer {
	var t Tokenizer
	t.reader = strings.NewReader(expression)
	t.s.Init(t.reader)
	t.s.Filename = source_name
	return &t
}

// Checks to see if it has reached the end of the expression.
func (t *Tokenizer) HasNextToken() bool {
	return t.s.Peek() != scanner.EOF
}

// Gets the next token type and value.
func (t *Tokenizer) GetNext() (*TokenValue, error) {
	t.s.Scan()
	tokenValue := t.s.TokenText()
	for _, tokenPattern := range tokenPatterns {
		if tokenPattern.Regexp.MatchString(tokenValue) {
			return &TokenValue{tokenValue, tokenPattern.TokenID, t.s.Filename, t.s.Line, t.s.Column}, nil
		}
	}
	result := TokenValue{tokenValue, UnknownToken, t.s.Filename, t.s.Line, t.s.Column}
	return &result, &InvalidTokenError{result}
}
