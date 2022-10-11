package expressions

import (
	"regexp"
	"strings"
	"text/scanner"
)

// TokenID Represents the name of a type of token that has a pattern.
type TokenID string

const (
	// IdentifierToken represents a token with any valid object name.
	IdentifierToken TokenID = "identifier"
	// StringLiteralToken represents a token that has a sequence of characters.
	// Currently supports the string format used in golang, and will include
	// the " before and after the conents of the string.
	// Characters can be escaped the common way with a backslash.
	StringLiteralToken TokenID = "string"
	// IntLiteralToken represents an integer token. Must not start with 0.
	IntLiteralToken TokenID = "int"
	// BracketAccessDelimiterStartToken represents the token before an object
	//  access. The '[' in 'obj["key"]'.
	//nolint:gosec
	BracketAccessDelimiterStartToken TokenID = "map-delimiter-start"
	// BracketAccessDelimiterEndToken represents the token before an object
	// access. The '[' in 'obj["key"]'.
	//nolint:gosec
	BracketAccessDelimiterEndToken TokenID = "map-delimiter-end"
	// ExpressionStartToken represents the start token of a sub-expression
	// inside of an object bracket access. The '(' in 'obj[($.a)]'.
	ExpressionStartToken TokenID = "expression-start"
	// ExpressionEndToken represents the end token of a sub-expression inside
	// of an object bracket access. The ')' in 'obj[($.a)]'.
	ExpressionEndToken TokenID = "expression-end"
	// DotObjectAccessToken represents the '.' token in 'a.b' (dot notation).
	DotObjectAccessToken TokenID = "object-access"
	// RootAccessToken represents the token that identifies accessing the
	// root object.
	RootAccessToken TokenID = "root-access"
	// CurrentObjectAccessToken represents the token, @, that identifies the current
	// object in a filter.
	CurrentObjectAccessToken TokenID = "current-object-access"
	// EqualsToken represents the token that represents a single equals sign.
	EqualsToken TokenID = "equals-sign"
	// SelectorToken Represents the ':' character used in selector expressions in bracket
	// object access.
	SelectorToken TokenID = "selector"
	// FilterToken represents the '?' used in filter expressions in bracket object access.
	FilterToken TokenID = "filter"
	// NegationToken represents a negation sign '-'.
	//nolint:gosec
	NegationToken TokenID = "negation-sign"
	// WildcardToken represents a wildcard token '*'.
	WildcardToken TokenID = "wildcard"
	// UnknownToken is a placeholder for when there was an error in the token.
	UnknownToken TokenID = "error"
)

// TokenValue represents the token parsed from the expression the tokenizer
// was initialized with.
// The line number and column is relative to the beginning of the expression.
// If part of a greater file, it's recommended that you offset those values to
// get the line and column within the file to prevent confusion.
type TokenValue struct {
	Value    string
	TokenID  TokenID
	Filename string
	Line     int
	Column   int
}

// Tokenizer is used for reading tokens of an expression.
type Tokenizer struct {
	s      scanner.Scanner
	reader *strings.Reader
}

type tokenPattern struct {
	TokenID TokenID
	*regexp.Regexp
}

var tokenPatterns = []tokenPattern{
	{IntLiteralToken, regexp.MustCompile(`^0$|^[1-9]\d*$`)},        // Note: numbers that start with 0 are identifiers.
	{IdentifierToken, regexp.MustCompile(`^\w+$`)},                 // Any valid object name
	{StringLiteralToken, regexp.MustCompile(`^".*"$|^'.*'$`)},      // "string example"
	{BracketAccessDelimiterStartToken, regexp.MustCompile(`^\[$`)}, // the [ in map["key"]
	{BracketAccessDelimiterEndToken, regexp.MustCompile(`^\]$`)},   // the ] in map["key"]
	{ExpressionStartToken, regexp.MustCompile(`^\($`)},             // (
	{ExpressionEndToken, regexp.MustCompile(`^\)$`)},               // )
	{DotObjectAccessToken, regexp.MustCompile(`^\.$`)},             // .
	{RootAccessToken, regexp.MustCompile(`^\$$`)},                  // $
	{CurrentObjectAccessToken, regexp.MustCompile(`^@$`)},          // @
	{EqualsToken, regexp.MustCompile(`^=$`)},                       // =
	{SelectorToken, regexp.MustCompile(`^\:$`)},                    // :
	{FilterToken, regexp.MustCompile(`^\?$`)},                      // ?
	{NegationToken, regexp.MustCompile(`^\-$`)},                    // -
	{WildcardToken, regexp.MustCompile(`^\*$`)},                    // *
}

// InitTokenizer initializes the tokenizer struct with the given expression.
func InitTokenizer(expression string, sourceName string) *Tokenizer {
	var t Tokenizer
	t.reader = strings.NewReader(expression)
	t.s.Init(t.reader)
	t.s.Filename = sourceName
	return &t
}

// HasNextToken Checks to see if it has reached the end of the expression.
// If it has, it returns false. If there are tokens left, it returns true.
func (t *Tokenizer) HasNextToken() bool {
	return t.s.Peek() != scanner.EOF
}

// GetNext gets the next token type and value.
// If there is no token left, it returns an unknown token and an
// InvalidTokenError.
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
