package expressions

import (
	"regexp"
	"strings"
	"text/scanner"
)

type TokenID string

const (
	IdentifierToken   TokenID = "identifier"          // Any valid object name
	StringLiteral     TokenID = "string"              // "string example"
	MapDelimiterStart TokenID = "map-delimiter-start" // the [ in map["key"]
	MapDelimiterEnd   TokenID = "map-delimiter-end"   // the ] in map["key"]
	ExpressionStart   TokenID = "expression-start"
	ExpressionEnd     TokenID = "expression-end"
	ObjectAccess      TokenID = "object-access" // The . in a.b (dot notation)
	RootAccess        TokenID = "root-access"
	CurrentNodeAccess TokenID = "current-node-access"
	Equals            TokenID = "equals-sign"
	Selector          TokenID = "selector"
	Filter            TokenID = "filter"
	Negation          TokenID = "negation-sign"
	UnknownToken      TokenID = "error"
)

type TokenValue struct {
	Value    string
	Token_id TokenID
}

// Used for reading tokens of an expression.
type Tokenizer struct {
	s      scanner.Scanner
	reader *strings.Reader
}

var tokenPatterns = map[TokenID]*regexp.Regexp{
	IdentifierToken:   regexp.MustCompile(`\w+`),       // Any valid object name
	StringLiteral:     regexp.MustCompile(`".*"|'.*'`), // "string example"
	MapDelimiterStart: regexp.MustCompile(`\[`),        // the [ in map["key"]
	MapDelimiterEnd:   regexp.MustCompile(`\]`),        // the ] in map["key"]
	ExpressionStart:   regexp.MustCompile(`\(`),
	ExpressionEnd:     regexp.MustCompile(`\)`),
	ObjectAccess:      regexp.MustCompile(`\.`),
	RootAccess:        regexp.MustCompile(`\$`),
	CurrentNodeAccess: regexp.MustCompile(`@`),
	Equals:            regexp.MustCompile(`=`),
	Selector:          regexp.MustCompile(`\:`),
	Filter:            regexp.MustCompile(`\?`),
	Negation:          regexp.MustCompile(`\-`),
}

func (t *Tokenizer) Init(expression string, source_name string) {
	t.reader = strings.NewReader(expression)
	t.s.Init(t.reader)
	t.s.Filename = source_name
}

// Checks to see if it has reached the end of the expression.
func (t *Tokenizer) HasNextToken() bool {
	return t.s.Peek() != scanner.EOF
}

// Gets the next token type and value.
func (t *Tokenizer) GetNext() (*TokenValue, error) {
	t.s.Scan()
	token_value := t.s.TokenText()
	for token, regex := range tokenPatterns {
		if regex.MatchString(token_value) {
			return &TokenValue{token_value, token}, nil
		}
	}
	return &TokenValue{token_value, UnknownToken},
		&InvalidTokenError{token_value, t.s.Filename, t.s.Line, t.s.Column}
}
