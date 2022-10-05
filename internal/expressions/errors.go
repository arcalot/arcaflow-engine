package expressions

import "fmt"

type InvalidTokenError struct {
	InvalidToken TokenValue
}

func (e *InvalidTokenError) Error() string {
	return fmt.Sprintf("Invalid token \"%s\" in %s at line %d:%d",
		e.InvalidToken.Value, e.InvalidToken.Filename, e.InvalidToken.Line, e.InvalidToken.Column)
}

type InvalidGrammarError struct {
	FoundToken    *TokenValue
	ExpectedToken TokenID
}

func (e *InvalidGrammarError) Error() string {
	return fmt.Sprintf("Token \"%s\" placed in invalid configuration in %s at line %d:%d. Expected \"%s\"",
		e.FoundToken.Value, e.FoundToken.Filename, e.FoundToken.Line, e.FoundToken.Column, e.ExpectedToken)
}
