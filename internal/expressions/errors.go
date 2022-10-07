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
	FoundToken     *TokenValue
	ExpectedTokens []TokenID // Nil for end, no expected token
}

func (e *InvalidGrammarError) Error() string {
	errorMsg := fmt.Sprintf("Token \"%s\" placed in invalid configuration in %s at line %d:%d.",
		e.FoundToken.Value, e.FoundToken.Filename, e.FoundToken.Line, e.FoundToken.Column)
	if e.ExpectedTokens == nil || len(e.ExpectedTokens) == 0 {
		errorMsg += " Expected end of expression."
	} else if len(e.ExpectedTokens) == 1 {
		errorMsg += fmt.Sprintf(" Expected token \"%v\"", e.ExpectedTokens[0])
	} else {
		errorMsg += fmt.Sprintf(" Expected one of tokens \"%v\"", e.ExpectedTokens)
	}

	return errorMsg
}
