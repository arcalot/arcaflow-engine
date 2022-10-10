package expressions

import "fmt"

// InvalidTokenError represents an error when the tokenizer doesn't recognise
// the token pattern. This is often caused by invalid characters, or characters
// in the wrong order.
type InvalidTokenError struct {
	InvalidToken TokenValue
}

func (e *InvalidTokenError) Error() string {
	return fmt.Sprintf("Invalid token \"%s\" in %s at line %d:%d",
		e.InvalidToken.Value, e.InvalidToken.Filename, e.InvalidToken.Line, e.InvalidToken.Column)
}

// InvalidGrammarError represents when the order of tokens is not valid for
// the language.
type InvalidGrammarError struct {
	FoundToken     *TokenValue
	ExpectedTokens []TokenID // Nil for end, no expected token
}

func (e *InvalidGrammarError) Error() string {
	errorMsg := fmt.Sprintf("Token \"%s\" placed in invalid configuration in %s at line %d:%d.",
		e.FoundToken.Value, e.FoundToken.Filename, e.FoundToken.Line, e.FoundToken.Column)
	switch {
	case e.ExpectedTokens == nil || len(e.ExpectedTokens) == 0:
		errorMsg += " Expected end of expression."
	case len(e.ExpectedTokens) == 1:
		errorMsg += fmt.Sprintf(" Expected token \"%v\"", e.ExpectedTokens[0])
	default:
		errorMsg += fmt.Sprintf(" Expected one of tokens \"%v\"", e.ExpectedTokens)
	}

	return errorMsg
}
