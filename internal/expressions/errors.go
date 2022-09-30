package expressions

import "fmt"

type InvalidTokenError struct {
	Token    string
	Filename string
	Line     int
	Column   int
}

func (e *InvalidTokenError) Error() string {
	return fmt.Sprintf("Invalid token \"%s\" in %s at line %d:%d", e.Token,
		e.Filename, e.Line, e.Column)
}
