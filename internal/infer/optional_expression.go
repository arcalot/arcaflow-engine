package infer

import (
	"go.flow.arcalot.io/expressions"
)

// OptionalExpression is an expression that can be used in an object as an optional field.
type OptionalExpression struct {
	Expr              expressions.Expression
	WaitForCompletion bool
	GroupNodePath     string
	ParentNodePath    string
}
