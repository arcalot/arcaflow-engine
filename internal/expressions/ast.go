// Package expressions is designed to tokenizer and
// parse jsonpath expressions.
// The AST module contains representations of the components
// of the abstract tree representation of use of the grammar.
package expressions

import "strconv"

const (
	invalid = "INVALID/MISSING"
)

// ASTNode represents any node in the abstract syntax tree.
// Left() and Right() can return nil for any node types that do not
// have left and right sides.
type ASTNode interface {
	Left() ASTNode
	Right() ASTNode
	String() string
}

// ASTValueLiteral represents any kind of literals that can be represented
// by the abstract systax tree. Examples: ints, strings.
type ASTValueLiteral interface {
	Value() interface{}
	String() string
}

// ASTStringLiteral represents a string literal value in the abstract syntax
// tree.
type ASTStringLiteral struct {
	StrValue string
}

// String returns the string surrounded by double quotes.
func (l *ASTStringLiteral) String() string {
	return `"` + l.StrValue + `"`
}

// Value returns the string contained. It can be cast to a string.
func (l *ASTStringLiteral) Value() interface{} {
	return l.StrValue
}

// ASTIntLiteral represents an integer literal value in the abstract syntax
// tree.
type ASTIntLiteral struct {
	IntValue int
}

// String returns a string representation of the integer contained.
func (l *ASTIntLiteral) String() string {
	return strconv.Itoa(l.IntValue)
}

// Value returns the integer contained.
func (l *ASTIntLiteral) Value() interface{} {
	return l.IntValue
}

// Key represents any of the valid values that can be stored inside
// of a map/object bracket access. It can be either a sub-expression,
// represented as an ASTNode, or as any supported literal, represented
// as an ASTValueLiteral. The one that is not being represented
// will be nil.
type Key struct {
	// A key can be either a literal or
	// a sub-expression that can be evaluated
	SubExpression ASTNode
	Literal       ASTValueLiteral
}

// Right returns nil, because a key does not branch left and right.
func (k *Key) Right() ASTNode {
	return nil
}

// Left returns nil, because a key does not branch left and right.
func (k *Key) Left() ASTNode {
	return nil
}

// String returns the string from either the literal, or its sub-expression,
// surrounded by '(' and ')'.
func (k *Key) String() string {
	switch {
	case k.Literal != nil:
		return k.Literal.String()
	case k.SubExpression != nil:
		return "(" + k.SubExpression.String() + ")"
	default:
		return invalid
	}
}

// MapAccessor represents a part of the abstract syntax tree that is accessing
// the value at a key in an object.
type MapAccessor struct {
	LeftNode ASTNode
	RightKey *Key
}

// Right returns the key.
func (m *MapAccessor) Right() ASTNode {
	return m.RightKey
}

// Left returns the node being accessed.
func (m *MapAccessor) Left() ASTNode {
	return m.LeftNode
}

// String returns the string from the accessed node, followed by '[', followed
// by the string from the key, followed by ']'.
func (m *MapAccessor) String() string {
	return m.LeftNode.String() + "[" + m.RightKey.String() + "]"
}

// Identifier represents a valid identifier in the abstract syntax tree.
type Identifier struct {
	IdentifierName string
}

// Right returns nil, because an identifier does not branch left and right.
func (i *Identifier) Right() ASTNode {
	return nil
}

// Left returns nil, because an identifier does not branch left and right.
func (i *Identifier) Left() ASTNode {
	return nil
}

// String returns the identifier name.
func (i *Identifier) String() string {
	return i.IdentifierName
}

// DotNotation represents the access of an identifier in a node.
type DotNotation struct {
	// The identifier on the right of the dot
	RightAccessIdentifier ASTNode
	// The expression on the left could be one of several nodes.
	// I.e. An Identifier, a MapAccessor, or another DotNotation
	LeftAccessableNode ASTNode
}

// Right returns the identifier being accessed in the left node.
func (d *DotNotation) Right() ASTNode {
	return d.RightAccessIdentifier
}

// Left returns the left node being accessed.
func (d *DotNotation) Left() ASTNode {
	return d.LeftAccessableNode
}

// String returns the string representing the left node, followed by '.',
// followed by the string representing the right identiier.
func (d *DotNotation) String() string {
	if d == nil {
		return invalid
	}
	var left, right string
	if d.LeftAccessableNode != nil {
		left = d.LeftAccessableNode.String()
	} else {
		left = invalid
	}
	if d.RightAccessIdentifier != nil {
		right = d.RightAccessIdentifier.String()
	} else {
		right = invalid
	}
	return left + "." + right
}
