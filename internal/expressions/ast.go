package expressions

type ASTNode interface {
	Left() ASTNode
	Right() ASTNode
	String() string
}

type Key struct {
	// A key can be either a literal or
	// a sub-expression that can be evaluated
}

func (k *Key) Right() ASTNode {
	return nil
}

func (k *Key) Left() ASTNode {
	return nil
}

func (k *Key) String() string {
	return "TODO"
}

type MapAccessor struct {
	LeftNode ASTNode
	RightKey *Key
}

func (m *MapAccessor) Right() ASTNode {
	return m.RightKey
}

func (m *MapAccessor) Left() ASTNode {
	return m.LeftNode
}

func (m *MapAccessor) String() string {
	return m.LeftNode.String() + "[" + m.RightKey.String() + "]"
}

type Identifier struct {
	IdentifierName string
}

func (i *Identifier) Right() ASTNode {
	return nil
}

func (i *Identifier) Left() ASTNode {
	return nil
}

func (i *Identifier) String() string {
	return i.IdentifierName
}

type DotNotation struct {
	// The identifier on the right of the dot
	RightAccessIdentifier ASTNode
	// The expression on the left could be one of several nodes.
	// I.e. An Identifier, a MapAccessor, or another DotNotation
	LeftAccessableNode ASTNode
}

func (d *DotNotation) Right() ASTNode {
	return d.RightAccessIdentifier
}

func (d *DotNotation) Left() ASTNode {
	return d.LeftAccessableNode
}

func (d *DotNotation) String() string {
	if d == nil {
		return "NIL"
	}
	var left, right string
	if d.LeftAccessableNode != nil {
		left = d.LeftAccessableNode.String()
	} else {
		left = "NIL"
	}
	if d.RightAccessIdentifier != nil {
		right = d.RightAccessIdentifier.String()
	} else {
		right = "NIL"
	}
	return left + "." + right
}
