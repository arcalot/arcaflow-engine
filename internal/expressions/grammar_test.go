package expressions_test

import (
	"testing"

	"go.arcalot.io/assert"
	"go.flow.arcalot.io/engine/internal/expressions"
)

func TestIdentifierParser(t *testing.T) {
	identifierName := "abc"

	// Create parser
	p, err := expressions.InitParser(identifierName, "test.go")

	assert.NoError(t, err)

	identifierResult, err := p.ParseIdentifier()

	assert.NoError(t, err)
	assert.Equals(t, identifierName, identifierResult.IdentifierName)

	// No tokens left, so should error out

	_, err = p.ParseIdentifier()
	assert.NotNil(t, err)
}

// Test cases that test the entire parser
func TestRootVar(t *testing.T) {
	expression := "$.test"

	// $.test
	root := &expressions.DotNotation{}
	root.LeftAccessableNode = &expressions.Identifier{IdentifierName: "$"}
	root.RightAccessIdentifier = &expressions.Identifier{IdentifierName: "test"}

	// Create parser
	p, err := expressions.InitParser(expression, "test.go")

	assert.NoError(t, err)

	parsedResult, err := p.ParseRoot()

	assert.NoError(t, err)
	assert.NotNil(t, parsedResult)

	println(expression)
	println(root.String())
	println(parsedResult.String())
	//assert.Equals(t, expression, root.String())
}

func TestDotNotation(t *testing.T) {
	expression := "$.parent.child"

	// level2: $.parent
	level2 := &expressions.DotNotation{}
	level2.LeftAccessableNode = &expressions.Identifier{IdentifierName: "$"}
	level2.RightAccessIdentifier = &expressions.Identifier{IdentifierName: "parent"}
	// root: <level2>.child
	root := expressions.DotNotation{}
	root.LeftAccessableNode = level2
	root.RightAccessIdentifier = &expressions.Identifier{IdentifierName: "child"}

	// Create parser
	p, err := expressions.InitParser(expression, "test.go")

	assert.NoError(t, err)

	parsedResult, err := p.ParseRoot()

	assert.NoError(t, err)
	assert.NotNil(t, parsedResult)

	println(expression)
	println(root.String())
	println(parsedResult.String())
	//assert.Equals(t, expression, root.String())
}

func TestMapAccess(t *testing.T) {
	expression := `$.map["key"]`

	// level2: $.map
	level2 := &expressions.DotNotation{}
	level2.LeftAccessableNode = &expressions.Identifier{IdentifierName: "$"}
	level2.RightAccessIdentifier = &expressions.Identifier{IdentifierName: "map"}
	// root: <level2>.["key"]
	root := &expressions.MapAccessor{}
	root.LeftNode = level2
	root.RightKey = &expressions.Key{ /*"key"*/ }

	// Create parser
	p, err := expressions.InitParser(expression, "test.go")

	assert.NoError(t, err)

	parsedResult, err := p.ParseRoot()

	assert.NoError(t, err)
	assert.NotNil(t, parsedResult)

	println(expression)
	println(root.String())
	println(parsedResult.String())
	//assert.Equals(t, expression, root.String())
}

func TestDeepMapAccess(t *testing.T) {
	expression := `$.a.b[0].c["k"]`

	// level5: $.a
	level5 := &expressions.DotNotation{}
	level5.LeftAccessableNode = &expressions.Identifier{IdentifierName: "$"}
	level5.RightAccessIdentifier = &expressions.Identifier{IdentifierName: "a"}
	// level4: <level5>.b
	level4 := &expressions.DotNotation{}
	level4.LeftAccessableNode = level5
	level4.RightAccessIdentifier = &expressions.Identifier{IdentifierName: "b"}
	// level3: <level4>[0]
	level3 := &expressions.MapAccessor{}
	level3.LeftNode = level4
	level3.RightKey = &expressions.Key{ /*0*/ }
	// level2: <level3>.c
	level2 := &expressions.DotNotation{}
	level2.LeftAccessableNode = level3
	level2.RightAccessIdentifier = &expressions.Identifier{IdentifierName: "c"}
	// root: <level2>["k"]
	root := &expressions.MapAccessor{}
	root.LeftNode = level2
	root.RightKey = &expressions.Key{ /*"k"*/ }

	// Create parser
	p, err := expressions.InitParser(expression, "test.go")

	assert.NoError(t, err)

	parsedResult, err := p.ParseRoot()

	assert.NoError(t, err)
	assert.NotNil(t, parsedResult)

	println(expression)
	println(root.String())
	println(parsedResult.String())
	//assert.Equals(t, expression, root.String())
}

func TestCompound(t *testing.T) {
	expression := `$.a.b.c["key"].d`

	// level5: $.a
	level5 := &expressions.DotNotation{}
	level5.LeftAccessableNode = &expressions.Identifier{IdentifierName: "$"}
	level5.RightAccessIdentifier = &expressions.Identifier{IdentifierName: "a"}
	// level4: <level5>.b
	level4 := &expressions.DotNotation{}
	level4.LeftAccessableNode = level5
	level4.RightAccessIdentifier = &expressions.Identifier{IdentifierName: "b"}
	// level3: <level4>.c
	level3 := &expressions.DotNotation{}
	level3.LeftAccessableNode = level4
	level3.RightAccessIdentifier = &expressions.Identifier{IdentifierName: "c"}
	// level2: <level3>["key"]
	level2 := &expressions.MapAccessor{}
	level2.LeftNode = level3
	level2.RightKey = &expressions.Key{ /* "key" */ }
	// root: <level2>.d
	root := &expressions.DotNotation{}
	root.LeftAccessableNode = level2
	root.RightAccessIdentifier = &expressions.Identifier{IdentifierName: "d"}

	// Create parser
	p, err := expressions.InitParser(expression, "test.go")

	assert.NoError(t, err)

	parsedResult, err := p.ParseRoot()

	assert.NoError(t, err)
	assert.NotNil(t, parsedResult)

	println(expression)
	println(root.String())
	println(parsedResult.String())
	//assert.Equals(t, expression, root.String())
}
