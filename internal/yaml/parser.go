package yaml

import (
	"fmt"

	"gopkg.in/yaml.v3"
)

// New creates a new YAML parser.
func New() Parser {
	return &parser{}
}

// Parser is a YAML parser that parses into a simplified value structure.
type Parser interface {
	// Parse parses the provided value into the simplified node representation.
	Parse(data []byte) (Node, error)
}

// TypeID represents the value structure in accordance with the YAML specification 10.1.1.
// See https://yaml.org/spec/1.2.2/#101-failsafe-schema for details.
type TypeID string

const (
	// TypeIDMap is a generic map in accordance with the YAML specification 10.1.1.1.
	TypeIDMap TypeID = "map"
	// TypeIDSequence is a generic sequence in accordance with YAML specification 10.1.1.2.
	TypeIDSequence TypeID = "seq"
	// TypeIDString is a generic string in accordance with YAML specification 10.1.1.3.
	TypeIDString TypeID = "str"
)

// Node is a simplified representation of a YAML node.
type Node interface {
	Type() TypeID
	// Tag returns a YAML tag if any.
	Tag() string
	// Contents returns the contents as further Node items. For maps, this will contain exactly two nodes, while
	// for sequences this will contain as many nodes as there are items. For strings, this will contain no items.
	Contents() []Node
	// Value returns the value in case of a string node.
	Value() string
}

type node struct {
	typeID   TypeID
	tag      string
	contents []Node
	value    string
}

func (n node) Contents() []Node {
	return n.contents
}

func (n node) Type() TypeID {
	return n.typeID
}

func (n node) Tag() string {
	return n.tag
}

func (n node) Value() string {
	return n.value
}

type parser struct {
}

func (p parser) Parse(data []byte) (Node, error) {
	var n yaml.Node
	if err := yaml.Unmarshal(data, &n); err != nil {
		return nil, err
	}
	return p.transform(&n)
}

func (p parser) transform(n *yaml.Node) (Node, error) {
	var t TypeID
	switch n.Kind {
	case yaml.MappingNode:
		t = TypeIDMap
	case yaml.SequenceNode:
		t = TypeIDSequence
	case yaml.ScalarNode:
		t = TypeIDString
	case yaml.DocumentNode:
		return p.transform(n.Content[0])
	default:
		return nil, fmt.Errorf("unsupported node type: %d", n.Kind)
	}

	contents := make([]Node, len(n.Content))
	for i, subNode := range n.Content {
		subContent, err := p.transform(subNode)
		if err != nil {
			return nil, err
		}
		contents[i] = subContent
	}

	return &node{
		t,
		n.Tag,
		contents,
		n.Value,
	}, nil

}
