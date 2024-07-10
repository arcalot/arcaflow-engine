package yaml

import (
	"fmt"
	"github.com/goccy/go-yaml"
	"github.com/goccy/go-yaml/ast"
	"github.com/goccy/go-yaml/token"
)

const BoolTag token.ReservedTagKeyword = "!!bool"

// New creates a new YAML parser.
func New() Parser {
	return &parser{}
}

// EmptyNode returns an empty node.
func EmptyNode() Node {
	return &node{
		typeID:   TypeIDString,
		tag:      "!!null",
		value:    "",
		contents: nil,
		nodeMap:  nil,
	}
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
	// MapKey selects a specific map key. If the node is not a map, this function panics.
	MapKey(key string) (Node, bool)
	// MapKeys lists all keys of a map. If the node is not a map, this function panics.
	MapKeys() []string
	// Value returns the value in case of a string node.
	Value() string
	// Raw outputs the node as raw data without type annotation.
	Raw() any
}

type node struct {
	typeID   TypeID
	tag      string
	contents []Node
	value    string
	nodeMap  map[string]Node
}

func (n node) MapKeys() []string {
	if n.typeID != TypeIDMap {
		panic(fmt.Errorf("node is not a map, cannot call MapKeys"))
	}
	result := make([]string, 0)
	for k := range n.nodeMap {
		result = append(result, k)
	}
	return result
}

func (n node) MapKey(key string) (Node, bool) {
	if n.typeID != TypeIDMap {
		panic(fmt.Errorf("node is not a map, cannot call MapKey"))
	}
	if value, ok := n.nodeMap[key]; ok {
		return value, true
	}
	return nil, false
}

func (n node) Raw() any {
	switch n.typeID {
	case TypeIDString:
		return n.value
	case TypeIDMap:
		result := make(map[string]any, len(n.nodeMap))
		for key, value := range n.nodeMap {
			result[key] = value.Raw()
		}
		return result
	case TypeIDSequence:
		result := make([]any, len(n.contents))
		for i, item := range n.contents {
			result[i] = item.Raw()
		}
		return result
	default:
		panic(fmt.Errorf("bug: unexpected type ID: %s", n.typeID))
	}
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
	var n ast.Node
	if err := yaml.Unmarshal(data, &n); err != nil {
		return nil, err
	}
	if n == nil {
		return node{}, fmt.Errorf("empty YAML file given")
	}
	return p.transform(&n)
}

// transform converts an instance of ast.Node to the arcaflow engine's
// yaml.node
func (p parser) transform(n *ast.Node) (node, error) {
	arcaNode := node{contents: make([]Node, 0), nodeMap: make(map[string]Node)}
	var childArcaNode node
	var err error
	switch (*n).Type() {
	case ast.TagType:
		// tag nodes do not exist in the engine's yaml ast, so they need to be
		// replaced by their child value node
		tagNode := (*n).(*ast.TagNode)
		tagExplicit := string(token.ReservedTagKeyword(tagNode.GetToken().Value))
		childArcaNode, err = p.transform(&tagNode.Value)
		if err == nil {
			childArcaNode.tag = tagExplicit
			arcaNode = childArcaNode
		}
	case ast.DocumentType:
		docNode := (*n).(*ast.DocumentNode)
		// recursively transform nodes in non-empty container nodes
		arcaNode, err = p.transform(&docNode.Body)
	case ast.MappingType:
		arcaNode.typeID = TypeIDMap
		arcaNode.tag = string(token.MappingTag)
		mappingNode := (*n).(*ast.MappingNode)
		// recursively transform nodes in non-empty container nodes
		arcaNode, err = p.fillNodeMap(mappingNode.MapRange(), arcaNode)
	case ast.MappingValueType:
		arcaNode.typeID = TypeIDMap
		arcaNode.tag = string(token.MappingTag)
		mappingValueNode := (*n).(*ast.MappingValueNode)
		// recursively transform nodes in non-empty container nodes
		arcaNode, err = p.fillNodeMap(mappingValueNode.MapRange(), arcaNode)
	case ast.SequenceType:
		arcaNode.typeID = TypeIDSequence
		arcaNode.tag = string(token.SequenceTag)
		sequenceNode := (*n).(*ast.SequenceNode)
		// recursively transform nodes in non-empty container nodes
		for _, subNode := range sequenceNode.Values {
			childArcaNode, err = p.transform(&subNode)
			if err != nil {
				return node{}, err
			}
			arcaNode.contents = append(arcaNode.contents, childArcaNode)
		}
	case ast.LiteralType:
		arcaNode.tag = string(token.StringTag)
		arcaNode.typeID = TypeIDString
		literalNode := (*n).(*ast.LiteralNode)
		scalarNode := any(literalNode.Value).(ast.ScalarNode)
		arcaNode.value = fmt.Sprintf("%v", scalarNode.GetValue())
	case ast.NullType:
		arcaNode.tag = string(token.NullTag)
		arcaNode.value = (*n).GetToken().Value
	case ast.FloatType:
		arcaNode.tag = string(token.FloatTag)
		arcaNode.typeID = TypeIDString
		// must use token to get expected decimal precision for numbers with
		// trailing zeros after decimal
		arcaNode.value = (*n).GetToken().Value
	case ast.BoolType:
		arcaNode.tag = string(BoolTag)
		arcaNode.typeID = TypeIDString
		scalarNode := (*n).(ast.ScalarNode)
		arcaNode.value = fmt.Sprintf("%v", scalarNode.GetValue())
	case ast.IntegerType:
		arcaNode.tag = string(token.IntegerTag)
		arcaNode.typeID = TypeIDString
		scalarNode := (*n).(ast.ScalarNode)
		arcaNode.value = fmt.Sprintf("%v", scalarNode.GetValue())
	case ast.StringType:
		arcaNode.tag = string(token.StringTag)
		arcaNode.typeID = TypeIDString
		scalarNode := (*n).(ast.ScalarNode)
		arcaNode.value = fmt.Sprintf("%v", scalarNode.GetValue())
	default:
		return node{}, fmt.Errorf("unsupported node type: %s", (*n).Type())
	}
	return arcaNode, nil
}

func (p parser) fillNodeMap(mapIter *ast.MapNodeIter, arcaNode node) (node, error) {
	for mapIter.Next() {
		subNodeKey := mapIter.Key().String()
		subNode := mapIter.Value()
		subContent, err := p.transform(&subNode)
		if err != nil {
			return node{}, err
		}
		arcaNode.nodeMap[subNodeKey] = subContent
	}
	return arcaNode, nil
}
