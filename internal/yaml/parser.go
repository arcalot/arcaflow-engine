package yaml

import (
	"fmt"

	//"gopkg.in/yaml.v3"
	"github.com/goccy/go-yaml"
	"github.com/goccy/go-yaml/ast"
)

// New creates a new YAML parser.
func New() Parser {
	return &parser{}
}

// EmptyNode returns an empty node.
func EmptyNode() Node {
	return &node{
		typeID:   TypeIDString,
		tag:      "!!null",
		contents: nil,
		value:    "",
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
	//for i := 0; i < len(n.contents); i += 2 {
	//	if key == n.contents[i].Raw() {
	//		return n.contents[i+1], true
	//	}
	//}
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
	return p.transform(&n)
}

func (p parser) transform(n *ast.Node) (Node, error) {
	var mappingNode *ast.MappingNode
	//var mappingValueNode *ast.MappingValueNode
	var sequenceNode *ast.SequenceNode
	var scalarNode ast.ScalarNode
	arcaNode := node{contents: make([]Node, 0), nodeMap: make(map[string]Node)}
	switch (*n).Type() {
	case 0:
		return nil, fmt.Errorf("empty YAML file given")
	case ast.MappingType:
		arcaNode.typeID = TypeIDMap
		mappingNode = (*n).(*ast.MappingNode)
	//case ast.MappingValueType:
	//	arcaNode.typeID = TypeIDMap
	//	mappingValueNode := (*n).(*ast.MappingValueNode)
	//	return p.transform(&mappingValueNode.Value)
	case ast.SequenceType:
		arcaNode.typeID = TypeIDSequence
		sequenceNode = (*n).(*ast.SequenceNode)
		//arcaNode.contents = make([]Node, len(sequenceNode.Values))
	case ast.TagType:
		arcaNode.typeID = TypeIDString
		tagNode := (*n).(*ast.TagNode)
		arcaNode.tag = tagNode.GetToken().Value
		//arcaNode.value = tagNode.BaseNode
		arcaNode.value = tagNode.Value.(*ast.StringNode).Value
	case ast.DocumentType:
		docNode := (*n).(*ast.DocumentNode)
		return p.transform(&docNode.Body)
	default:
		var ok bool
		arcaNode.typeID = TypeIDString
		scalarNode, ok = (*n).(ast.ScalarNode)
		if !ok {
			return nil, fmt.Errorf("unsupported node type: %s", (*n).Type())
		}
	}

	if mappingNode != nil {
		mapIter := mappingNode.MapRange()
		for mapIter.Next() {
			subNodeKey := mapIter.Key().String()
			subNode := mapIter.Value()
			//valueNode := subNode.(*ast.MappingValueNode)
			mappingValueNode, ok := subNode.(*ast.MappingValueNode)
			if ok {
				subNode = mappingValueNode.Value
			}
			subContent, err := p.transform(&subNode)
			if err != nil {
				return nil, err
			}
			arcaNode.nodeMap[subNodeKey] = subContent
		}
	}
	if sequenceNode != nil {
		for _, subNode := range sequenceNode.Values {
			subContent, err := p.transform(&subNode)
			if err != nil {
				return nil, err
			}
			arcaNode.contents = append(arcaNode.contents, subContent)
		}
	}
	if scalarNode != nil {
		arcaNode.value = fmt.Sprintf("%v", scalarNode.GetValue())
	}

	return &arcaNode, nil
}
