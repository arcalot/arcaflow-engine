package yaml

import (
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"

	"gopkg.in/yaml.v3"
)

// Marshal serializes a value to YAML, preserving float64 type fidelity.
// Unlike yaml.v3's default Marshal, whole-number floats like 99.0 are
// rendered as "99.0" (not "99"), so consumers parsing the YAML output
// will correctly interpret them as floats rather than integers.
func Marshal(v any) ([]byte, error) {
	node, err := toYAMLNode(v)
	if err != nil {
		return nil, err
	}
	doc := &yaml.Node{
		Kind:    yaml.DocumentNode,
		Content: []*yaml.Node{node},
	}
	return yaml.Marshal(doc)
}

// toYAMLNode recursively converts a Go value into a yaml.Node tree
// with explicit tags, ensuring type-faithful YAML output.
func toYAMLNode(v any) (*yaml.Node, error) { //nolint:gocognit,cyclop
	if v == nil {
		return &yaml.Node{Kind: yaml.ScalarNode, Tag: "!!null", Value: "null"}, nil
	}
	switch val := v.(type) {
	case bool:
		return &yaml.Node{
			Kind:  yaml.ScalarNode,
			Tag:   "!!bool",
			Value: strconv.FormatBool(val),
		}, nil
	case string:
		return &yaml.Node{
			Kind:  yaml.ScalarNode,
			Tag:   "!!str",
			Value: val,
		}, nil
	case int:
		return &yaml.Node{
			Kind:  yaml.ScalarNode,
			Tag:   "!!int",
			Value: strconv.FormatInt(int64(val), 10),
		}, nil
	case int64:
		return &yaml.Node{
			Kind:  yaml.ScalarNode,
			Tag:   "!!int",
			Value: strconv.FormatInt(val, 10),
		}, nil
	case uint64:
		return &yaml.Node{
			Kind:  yaml.ScalarNode,
			Tag:   "!!int",
			Value: strconv.FormatUint(val, 10),
		}, nil
	case float32:
		return floatNode(float64(val)), nil
	case float64:
		return floatNode(val), nil
	case []any:
		return sliceNode(val)
	case map[string]any:
		return stringMapNode(val)
	case map[any]any:
		return anyMapNode(val)
	default:
		return nil, fmt.Errorf("unsupported type for YAML marshaling: %T", v)
	}
}

func floatNode(val float64) *yaml.Node {
	var s string
	switch {
	case math.IsNaN(val):
		s = ".nan"
	case math.IsInf(val, 1):
		s = ".inf"
	case math.IsInf(val, -1):
		s = "-.inf"
	default:
		s = strconv.FormatFloat(val, 'f', -1, 64)
		if !strings.Contains(s, ".") {
			s += ".0"
		}
	}
	return &yaml.Node{Kind: yaml.ScalarNode, Tag: "!!float", Value: s}
}

func sliceNode(val []any) (*yaml.Node, error) {
	node := &yaml.Node{Kind: yaml.SequenceNode, Tag: "!!seq"}
	for _, item := range val {
		child, err := toYAMLNode(item)
		if err != nil {
			return nil, err
		}
		node.Content = append(node.Content, child)
	}
	return node, nil
}

func stringMapNode(val map[string]any) (*yaml.Node, error) {
	node := &yaml.Node{Kind: yaml.MappingNode, Tag: "!!map"}
	keys := make([]string, 0, len(val))
	for k := range val {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		keyNode := &yaml.Node{Kind: yaml.ScalarNode, Tag: "!!str", Value: k}
		valNode, err := toYAMLNode(val[k])
		if err != nil {
			return nil, err
		}
		node.Content = append(node.Content, keyNode, valNode)
	}
	return node, nil
}

func anyMapNode(val map[any]any) (*yaml.Node, error) {
	node := &yaml.Node{Kind: yaml.MappingNode, Tag: "!!map"}
	type kv struct {
		sortKey string
		key     any
		value   any
	}
	pairs := make([]kv, 0, len(val))
	for k, v := range val {
		pairs = append(pairs, kv{sortKey: fmt.Sprint(k), key: k, value: v})
	}
	sort.Slice(pairs, func(i, j int) bool { return pairs[i].sortKey < pairs[j].sortKey })
	for _, p := range pairs {
		keyNode, err := toYAMLNode(p.key)
		if err != nil {
			return nil, err
		}
		valNode, err := toYAMLNode(p.value)
		if err != nil {
			return nil, err
		}
		node.Content = append(node.Content, keyNode, valNode)
	}
	return node, nil
}
