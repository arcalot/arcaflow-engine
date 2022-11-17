package expand

import (
	"fmt"
	"reflect"
	"strconv"

	"go.flow.arcalot.io/expressions"
)

// Datastructure traverses an entire YAML data structure and processes any expression nodes with the baseData passed.
func Datastructure(dataStructure any, baseData map[string]any) (any, error) {
	switch d := dataStructure.(type) {
	case expressions.ASTNode:
		return evaluate(d, baseData, baseData)
	case string:
		return d, nil
	case []any:
		result := make([]any, len(d))
		for i, n := range d {
			var err error
			result[i], err = Datastructure(n, baseData)
			if err != nil {
				return nil, err
			}
		}
		return result, nil
	case map[any]any:
		result := make(map[any]any, len(d))
		for k, v := range d {
			var err error
			result[k], err = Datastructure(v, baseData)
			if err != nil {
				return nil, err
			}
		}
		return result, nil
	default:
		return nil, fmt.Errorf("unsupported node type: %T", dataStructure)
	}
}

func evaluate(node expressions.ASTNode, data any, rootData any) (any, error) {
	switch n := node.(type) {
	case *expressions.DotNotation:
		leftResult, err := evaluate(n.LeftAccessableNode, data, rootData)
		if err != nil {
			return nil, err
		}
		return evaluate(n.RightAccessIdentifier, leftResult, rootData)
	case *expressions.MapAccessor:
		leftResult, err := evaluate(n.LeftNode, data, rootData)
		if err != nil {
			return nil, err
		}
		mapKey, err := evaluate(n.RightKey, leftResult, rootData)
		if err != nil {
			return nil, err
		}
		return evaluateMapKey(data, mapKey)
	case *expressions.Key:
		switch {
		case n.Literal != nil:
			return n.Literal.Value(), nil
		case n.SubExpression != nil:
			return evaluate(n.SubExpression, data, rootData)
		default:
			return nil, fmt.Errorf("bug: neither literal, nor subexpression are set on key")
		}
	case *expressions.Identifier:
		switch n.IdentifierName {
		case "$":
			return rootData, nil
		default:
			return evaluateMapKey(data, n.IdentifierName)
		}
	default:
		return nil, fmt.Errorf("unsupported AST node type: %T", n)
	}
}

func evaluateMapKey(data any, mapKey any) (any, error) {
	dataVal := reflect.ValueOf(data)
	switch dataVal.Kind() {
	case reflect.Map:
		indexValue := dataVal.MapIndex(reflect.ValueOf(mapKey))
		if !indexValue.IsValid() {
			return nil, fmt.Errorf("map key %v not found", mapKey)
		}
		return indexValue.Interface(), nil
	case reflect.Slice:
		var sliceIndex int
		switch t := mapKey.(type) {
		case string:
			var err error
			i, err := strconv.ParseInt(t, 10, 64)
			if err != nil {
				return nil, fmt.Errorf("cannot parse %v as an integer index for a list", mapKey)
			}
			sliceIndex = int(i)
		case int:
			sliceIndex = t
		default:
			return nil, fmt.Errorf("unsupported map key type: %T", mapKey)
		}
		sliceLen := dataVal.Len()
		if sliceLen <= sliceIndex {
			return nil, fmt.Errorf("index %d is larger than the list items (%d)", sliceIndex, sliceLen)
		}
		indexValue := dataVal.Index(sliceIndex)
		return indexValue.Interface(), nil
	default:
		return nil, fmt.Errorf(
			"cannot evaluate identifier %v on a %s",
			mapKey,
			dataVal.Kind().String(),
		)
	}
}
