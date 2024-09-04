package infer

import (
	"fmt"
	"go.flow.arcalot.io/pluginsdk/schema"
)

// OneOfExpression stores the discriminator, and a key-value pair of all possible oneof values.
// The keys are the value for the discriminator, and the values are the YAML inputs, which can be
// inferred within the infer class.
type OneOfExpression struct {
	Discriminator string
	Options       map[string]any
	Node          string
}

func (o *OneOfExpression) String() string {
	return fmt.Sprintf("{OneOf Expression; Discriminator: %s; Options: %v}", o.Discriminator, o.Options)
}

// Type returns the OneOf type. Calculates the types of all possible oneof options for this.
func (o *OneOfExpression) Type(
	internalDataModel schema.Scope,
	functions map[string]schema.Function,
	workflowContext map[string][]byte,
) (schema.Type, error) {
	schemas := map[string]schema.Object{}
	// Gets the type for all options.
	for optionID, data := range o.Options {
		inferredType, err := Type(data, internalDataModel, functions, workflowContext)
		if err != nil {
			return nil, err
		}
		inferredObjectType, isObject := inferredType.(schema.Object)
		if !isObject {
			return nil, fmt.Errorf("type of OneOf option is not an object; got %T", inferredType)
		}
		schemas[optionID] = inferredObjectType
	}
	return schema.NewOneOfStringSchema[any](schemas, o.Discriminator, false), nil
}
