// Package builtinfunctions provides functions available to expressions in workflows.
package builtinfunctions

import (
	"fmt"
	"go.flow.arcalot.io/pluginsdk/schema"
	"math"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
)

// GetFunctions returns a map of all functions currently available.
func GetFunctions() map[string]schema.CallableFunction {
	// Simple conversions
	intToFloatFunction := getIntToFloatFunction()
	floatToIntFunction := getFloatToIntFunction()
	intToStringFunction := getIntToStringFunction()
	floatToStringFunction := getFloatToStringFunction()
	floatToFormattedStringFunction := getFloatToFormattedStringFunction()
	booleanToStringFunction := getBooleanToStringFunction()
	// Parsers, that could fail
	stringToIntFunction := getStringToIntFunction()
	stringToFloatFunction := getStringToFloatFunction()
	stringToBoolFunction := getStringToBoolFunction()
	// Math helper functions
	ceilFunction := getCeilFunction()
	floorFunction := getFloorFunction()
	roundFunction := getRoundFunction()
	absFunction := getAbsFunction()
	// String helper functions
	toLowerFunction := getToLowerFunction()
	toUpperFunction := getToUpperFunction()
	splitStringFunction := getSplitStringFunction()
	loadFileFunction := getReadFileFunction()
	// Data transformation functions
	bindConstantsFunction := getBindConstantsFunction()

	// Combine in a map
	allFunctions := map[string]schema.CallableFunction{
		intToFloatFunction.ID():             intToFloatFunction,
		floatToIntFunction.ID():             floatToIntFunction,
		intToStringFunction.ID():            intToStringFunction,
		floatToStringFunction.ID():          floatToStringFunction,
		floatToFormattedStringFunction.ID(): floatToFormattedStringFunction,
		booleanToStringFunction.ID():        booleanToStringFunction,
		stringToIntFunction.ID():            stringToIntFunction,
		stringToFloatFunction.ID():          stringToFloatFunction,
		stringToBoolFunction.ID():           stringToBoolFunction,
		ceilFunction.ID():                   ceilFunction,
		floorFunction.ID():                  floorFunction,
		roundFunction.ID():                  roundFunction,
		absFunction.ID():                    absFunction,
		toLowerFunction.ID():                toLowerFunction,
		toUpperFunction.ID():                toUpperFunction,
		splitStringFunction.ID():            splitStringFunction,
		loadFileFunction.ID():               loadFileFunction,
		bindConstantsFunction.ID():          bindConstantsFunction,
	}

	return allFunctions
}

func getIntToFloatFunction() schema.CallableFunction {
	funcSchema, err := schema.NewCallableFunction(
		"intToFloat",
		[]schema.Type{schema.NewIntSchema(nil, nil, nil)},
		schema.NewFloatSchema(nil, nil, nil),
		false,
		schema.NewDisplayValue(
			schema.PointerTo("intToFloat"),
			schema.PointerTo("Returns a floating-point representation of the integer parameter."),
			nil,
		),
		func(a int64) float64 {
			return float64(a)
		},
	)
	if err != nil {
		panic(err)
	}
	return funcSchema
}

func getFloatToIntFunction() schema.CallableFunction {
	funcSchema, err := schema.NewCallableFunction(
		"floatToInt",
		[]schema.Type{schema.NewFloatSchema(nil, nil, nil)},
		schema.NewIntSchema(nil, nil, nil),
		true,
		schema.NewDisplayValue(
			schema.PointerTo("floatToInt"),
			schema.PointerTo(
				"Returns an integer representation of the floating-point parameter.\n"+
					" It does this by discarding the the fraction.\n"+
					" In other words, it is rounded to the nearest integer towards zero.\n"+
					"Special cases:\n"+
					" +Inf outputs the maximum signed 64-bit integer (9223372036854775807)\n"+
					" -Inf outputs the minimum signed 64-bit integer (-9223372036854775808)\n"+
					" NaN outputs an error\n\n"+
					"For example, `5.5` becomes `5`, and `-1.9` becomes `-1`",
			),
			nil,
		),
		func(a float64) (int64, error) {
			// Because the type conversion in Go has platform-specific behavior, handle
			// the special cases explicitly so that we get consistent, portable results.
			switch {
			case math.IsInf(a, 1):
				return math.MaxInt64, nil
			case math.IsInf(a, -1):
				return math.MinInt64, nil
			case math.IsNaN(a):
				return math.MinInt64, fmt.Errorf("attempted to convert a NaN float to an integer")
			}
			return int64(a), nil
		},
	)
	if err != nil {
		panic(err)
	}
	return funcSchema
}

func getIntToStringFunction() schema.CallableFunction {
	funcSchema, err := schema.NewCallableFunction(
		"intToString",
		[]schema.Type{schema.NewIntSchema(nil, nil, nil)},
		schema.NewStringSchema(nil, nil, nil),
		false,
		schema.NewDisplayValue(
			schema.PointerTo("intToString"),
			schema.PointerTo(
				"Returns a string containing the base-10 representation of the provided integer value.\n"+
					"For example, an input of `55` will output `\"55\"`",
			),
			nil,
		),
		func(a int64) string {
			return strconv.FormatInt(a, 10)
		},
	)
	if err != nil {
		panic(err)
	}
	return funcSchema
}

func getFloatToStringFunction() schema.CallableFunction {
	funcSchema, err := schema.NewCallableFunction(
		"floatToString",
		[]schema.Type{schema.NewFloatSchema(nil, nil, nil)},
		schema.NewStringSchema(nil, nil, regexp.MustCompile(`^\d+\.\d*$`)),
		false,
		schema.NewDisplayValue(
			schema.PointerTo("floatToString"),
			schema.PointerTo(
				"Returns a string containing the base-10 representation of the provided "+
					"floating point value formatted without an exponent.\n"+
					"For example, an input of `5000.5` will output `\"5000.5\"`",
			),
			nil,
		),
		func(a float64) string {
			return strconv.FormatFloat(a, 'f', -1, 64)
		},
	)
	if err != nil {
		panic(err)
	}
	return funcSchema
}

func getFloatToFormattedStringFunction() schema.CallableFunction {
	funcSchema, err := schema.NewCallableFunction(
		"floatToFormattedString",
		[]schema.Type{
			schema.NewFloatSchema(nil, nil, nil),
			schema.NewStringSchema(nil, nil, regexp.MustCompile(`^[beEfgGxX]$`)),
			schema.NewIntSchema(nil, nil, nil),
		},
		// 'b' format: -ddddp±ddd
		// 'e' format: -d.dddde±dd
		// 'E' format: -d.ddddE±dd
		// 'f' format: -ddd.dddd
		// 'x' format: -0xd.ddddp±ddd
		// 'X' format: -0Xd.ddddP±ddd
		schema.NewStringSchema(
			nil,
			nil,
			regexp.MustCompile(`^-?(?:0[xX])?\d+(?:\.\d*)?(?:[pPeE][-+]\d{2,3})?$`)),
		false,
		schema.NewDisplayValue(
			schema.PointerTo("floatToFormattedString"),
			schema.PointerTo(
				"Converts a floating point number to a string according to the "+
					"specified formatting directive and precision."+
					" Param 1: the floating point value to convert\n"+
					" Param 2: the format specifier: 'b', 'e', 'E', 'f', 'g', 'G', 'x', 'X'\n"+
					" Param 3: the number of digits included in the fraction portion; "+
					"Specifying -1 will produce the minimum number of digits required to represent the value exactly"+
					" (See https://pkg.go.dev/strconv@go1.22.0#FormatFloat for details.)",
			),
			nil,
		),
		func(f float64, fmt string, precision int64) string {
			return strconv.FormatFloat(f, fmt[0], int(precision), 64)
		},
	)
	if err != nil {
		panic(err)
	}
	return funcSchema
}

func getBooleanToStringFunction() schema.CallableFunction {
	funcSchema, err := schema.NewCallableFunction(
		"boolToString",
		[]schema.Type{schema.NewBoolSchema()},
		schema.NewStringSchema(nil, nil, regexp.MustCompile(`^true|false$`)),
		false,
		schema.NewDisplayValue(
			schema.PointerTo("boolToString"),
			schema.PointerTo(
				"Returns `\"true\"` for `true`, and `\"false\"` for `false`.",
			),
			nil,
		),
		strconv.FormatBool, // Wrap go standard lib function.
	)
	if err != nil {
		panic(err)
	}
	return funcSchema
}

func getStringToIntFunction() schema.CallableFunction {
	funcSchema, err := schema.NewCallableFunction(
		"stringToInt",
		[]schema.Type{schema.NewStringSchema(nil, nil, regexp.MustCompile(`^-?\d+$`))},
		schema.NewIntSchema(nil, nil, nil),
		true,
		schema.NewDisplayValue(
			schema.PointerTo("stringToInt"),
			schema.PointerTo(
				"Returns an integer by interpreting the string parameter as\n"+
					"a base-10 integer. Will fail if the input is not a valid integer.",
			),
			nil,
		),
		func(s string) (int64, error) {
			return strconv.ParseInt(s, 10, 0)
		},
	)
	if err != nil {
		panic(err)
	}
	return funcSchema
}

func getStringToFloatFunction() schema.CallableFunction {
	funcSchema, err := schema.NewCallableFunction(
		"stringToFloat",
		[]schema.Type{schema.NewStringSchema(nil, nil, nil)},
		schema.NewFloatSchema(nil, nil, nil),
		true,
		schema.NewDisplayValue(
			schema.PointerTo("stringToFloat"),
			schema.PointerTo(
				"Returns a floating point number by interpreting the string\n"+
					"parameter as a 64-bit floating-point number.\n\n"+
					"Accepts decimal and hexadecimal floating-point numbers\n"+
					"as defined by the Go syntax for floating point literals\n"+
					"https://go.dev/ref/spec#Floating-point_literals.\n"+
					"If the input is well-formed and near a valid floating-point number,\n"+
					"stringToFloat returns the nearest floating-point number rounded\n"+
					"using IEEE754 unbiased rounding.\n\n"+
					"Returns an error when an invalid input is received.",
			),
			nil,
		),
		func(s string) (float64, error) {
			return strconv.ParseFloat(s, 64)
		},
	)
	if err != nil {
		panic(err)
	}
	return funcSchema
}

func getStringToBoolFunction() schema.CallableFunction {
	funcSchema, err := schema.NewCallableFunction(
		"stringToBool",
		[]schema.Type{
			schema.NewStringSchema(
				nil,
				nil,
				regexp.MustCompile(`(?i)^(?:true|false|[tf01])$`),
			)},
		schema.NewBoolSchema(),
		true,
		schema.NewDisplayValue(
			schema.PointerTo("stringToBool"),
			schema.PointerTo(
				"Returns a boolean by interpreting the string parameter.\n"+
					" Accepts `\"1\"`, `\"t\"`, and `\"true\"` for `true`.\n"+
					" Accepts `\"0\"`, '\"f\"', and '\"false\"' for `false`.\n"+
					"Returns an error for any other input.\n"+
					"Inputs are case insensitive. So `\"True\"` is interpreted as `\"true\"`, for example.",
			),
			nil,
		),
		func(s string) (bool, error) {
			return strconv.ParseBool(strings.ToLower(s))
		},
	)
	if err != nil {
		panic(err)
	}
	return funcSchema
}

func getCeilFunction() schema.CallableFunction {
	funcSchema, err := schema.NewCallableFunction(
		"ceil",
		[]schema.Type{schema.NewFloatSchema(nil, nil, nil)},
		schema.NewFloatSchema(nil, nil, nil),
		false,
		schema.NewDisplayValue(
			schema.PointerTo("ceil"),
			schema.PointerTo(
				// Description based on documentation for math.Ceil
				"Returns the least integer value greater than or equal to the input.\n"+
					"For example `ceil(1.5)` outputs `2.0`, and `ceil(-1.5)` outputs `-1.0`"+
					"Special cases are:\n"+ //nolint:goconst
					" ceil(±0) = ±0.0"+
					" ceil(±Inf) = ±Inf\n"+
					" ceil(NaN) = NaN",
			),
			nil,
		),
		math.Ceil, // Wrap go standard lib function.
	)
	if err != nil {
		panic(err)
	}
	return funcSchema
}

func getFloorFunction() schema.CallableFunction {
	funcSchema, err := schema.NewCallableFunction(
		"floor",
		[]schema.Type{schema.NewFloatSchema(nil, nil, nil)},
		schema.NewFloatSchema(nil, nil, nil),
		false,
		schema.NewDisplayValue(
			schema.PointerTo("floor"),
			schema.PointerTo(
				// Description based on documentation for math.Floor
				"Returns the greatest integer value less than or equal to the input.\n"+
					"For example `floor(1.5)` outputs `1.0`, and `floor(-1.5)` outputs `-2.0`"+
					"Special cases are:\n"+
					" floor(±0) = ±0.0\n"+
					" floor(±Inf) = ±Inf\n"+
					" floor(NaN) = NaN",
			),
			nil,
		),
		math.Floor, // Wrap go standard lib function.
	)
	if err != nil {
		panic(err)
	}
	return funcSchema
}

func getRoundFunction() schema.CallableFunction {
	funcSchema, err := schema.NewCallableFunction(
		"round",
		[]schema.Type{schema.NewFloatSchema(nil, nil, nil)},
		schema.NewFloatSchema(nil, nil, nil),
		false,
		schema.NewDisplayValue(
			schema.PointerTo("round"),
			schema.PointerTo(
				// Description based on documentation for math.Round
				"Returns the nearest integer to the input, rounding one-half value away from zero.\n"+
					"For example `round(1.5)` outputs `2.0`, and `round(-1.5)` outputs `-2.0`"+
					"Special cases are:\n"+
					" round(±0) = ±0.0\n"+
					" round(±Inf) = ±Inf\n"+
					" round(NaN) = NaN",
			),
			nil,
		),
		math.Round, // Wrap go standard lib function.
	)
	if err != nil {
		panic(err)
	}
	return funcSchema
}

func getAbsFunction() schema.CallableFunction {
	funcSchema, err := schema.NewCallableFunction(
		"abs",
		[]schema.Type{schema.NewFloatSchema(nil, nil, nil)},
		schema.NewFloatSchema(nil, nil, nil),
		false,
		schema.NewDisplayValue(
			schema.PointerTo("abs"),
			schema.PointerTo(
				// Description based on documentation for math.Abs
				"Returns the absolute value of x.\n"+
					"Special cases are:\n"+
					" abs(±Inf) = +Inf\n"+
					" abs(NaN) = NaN",
			),
			nil,
		),
		math.Abs, // Wrap go standard lib function.
	)
	if err != nil {
		panic(err)
	}
	return funcSchema
}

func getToLowerFunction() schema.CallableFunction {
	funcSchema, err := schema.NewCallableFunction(
		"toLower",
		[]schema.Type{schema.NewStringSchema(nil, nil, nil)},
		schema.NewStringSchema(nil, nil, nil),
		false,
		schema.NewDisplayValue(
			schema.PointerTo("toLower"),
			schema.PointerTo(
				"Returns a version of the input with Unicode letters mapped to their lower case form.",
			),
			nil,
		),
		strings.ToLower, // Wrap go standard lib function.
	)
	if err != nil {
		panic(err)
	}
	return funcSchema
}

func getToUpperFunction() schema.CallableFunction {
	funcSchema, err := schema.NewCallableFunction(
		"toUpper",
		[]schema.Type{schema.NewStringSchema(nil, nil, nil)},
		schema.NewStringSchema(nil, nil, nil),
		false,
		schema.NewDisplayValue(
			schema.PointerTo("toUpper"),
			schema.PointerTo(
				"Returns a version of the input with Unicode letters mapped to their upper case form.",
			),
			nil,
		),
		strings.ToUpper, // Wrap go standard lib function.
	)
	if err != nil {
		panic(err)
	}
	return funcSchema
}

func getSplitStringFunction() schema.CallableFunction {
	funcSchema, err := schema.NewCallableFunction(
		"splitString",
		[]schema.Type{
			schema.NewStringSchema(nil, nil, nil),
			schema.NewStringSchema(nil, nil, nil),
		},
		schema.NewListSchema(schema.NewStringSchema(nil, nil, nil), nil, nil),
		false,
		schema.NewDisplayValue(
			schema.PointerTo("strSplit"),
			schema.PointerTo(
				"Returns a list of strings by splitting the given string\n"+
					"with the given separator.\n"+
					" Param 1: The string to split.\n"+
					" Param 2: The separator.",
			),
			nil,
		),
		strings.Split,
	)
	if err != nil {
		panic(err)
	}
	return funcSchema
}

func getReadFileFunction() schema.CallableFunction {
	funcSchema, err := schema.NewCallableFunction(
		"readFile",
		[]schema.Type{schema.NewStringSchema(nil, nil, nil)},
		schema.NewStringSchema(nil, nil, nil),
		true,
		schema.NewDisplayValue(
			schema.PointerTo("readFile"),
			schema.PointerTo(
				"Return a file as a string.\n"+
					"Param 1: The filepath to read into memory."),
			nil,
		),
		func(filePath string) (string, error) {
			absPath, err := filepath.Abs(filePath)
			if err != nil {
				return "", err
			}
			fileData, err := os.ReadFile(absPath) //nolint:gosec // potential file inclusion is handled because filepath.Abs() calls filepath.Clean()
			if err != nil {
				return "", err
			}
			return string(fileData), nil
		},
	)
	if err != nil {
		panic(err)
	}
	return funcSchema
}

// CombinedObjPropertyConstantName is the identifier string key that points to the container of constant or repeated values.
const CombinedObjPropertyConstantName = "constant"

// CombinedObjPropertyItemName is the identifier string key that points to the container of a list of objects.
const CombinedObjPropertyItemName = "item"

// CombinedObjIDDelimiter is the delimiter for joining Object ID strings and/or TypeID strings.
const CombinedObjIDDelimiter = "__"

func getBindConstantsFunction() schema.CallableFunction {
	funcSchema, err := schema.NewDynamicCallableFunction(
		"bindConstants",
		[]schema.Type{
			schema.NewListSchema(schema.NewAnySchema(), nil, nil),
			schema.NewAnySchema()},
		schema.NewDisplayValue(
			schema.PointerTo("Bind Constants"),
			schema.PointerTo(
				"Creates a list of objects with ID `CombinedObject`. "+
					"Each object has two properties `item` and `constant`.\n"+
					"Param 1: Value(s) to be included in the `item` field \n"+
					"Param 2: Value(s) to populate the field `constant` with every output item\n"),
			nil),
		func(items []any, columnValues any) (any, error) {
			combinedItems := make([]any, len(items))
			for k := range items {
				combinedItems[k] = map[string]any{
					CombinedObjPropertyItemName:     items[k],
					CombinedObjPropertyConstantName: columnValues,
				}
			}
			return combinedItems, nil
		},
		HandleTypeSchemaCombine,
	)
	if err != nil {
		panic(err)
	}
	return funcSchema
}

// HandleTypeSchemaCombine returns the type that is output by the 'bindConstants' function.
// Its parameter list requires a ListSchema, and at least one other schema of any type.
// A new ListSchema of ObjectSchemas is created from the two given types.
func HandleTypeSchemaCombine(inputType []schema.Type) (schema.Type, error) {
	itemsType, isList := inputType[0].(*schema.ListSchema)
	if !isList {
		return nil, fmt.Errorf("expected first type to be a list schema")
	}
	if len(inputType) != 2 {
		return nil, fmt.Errorf("expected exactly two types")
	}

	itemType := itemsType.ItemsValue
	objItemType, itemIsObject := schema.ConvertToObjectSchema(itemType)

	constantsTypeArg := inputType[1]
	constantsType, constantsIsObject := schema.ConvertToObjectSchema(constantsTypeArg)

	var combinedObjectName string
	if itemIsObject {
		combinedObjectName = objItemType.ID()
	} else {
		combinedObjectName = string(itemType.TypeID())
	}
	if constantsIsObject {
		combinedObjectName += CombinedObjIDDelimiter + constantsType.ID()
	} else {
		combinedObjectName += CombinedObjIDDelimiter + string(constantsTypeArg.TypeID())
	}

	return schema.NewListSchema(
		schema.NewObjectSchema(
			combinedObjectName,
			map[string]*schema.PropertySchema{
				CombinedObjPropertyItemName:     schema.NewPropertySchema(itemType, nil, false, nil, nil, nil, nil, nil),
				CombinedObjPropertyConstantName: schema.NewPropertySchema(constantsTypeArg, nil, false, nil, nil, nil, nil, nil),
			}),
		nil, nil), nil
}
