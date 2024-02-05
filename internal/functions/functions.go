package functions

import (
	"go.flow.arcalot.io/pluginsdk/schema"
	"math"
	"regexp"
	"strconv"
)

func getFunctions() map[string]schema.Function {
	// Getters
	intToFloatFunction := getIntToFloatFunction()
	floatToIntFunction := getFloatToIntFunction()
	intToStringFunction := getIntToStringFunction()
	floatToStringFunction := getFloatToStringFunction()
	booleanToStringFunction := getBooleanToStringFunction()
	ceilFunction := getCeilFunction()
	floorFunction := getFloorFunction()
	roundFunction := getRoundFunction()

	// Combine in a map
	allFunctions := map[string]schema.Function{
		intToFloatFunction.ID():      intToFloatFunction,
		floatToIntFunction.ID():      floatToIntFunction,
		intToStringFunction.ID():     intToStringFunction,
		floatToStringFunction.ID():   floatToStringFunction,
		booleanToStringFunction.ID(): booleanToStringFunction,
		ceilFunction.ID():            ceilFunction,
		floorFunction.ID():           floorFunction,
		roundFunction.ID():           roundFunction,
	}

	return allFunctions
}

func getIntToFloatFunction() schema.Function {
	funcSchema, err := schema.NewCallableFunction(
		"intToFloat",
		[]schema.Type{schema.NewIntSchema(nil, nil, nil)},
		schema.NewFloatSchema(nil, nil, nil),
		false,
		schema.NewDisplayValue(
			schema.PointerTo("intToFloat"),
			schema.PointerTo("Converts an integer type into a floating point type."),
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

func getFloatToIntFunction() schema.Function {
	funcSchema, err := schema.NewCallableFunction(
		"floatToInt",
		[]schema.Type{schema.NewFloatSchema(nil, nil, nil)},
		schema.NewIntSchema(nil, nil, nil),
		false,
		schema.NewDisplayValue(
			schema.PointerTo("floatToInt"),
			schema.PointerTo(
				"Converts a float type into an integer type by down-casting. "+
					"The value loses any data after the decimal point.\n"+
					"For example, `5.5` becomes `5`",
			),
			nil,
		),
		func(a float64) int64 {
			return int64(a)
		},
	)
	if err != nil {
		panic(err)
	}
	return funcSchema
}

func getIntToStringFunction() schema.Function {
	funcSchema, err := schema.NewCallableFunction(
		"intToString",
		[]schema.Type{schema.NewIntSchema(nil, nil, nil)},
		schema.NewStringSchema(nil, nil, regexp.MustCompile(`^(?:0|[1-9]\d*)$`)),
		false,
		schema.NewDisplayValue(
			schema.PointerTo("intToString"),
			schema.PointerTo(
				"Converts an integer to a string whose characters represent that integer in base-10.\n"+
					"For example, an input of `55` will output \"55\"",
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

func getFloatToStringFunction() schema.Function {
	funcSchema, err := schema.NewCallableFunction(
		"floatToString",
		[]schema.Type{schema.NewFloatSchema(nil, nil, nil)},
		schema.NewStringSchema(nil, nil, regexp.MustCompile(`^\d+\.\d*$`)),
		false,
		schema.NewDisplayValue(
			schema.PointerTo("floatToString"),
			schema.PointerTo(
				"Converts a floating point number to a string whose characters"+
					"represent that number in base-10 as as simple decimal.\n"+
					"For example, an input of `5000.5` will output \"5000.5\"",
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

// TODO: Webb can add a function called floatToStringAdvanced that allows other float formats

func getBooleanToStringFunction() schema.Function {
	funcSchema, err := schema.NewCallableFunction(
		"booleanToString",
		[]schema.Type{schema.NewBoolSchema()},
		schema.NewStringSchema(nil, nil, regexp.MustCompile(`^true|false$`)),
		false,
		schema.NewDisplayValue(
			schema.PointerTo("booleanToString"),
			schema.PointerTo(
				"Represents `true` as \"true\", and `false` as \"false\".",
			),
			nil,
		),
		func(a bool) string {
			return strconv.FormatBool(a)
		},
	)
	if err != nil {
		panic(err)
	}
	return funcSchema
}

func getStringToIntFunction() schema.Function {
	funcSchema, err := schema.NewCallableFunction(
		"",
		[]schema.Type{schema.NewStringSchema(nil, nil, nil)},
		schema.NewStringSchema(nil, nil, regexp.MustCompile(``)),
		false,
		schema.NewDisplayValue(
			schema.PointerTo(""),
			schema.PointerTo(
				"",
			),
			nil,
		),
		func(a string) {
			return
		},
	)
	if err != nil {
		panic(err)
	}
	return funcSchema
}

// TODO:
// string to float
// string to bool

func getCeilFunction() schema.Function {
	funcSchema, err := schema.NewCallableFunction(
		"ceil",
		[]schema.Type{schema.NewFloatSchema(nil, nil, nil)},
		schema.NewFloatSchema(nil, nil, nil),
		false,
		schema.NewDisplayValue(
			schema.PointerTo("ceil"),
			schema.PointerTo(
				// Description based on documentation for math.Ceil
				"Ceil returns the least integer value greater than or equal to x.\n"+
					"For example `ceil(1.5)` outputs `2.0`, and `ceil(-1.5)` outputs `-1.0`"+
					"Special cases are:\n"+
					"Ceil(±0) = ±0\n"+
					"Ceil(±Inf) = ±Inf\n"+
					"Ceil(NaN) = Na",
			),
			nil,
		),
		func(a float64) float64 {
			return math.Ceil(a)
		},
	)
	if err != nil {
		panic(err)
	}
	return funcSchema
}

func getFloorFunction() schema.Function {
	funcSchema, err := schema.NewCallableFunction(
		"floor",
		[]schema.Type{schema.NewFloatSchema(nil, nil, nil)},
		schema.NewFloatSchema(nil, nil, nil),
		false,
		schema.NewDisplayValue(
			schema.PointerTo("floor"),
			schema.PointerTo(
				// Description based on documentation for math.Floor
				"Floor returns the greatest integer value less than or equal to x.\n"+
					"For example `floor(1.5)` outputs `1.0`, and `floor(-1.5)` outputs `-2.0`"+
					"Special cases are:\n"+
					"Ceil(±0) = ±0\n"+
					"Ceil(±Inf) = ±Inf\n"+
					"Ceil(NaN) = Na",
			),
			nil,
		),
		func(a float64) float64 {
			return math.Floor(a)
		},
	)
	if err != nil {
		panic(err)
	}
	return funcSchema
}

func getRoundFunction() schema.Function {
	funcSchema, err := schema.NewCallableFunction(
		"round",
		[]schema.Type{schema.NewFloatSchema(nil, nil, nil)},
		schema.NewFloatSchema(nil, nil, nil),
		false,
		schema.NewDisplayValue(
			schema.PointerTo("round"),
			schema.PointerTo(
				// Description based on documentation for math.Round
				"Round returns the nearest integer, rounding half away from zero.\n"+
					"For example `round(1.5)` outputs `2.0`, and `round(-1.5)` outputs `-2.0`"+
					"Special cases are:\n"+
					"Ceil(±0) = ±0\n"+
					"Ceil(±Inf) = ±Inf\n"+
					"Ceil(NaN) = Na",
			),
			nil,
		),
		func(a float64) float64 {
			return math.Round(a)
		},
	)
	if err != nil {
		panic(err)
	}
	return funcSchema
}
