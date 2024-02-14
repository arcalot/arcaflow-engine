package builtinfunctions_test

import (
	"go.flow.arcalot.io/engine/internal/builtinfunctions"
	"math"
	"reflect"
	"testing"
)

var testData = map[string]struct {
	functionName   string
	rawArguments   []any
	returnError    bool
	expectedResult any
}{
	"int-to-float-positive": {
		"intToFloat",
		[]any{int64(1)},
		false,
		1.0,
	},
	"int-to-float-negative": {
		"intToFloat",
		[]any{int64(-1)},
		false,
		-1.0,
	},
	"int-to-float-zero": {
		"intToFloat",
		[]any{int64(0)},
		false,
		0.0,
	},
	"float-to-int-positive": {
		"floatToInt",
		[]any{1.0},
		false,
		int64(1),
	},
	"float-to-int-negative-whole": {
		"floatToInt",
		[]any{-1.0},
		false,
		int64(-1),
	},
	"float-to-int-negative-fractional": {
		"floatToInt",
		[]any{-1.9},
		false,
		int64(-1),
	},
	"float-to-int-zero": {
		"floatToInt",
		[]any{0.0},
		false,
		int64(0),
	},
	"float-to-int-positive-infinity": {
		"floatToInt",
		[]any{math.Inf(1)},
		false,
		int64(math.MaxInt64),
	},
	"float-to-int-negative-infinity": {
		"floatToInt",
		[]any{math.Inf(-1)},
		false,
		int64(math.MinInt64),
	},
	"float-to-int-nan": {
		"floatToInt",
		[]any{math.NaN()},
		false,
		int64(math.MinInt64),
	},
	"int-to-string-positive": {
		"intToString",
		[]any{int64(11)},
		false,
		"11",
	},
	"int-to-string-zero": {
		"intToString",
		[]any{int64(0)},
		false,
		"0",
	},
	"int-to-string-negative": {
		"intToString",
		[]any{int64(-11)},
		false,
		"-11",
	},
	"float-to-string-positive-whole": {
		"floatToString",
		[]any{11.0},
		false,
		"11",
	},
	"float-to-string-negative-whole": {
		"floatToString",
		[]any{-11.0},
		false,
		"-11",
	},
	"float-to-string-positive-fractional": {
		"floatToString",
		[]any{11.1},
		false,
		"11.1",
	},
	"float-to-string-negative-fractional": {
		"floatToString",
		[]any{-11.1},
		false,
		"-11.1",
	},
	"float-to-string-zero": {
		"floatToString",
		[]any{0.0},
		false,
		"0",
	},
	"float-to-string-nan": {
		"floatToString",
		[]any{math.NaN()},
		false,
		"NaN",
	},
	"float-to-string-positive-infinity": {
		"floatToString",
		[]any{math.Inf(1)},
		false,
		"+Inf",
	},
	"float-to-string-negative-infinity": {
		"floatToString",
		[]any{math.Inf(-1)},
		false,
		"-Inf",
	},
	"bool-to-string-true": {
		"boolToString",
		[]any{true},
		false,
		"true",
	},
	"bool-to-string-false": {
		"boolToString",
		[]any{false},
		false,
		"false",
	},
	"string-to-int-positive": {
		"stringToInt",
		[]any{"11"},
		false,
		int64(11),
	},
	"string-to-int-negative": {
		"stringToInt",
		[]any{"-11"},
		false,
		int64(-11),
	},
	"string-to-int-zero": {
		"stringToInt",
		[]any{"0"},
		false,
		int64(0),
	},
	"string-to-int-invalid-1": {
		"stringToInt",
		[]any{"zero"},
		true,
		nil,
	},
	"string-to-int-invalid-2": {
		"stringToInt",
		[]any{"1.0"},
		true,
		nil,
	},
	"string-to-int-invalid-3": {
		"stringToInt",
		[]any{""},
		true,
		nil,
	},
	"string-to-int-invalid-4": {
		"stringToInt",
		[]any{"0b1"},
		true,
		nil,
	},
	"string-to-int-invalid-5": {
		"stringToInt",
		[]any{"0o1"},
		true,
		nil,
	},
	"string-to-int-invalid-6": {
		"stringToInt",
		[]any{"0x1"},
		true,
		nil,
	},
	"string-to-int-invalid-7": {
		"stringToInt",
		[]any{"1_000_000"},
		true,
		nil,
	},
	"string-to-float-positive": {
		"stringToFloat",
		[]any{"11.1"},
		false,
		11.1,
	},
	"string-to-float-negative": {
		"stringToFloat",
		[]any{"-11.1"},
		false,
		-11.1,
	},
	"string-to-float-zero-1": {
		"stringToFloat",
		[]any{"0"},
		false,
		0.0,
	},
	"string-to-float-zero-2": {
		"stringToFloat",
		[]any{"-0"},
		false,
		0.0,
	},
	"string-to-float-positive-infinity-1": {
		"stringToFloat",
		[]any{"Inf"},
		false,
		math.Inf(1),
	},
	"string-to-float-positive-infinity-2": {
		"stringToFloat",
		[]any{"+Inf"},
		false,
		math.Inf(1),
	},
	"string-to-float-negative-infinity": {
		"stringToFloat",
		[]any{"-Inf"},
		false,
		math.Inf(-1),
	},
	"string-to-float-scientific-notation-1": {
		"stringToFloat",
		[]any{"5e+5"},
		false,
		500000.0,
	},
	"string-to-float-scientific-notation-2": {
		"stringToFloat",
		[]any{"5E-5"},
		false,
		0.00005,
	},
	"string-to-float-error-1": {
		"stringToFloat",
		[]any{""},
		true,
		nil,
	},
	"string-to-float-error-2": {
		"stringToFloat",
		[]any{"ten"},
		true,
		nil,
	},
	"string-to-float-error-3": {
		"stringToFloat",
		[]any{"5E+500"},
		true,
		nil,
	},
	"string-to-bool-1": {
		"stringToBool",
		[]any{"true"},
		false,
		true,
	},
	"string-to-bool-2": {
		"stringToBool",
		[]any{"True"},
		false,
		true,
	},
	"string-to-bool-3": {
		"stringToBool",
		[]any{"TRUE"},
		false,
		true,
	},
	"string-to-bool-4": {
		"stringToBool",
		[]any{"t"},
		false,
		true,
	},
	"string-to-bool-5": {
		"stringToBool",
		[]any{"T"},
		false,
		true,
	},
	"string-to-bool-6": {
		"stringToBool",
		[]any{"1"},
		false,
		true,
	},
	"string-to-bool-7": {
		"stringToBool",
		[]any{"false"},
		false,
		false,
	},
	"string-to-bool-8": {
		"stringToBool",
		[]any{"False"},
		false,
		false,
	},
	"string-to-bool-9": {
		"stringToBool",
		[]any{"FALSE"},
		false,
		false,
	},
	"string-to-bool-10": {
		"stringToBool",
		[]any{"f"},
		false,
		false,
	},
	"string-to-bool-11": {
		"stringToBool",
		[]any{"F"},
		false,
		false,
	},
	"string-to-bool-12": {
		"stringToBool",
		[]any{"0"},
		false,
		false,
	},
	"string-to-bool-error-1": {
		"stringToBool",
		[]any{""},
		true,
		nil,
	},
	"string-to-bool-error-2": {
		"stringToBool",
		[]any{"abc"},
		true,
		nil,
	},
	"ceil-1": {
		"ceil",
		[]any{0.0},
		false,
		0.0,
	},
	"ceil-2": {
		"ceil",
		[]any{0.1},
		false,
		1.0,
	},
	"ceil-3": {
		"ceil",
		[]any{0.99},
		false,
		1.0,
	},
	"ceil-4": {
		"ceil",
		[]any{-0.1},
		false,
		0.0,
	},
	"floor-1": {
		"floor",
		[]any{0.0},
		false,
		0.0,
	},
	"floor-2": {
		"floor",
		[]any{1.1},
		false,
		1.0,
	},
	"floor-3": {
		"floor",
		[]any{2.9999},
		false,
		2.0,
	},
	"floor-4": {
		"floor",
		[]any{-0.1},
		false,
		-1.0,
	},
	"round-1": {
		"round",
		[]any{0.0},
		false,
		0.0,
	},
	"round-2": {
		"round",
		[]any{0.1},
		false,
		0.0,
	},
	"round-3": {
		"round",
		[]any{0.4},
		false,
		0.0,
	},
	"round-4": {
		"round",
		[]any{0.5},
		false,
		1.0,
	},
	"round-5": {
		"round",
		[]any{0.9},
		false,
		1.0,
	},
	"round-6": {
		"round",
		[]any{-0.9},
		false,
		-1.0,
	},
	"round-7": {
		"round",
		[]any{-0.5},
		false,
		-1.0,
	},
	"round-8": {
		"round",
		[]any{-0.01},
		false,
		0.0,
	},
	"abs-1": {
		"abs",
		[]any{0.0},
		false,
		0.0,
	},
	"abs-2": {
		"abs",
		[]any{-1.0},
		false,
		1.0,
	},
	"abs-3": {
		"abs",
		[]any{1.0},
		false,
		1.0,
	},
	"to-lower-1": {
		"toLower",
		[]any{""},
		false,
		"",
	},
	"to-lower-2": {
		"toLower",
		[]any{"abc"},
		false,
		"abc",
	},
	"to-lower-3": {
		"toLower",
		[]any{"ABC"},
		false,
		"abc",
	},
	"to-lower-4": {
		"toLower",
		[]any{"aBc"},
		false,
		"abc",
	},
	"to-upper-1": {
		"toUpper",
		[]any{""},
		false,
		"",
	},
	"to-upper-2": {
		"toUpper",
		[]any{"abc"},
		false,
		"ABC",
	},
	"to-upper-3": {
		"toUpper",
		[]any{"ABC"},
		false,
		"ABC",
	},
	"to-upper-4": {
		"toUpper",
		[]any{"aBc"},
		false,
		"ABC",
	},
	"split-string-1": {
		"splitString",
		[]any{"", ","},
		false,
		make([]string, 1), // The size appears to matter with DeepEquals
	},
	"split-string-2": {
		"splitString",
		[]any{"abc", ","},
		false,
		[]string{"abc"},
	},
	"split-string-3": {
		"splitString",
		[]any{"a,b", ","},
		false,
		[]string{"a", "b"},
	},
	"split-string-4": {
		"splitString",
		[]any{"a,b,c,d", ","},
		false,
		[]string{"a", "b", "c", "d"},
	},
	"split-string-5": {
		"splitString",
		[]any{"1,,2,", ","},
		false,
		[]string{"1", "", "2", ""},
	},
}

func TestFunctionsBulk(t *testing.T) {
	allFunctions := builtinfunctions.GetFunctions()
	for name, tc := range testData {
		testCase := tc
		name := name
		t.Run(name, func(t *testing.T) {
			functionToTest, funcFound := allFunctions[testCase.functionName]
			if !funcFound {
				t.Fatalf("Function %q not found", testCase.functionName)
			}

			output, err := functionToTest.Call(testCase.rawArguments)

			if testCase.returnError && err == nil {
				t.Fatalf("expected error in test case %q; error returned nil", name)
			} else if !testCase.returnError && err != nil {
				t.Fatalf("unexpected error in test case %q (%s)", name, err.Error())
			}

			if !reflect.DeepEqual(testCase.expectedResult, output) {
				t.Fatalf("mismatch for test %q, expected: %v, got: %v", name, testCase.expectedResult, output)
			}
		})
	}
}
