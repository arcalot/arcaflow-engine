package builtinfunctions_test

import (
	"fmt"
	"go.arcalot.io/assert"
	"go.flow.arcalot.io/engine/internal/builtinfunctions"
	"go.flow.arcalot.io/pluginsdk/schema"
	"math"
	"os"
	"reflect"
	"regexp"
	"strings"
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
	"float-to-int-positive-zero": {
		"floatToInt",
		[]any{0.0},
		false,
		int64(0),
	},
	"float-to-int-negative-zero": {
		"floatToInt",
		[]any{math.Copysign(0.0, -1)},
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
		true,
		nil,
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
	"float-to-string-negative-zero": {
		"floatToString",
		[]any{math.Copysign(0.0, -1)},
		false,
		"-0",
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
	"string-to-int-zero-prefixed": {
		"stringToInt",
		[]any{"-00005"},
		false,
		int64(-5),
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
		math.Copysign(0.0, -1),
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
		[]string{""},
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
	"readFile": {
		"readFile",
		[]any{"../../fixtures/test-readFile/hello-world.yaml"},
		false,
		"hello: world\n",
	},
	"readFile_nonexistent-file": {
		"readFile",
		[]any{"../../fixtures/test-readFile/nonexistent-file.yaml"},
		true,
		nil,
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

// Test_floatToFormattedString_success tests the success cases for the builtin
// floatToFormattedString() function.
func Test_floatToFormattedString_success(t *testing.T) {
	// The floatToFormattedString() function supports 8 formats, specified by a
	// single-character string; for each format, we test four different precision
	// values; for each combination of format and precision, we test ten different
	// input values, for a total of 290 cases ('b' format ignores the precision,
	// so we test only one set of values for those cases).  We iterate over each
	// of the formats, each of the precisions, and each of the input values and
	// check the output against the expected value.  The expected output values
	// are the innermost lists in the nested map; the input values (which don't
	// change with the format and precision) are held in a separate list:  a small
	// exact (integral) value, a large integral value, a small decimal fraction,
	// an exact fraction (a negative power of two), an inexact fraction (a
	// "repeating decimal"), positive and negative infinity, positive and negative
	// zero, and not-a-number.
	inputs := []float64{
		123, 12300, 0.000123, 1.0 / 8.0, 2.0 / 3.0,
		math.Inf(1), math.Inf(-1), math.NaN(),
		math.Copysign(0.0, 1.0), math.Copysign(0.0, -1.0),
	}
	results := map[string]map[int64][]string{
		"b": {
			// The 'b' format ignores the precision, so test only one precision value
			0: {
				"8655355533852672p-46", "6761996510822400p-39", "4537899042132550p-65",
				"4503599627370496p-55", "6004799503160661p-53", "+Inf", "-Inf", "NaN",
				"0p-1074", "-0p-1074",
			},
		},
		"e": {
			0: {
				"1e+02", "1e+04", "1e-04", "1e-01", "7e-01", "+Inf", "-Inf", "NaN",
				"0e+00", "-0e+00",
			},
			1: {
				"1.2e+02", "1.2e+04", "1.2e-04", "1.2e-01", "6.7e-01", "+Inf", "-Inf",
				"NaN", "0.0e+00", "-0.0e+00",
			},
			-1: {
				"1.23e+02", "1.23e+04", "1.23e-04", "1.25e-01", "6.666666666666666e-01",
				"+Inf", "-Inf", "NaN", "0e+00", "-0e+00",
			},
			15: {
				"1.230000000000000e+02", "1.230000000000000e+04", "1.230000000000000e-04",
				"1.250000000000000e-01", "6.666666666666666e-01", "+Inf", "-Inf", "NaN",
				"0.000000000000000e+00", "-0.000000000000000e+00",
			},
		},
		"E": {
			0: {
				"1E+02", "1E+04", "1E-04", "1E-01", "7E-01", "+Inf", "-Inf", "NaN",
				"0E+00", "-0E+00",
			},
			1: {
				"1.2E+02", "1.2E+04", "1.2E-04", "1.2E-01", "6.7E-01", "+Inf", "-Inf",
				"NaN", "0.0E+00", "-0.0E+00",
			},
			-1: {
				"1.23E+02", "1.23E+04", "1.23E-04", "1.25E-01", "6.666666666666666E-01",
				"+Inf", "-Inf", "NaN", "0E+00", "-0E+00",
			},
			15: {
				"1.230000000000000E+02", "1.230000000000000E+04", "1.230000000000000E-04",
				"1.250000000000000E-01", "6.666666666666666E-01", "+Inf", "-Inf", "NaN",
				"0.000000000000000E+00", "-0.000000000000000E+00",
			},
		},
		"f": {
			0: {"123", "12300", "0", "0", "1", "+Inf", "-Inf", "NaN", "0", "-0"},
			1: {
				"123.0", "12300.0", "0.0", "0.1", "0.7", "+Inf", "-Inf", "NaN", "0.0",
				"-0.0",
			},
			-1: {
				"123", "12300", "0.000123", "0.125", "0.6666666666666666", "+Inf",
				"-Inf", "NaN", "0", "-0",
			},
			15: {
				"123.000000000000000", "12300.000000000000000", "0.000123000000000",
				"0.125000000000000", "0.666666666666667", "+Inf", "-Inf", "NaN",
				"0.000000000000000", "-0.000000000000000",
			},
		},
		"g": {
			// Uses 'f' format unless the exponent is less than -4, or if the
			// exponent is greater than 5 and the precision is too small to
			// represent all the digits, in which case it uses 'e' format; however,
			// unlike 'e' format, the precision limits the total number of digits
			// rather than the number of digits to the right of the decimal point.
			// Also, trailing zeros are removed.
			0: {
				"1e+02", "1e+04", "0.0001", "0.1", "0.7", "+Inf", "-Inf", "NaN",
				"0", "-0",
			},
			1: {
				"1e+02", "1e+04", "0.0001", "0.1", "0.7", "+Inf", "-Inf", "NaN",
				"0", "-0",
			},
			-1: {
				"123", "12300", "0.000123", "0.125", "0.6666666666666666", "+Inf",
				"-Inf", "NaN", "0", "-0",
			},
			15: {
				"123", "12300", "0.000123", "0.125", "0.666666666666667", "+Inf",
				"-Inf", "NaN", "0", "-0",
			},
		},
		"G": {
			// See note for 'g' format.
			0: {
				"1E+02", "1E+04", "0.0001", "0.1", "0.7", "+Inf", "-Inf", "NaN",
				"0", "-0",
			},
			1: {
				"1E+02", "1E+04", "0.0001", "0.1", "0.7", "+Inf", "-Inf", "NaN",
				"0", "-0",
			},
			-1: {
				"123", "12300", "0.000123", "0.125", "0.6666666666666666", "+Inf",
				"-Inf", "NaN", "0", "-0",
			},
			15: {
				"123", "12300", "0.000123", "0.125", "0.666666666666667", "+Inf",
				"-Inf", "NaN", "0", "-0",
			},
		},
		"x": {
			0: {
				"0x1p+07", "0x1p+14", "0x1p-13", "0x1p-03", "0x1p-01", "+Inf",
				"-Inf", "NaN", "0x0p+00", "-0x0p+00",
			},
			1: {
				"0x1.fp+06", "0x1.8p+13", "0x1.0p-13", "0x1.0p-03", "0x1.5p-01",
				"+Inf", "-Inf", "NaN", "0x0.0p+00", "-0x0.0p+00",
			},
			-1: {
				"0x1.ecp+06", "0x1.806p+13", "0x1.01f31f46ed246p-13", "0x1p-03",
				"0x1.5555555555555p-01", "+Inf", "-Inf", "NaN", "0x0p+00", "-0x0p+00",
			},
			15: {
				"0x1.ec0000000000000p+06", "0x1.806000000000000p+13",
				"0x1.01f31f46ed24600p-13", "0x1.000000000000000p-03",
				"0x1.555555555555500p-01", "+Inf", "-Inf", "NaN",
				"0x0.000000000000000p+00", "-0x0.000000000000000p+00",
			},
		},
		"X": {
			0: {
				"0X1P+07", "0X1P+14", "0X1P-13", "0X1P-03", "0X1P-01", "+Inf",
				"-Inf", "NaN", "0X0P+00", "-0X0P+00",
			},
			1: {
				"0X1.FP+06", "0X1.8P+13", "0X1.0P-13", "0X1.0P-03", "0X1.5P-01",
				"+Inf", "-Inf", "NaN", "0X0.0P+00", "-0X0.0P+00",
			},
			-1: {
				"0X1.ECP+06", "0X1.806P+13", "0X1.01F31F46ED246P-13", "0X1P-03",
				"0X1.5555555555555P-01", "+Inf", "-Inf", "NaN", "0X0P+00", "-0X0P+00",
			},
			15: {
				"0X1.EC0000000000000P+06", "0X1.806000000000000P+13",
				"0X1.01F31F46ED24600P-13", "0X1.000000000000000P-03",
				"0X1.555555555555500P-01", "+Inf", "-Inf", "NaN",
				"0X0.000000000000000P+00", "-0X0.000000000000000P+00",
			},
		},
	}
	functionToTest, funcFound :=
		builtinfunctions.GetFunctions()["floatToFormattedString"]
	if !funcFound {
		t.Fatalf("Function \"floatToFormattedString\" not found.")
	}
	for f, precisions := range results {
		for p, expectedValues := range precisions {
			// Make sure the test provides the same number of inputs and outputs
			assert.Equals(t, len(expectedValues), len(inputs))
			for i, expected := range expectedValues {
				v := inputs[i]
				name := fmt.Sprintf("floatToFormattedString:%s:%d:%f", f, p, v)
				rawArguments := []any{v, f, p}
				expectedValue := expected // Capture loop index in local scope
				t.Run(name, func(t *testing.T) {
					output, err := functionToTest.Call(rawArguments)
					if err != nil {
						t.Fatalf(
							"unexpected error in test case %q (%s)",
							name, err.Error(),
						)
					}
					if value, OK := output.(string); OK {
						assert.Equals[string](t, value, expectedValue)
					} else {
						t.Fatalf("output is not a string: %v", output)
					}
				})
			}
		}
	}
}

func expectedOutputBindConstants(repInp any, items []any) []any {
	return []any{
		map[string]any{builtinfunctions.CombinedObjPropertyItemName: items[0], builtinfunctions.CombinedObjPropertyConstantName: repInp},
		map[string]any{builtinfunctions.CombinedObjPropertyItemName: items[1], builtinfunctions.CombinedObjPropertyConstantName: repInp},
		map[string]any{builtinfunctions.CombinedObjPropertyItemName: items[2], builtinfunctions.CombinedObjPropertyConstantName: repInp},
	}
}

func Test_bindConstants(t *testing.T) {
	items := []any{
		map[string]any{"loop_id": 1},
		map[string]any{"loop_id": 2},
		map[string]any{"loop_id": 3},
	}
	repeatedInputsMap := map[string]any{
		"a": "A", "b": "B",
	}
	repeatedInputsInt := 2
	repeatedInputsRegexPattern := regexp.MustCompile("p([a-z]+)ch")
	testItems := map[string]any{
		"combine with map":   repeatedInputsMap,
		"combine with list":  items,
		"combine with int":   repeatedInputsInt,
		"combine with regex": repeatedInputsRegexPattern,
	}
	functionToTest := builtinfunctions.GetFunctions()["bindConstants"]

	for testName, testValue := range testItems {
		tvLocal := testValue
		t.Run(testName, func(t *testing.T) {
			output, err := functionToTest.Call([]any{items, tvLocal})
			assert.NoError(t, err)
			assert.Equals(t, output.([]any), expectedOutputBindConstants(tvLocal, items))
		})
	}

	t.Run("no items in input list", func(t *testing.T) {
		output, err := functionToTest.Call([]any{[]any{}, repeatedInputsMap})
		assert.NoError(t, err)
		assert.Equals(t, output.([]any), []any{})
	})
}

func defaultPropertySchema(t schema.Type) *schema.PropertySchema {
	return schema.NewPropertySchema(t, nil, false, nil, nil, nil, nil, nil)
}

func joinStrs(s1, s2 string) string {
	return strings.Join([]string{s1, s2}, builtinfunctions.CombinedObjIDDelimiter)
}

// TestHandleTypeSchemaCombine tests that the error cases for invalid
// and valid input cases create the expected error or type.
func TestHandleTypeSchemaCombine(t *testing.T) {
	basicStringSchema := schema.NewStringSchema(nil, nil, nil)
	basicIntSchema := schema.NewIntSchema(nil, nil, nil)
	strTypeID := string(basicStringSchema.TypeID())
	intTypeID := string(basicIntSchema.TypeID())
	listInt := schema.NewListSchema(basicIntSchema, nil, nil)
	listStr := schema.NewListSchema(basicStringSchema, nil, nil)
	myFirstObj := schema.NewObjectSchema(
		"ObjectTitle",
		map[string]*schema.PropertySchema{
			"a": defaultPropertySchema(basicStringSchema),
			"b": defaultPropertySchema(basicStringSchema),
		})
	listMyFirstObj := schema.NewListSchema(myFirstObj, nil, nil)
	constantsObj := schema.NewObjectSchema(
		"Constants",
		map[string]*schema.PropertySchema{
			"p_str": defaultPropertySchema(basicStringSchema),
			"p_int": defaultPropertySchema(basicIntSchema),
		})
	listPrefix := string(listInt.TypeID()) + builtinfunctions.ListSchemaNameDelimiter

	// invalid inputs
	t.Run("first argument incorrect type", func(t *testing.T) {
		_, err := builtinfunctions.HandleTypeSchemaCombine(
			[]schema.Type{basicStringSchema, basicIntSchema})
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "expected first input type to be a list schema")
	})
	t.Run("incorrect argument quantity", func(t *testing.T) {
		_, err := builtinfunctions.HandleTypeSchemaCombine(
			[]schema.Type{listInt})
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "expected exactly two input types")
	})

	// valid inputs
	type testInput struct {
		typeArgs       []schema.Type
		expectedResult string
	}
	testInputs := []testInput{
		{ // Inputs:  a list of int's and an object
			typeArgs:       []schema.Type{listInt, constantsObj},
			expectedResult: joinStrs(intTypeID, constantsObj.ID()),
		},
		{ // Inputs:  a list of objects and an object
			typeArgs:       []schema.Type{listMyFirstObj, constantsObj},
			expectedResult: joinStrs(myFirstObj.ID(), constantsObj.ID()),
		},
		{ // Inputs:  a list of objects and a string
			typeArgs:       []schema.Type{listMyFirstObj, basicStringSchema},
			expectedResult: joinStrs(myFirstObj.ID(), strTypeID),
		},
		{ // Inputs:  a list of strings and an int
			typeArgs:       []schema.Type{listStr, basicIntSchema},
			expectedResult: joinStrs(strTypeID, intTypeID),
		},
		{ // Inputs: a list of strings and a list of objects
			typeArgs:       []schema.Type{listStr, listMyFirstObj},
			expectedResult: joinStrs(strTypeID, listPrefix+myFirstObj.ID()),
		},
	}

	for _, input := range testInputs {
		lclInput := input
		t.Run(lclInput.expectedResult, func(t *testing.T) {
			outputType, err := builtinfunctions.HandleTypeSchemaCombine(lclInput.typeArgs)
			assert.NoError(t, err)
			listItemObj, isObj := schema.ConvertToObjectSchema(outputType.(*schema.ListSchema).ItemsValue)
			assert.Equals(t, isObj, true)
			assert.Equals(t, listItemObj.ID(), lclInput.expectedResult)
		})
	}
}

func TestBuildSchemaNames(t *testing.T) {
	basicStringSchema := schema.NewStringSchema(nil, nil, nil)
	basicIntSchema := schema.NewIntSchema(nil, nil, nil)
	intTypeID := string(basicIntSchema.TypeID())
	listInt := schema.NewListSchema(basicIntSchema, nil, nil)
	myFirstObj := schema.NewObjectSchema(
		"ObjectTitle",
		map[string]*schema.PropertySchema{
			"a": defaultPropertySchema(basicStringSchema),
			"b": defaultPropertySchema(basicStringSchema),
		})
	listMyFirstObj := schema.NewListSchema(myFirstObj, nil, nil)

	listTypeID := string(listMyFirstObj.TypeID())

	listListInt := schema.NewListSchema(listInt, nil, nil)
	listListListInt := schema.NewListSchema(listListInt, nil, nil)
	listListObj := schema.NewListSchema(listMyFirstObj, nil, nil)
	listListListObj := schema.NewListSchema(listListObj, nil, nil)

	// valid inputs
	type testInput struct {
		typeArgs       schema.Type
		expectedResult []string
	}
	testInputs := []testInput{
		{
			typeArgs:       listInt,
			expectedResult: []string{listTypeID, intTypeID},
		},
		{
			typeArgs:       listMyFirstObj,
			expectedResult: []string{listTypeID, myFirstObj.ID()},
		},
		{
			typeArgs:       listListInt,
			expectedResult: []string{listTypeID, listTypeID, intTypeID},
		},
		{
			typeArgs:       listListObj,
			expectedResult: []string{listTypeID, listTypeID, myFirstObj.ID()},
		},
		{
			typeArgs:       listListListInt,
			expectedResult: []string{listTypeID, listTypeID, listTypeID, intTypeID},
		},
		{
			typeArgs:       listListListObj,
			expectedResult: []string{listTypeID, listTypeID, listTypeID, myFirstObj.ID()},
		},
	}

	for _, input := range testInputs {
		lclInput := input
		t.Run(strings.Join(lclInput.expectedResult, "_"), func(t *testing.T) {
			outputNames := builtinfunctions.BuildSchemaNames(lclInput.typeArgs, []string{})
			assert.Equals(t, outputNames, lclInput.expectedResult)
		})
	}
}

func TestReadEnvVarFunction(t *testing.T) {
	// Set the env variables to get below.
	knownPresentEnvVarKey := "test_known_present_env_var_key"
	knownNotPresentEnvVarKey := "test_known_not_present_env_var_key"
	knownEnvVarValue := "known value"
	defaultEnvVarValue := "default"
	assert.NoError(t, os.Setenv(knownPresentEnvVarKey, knownEnvVarValue))
	assert.NoError(t, os.Unsetenv(knownNotPresentEnvVarKey))
	assert.MapContainsKey(t, "getEnvVar", builtinfunctions.GetFunctions())
	readEnvVarFunction := builtinfunctions.GetFunctions()["getEnvVar"]
	// Test the present env var
	result, err := readEnvVarFunction.Call([]any{knownPresentEnvVarKey, defaultEnvVarValue})
	assert.NoError(t, err)
	assert.Equals(t, result, any(knownEnvVarValue))
	// Test the missing env var
	result, err = readEnvVarFunction.Call([]any{knownNotPresentEnvVarKey, defaultEnvVarValue})
	assert.NoError(t, err)
	assert.Equals(t, result, any(defaultEnvVarValue))
}
