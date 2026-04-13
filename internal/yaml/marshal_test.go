package yaml //nolint:testpackage

import (
	"math"
	"testing"

	"go.arcalot.io/assert"
	goyaml "gopkg.in/yaml.v3"
)

func TestMarshalFloatWholeNumber(t *testing.T) {
	out, err := Marshal(float64(99.0))
	assert.NoError(t, err)
	assert.Contains(t, string(out), "99.0")
}

func TestMarshalFloatFractional(t *testing.T) {
	out, err := Marshal(float64(97.5))
	assert.NoError(t, err)
	assert.Contains(t, string(out), "97.5")
}

func TestMarshalFloatZero(t *testing.T) {
	out, err := Marshal(float64(0.0))
	assert.NoError(t, err)
	assert.Contains(t, string(out), "0.0")
}

func TestMarshalFloatNegativeWhole(t *testing.T) {
	out, err := Marshal(float64(-5.0))
	assert.NoError(t, err)
	assert.Contains(t, string(out), "-5.0")
}

func TestMarshalFloat32(t *testing.T) {
	out, err := Marshal(float32(1.0))
	assert.NoError(t, err)
	assert.Contains(t, string(out), "1.0")
}

func TestMarshalFloatNaN(t *testing.T) {
	out, err := Marshal(math.NaN())
	assert.NoError(t, err)
	assert.Contains(t, string(out), ".nan")
}

func TestMarshalFloatInf(t *testing.T) {
	out, err := Marshal(math.Inf(1))
	assert.NoError(t, err)
	assert.Contains(t, string(out), ".inf")
}

func TestMarshalFloatNegInf(t *testing.T) {
	out, err := Marshal(math.Inf(-1))
	assert.NoError(t, err)
	assert.Contains(t, string(out), "-.inf")
}

func TestMarshalNil(t *testing.T) {
	out, err := Marshal(nil)
	assert.NoError(t, err)
	assert.Contains(t, string(out), "null")
}

func TestMarshalBool(t *testing.T) {
	out, err := Marshal(true)
	assert.NoError(t, err)
	assert.Contains(t, string(out), "true")
}

func TestMarshalString(t *testing.T) {
	out, err := Marshal("hello")
	assert.NoError(t, err)
	assert.Contains(t, string(out), "hello")
}

func TestMarshalInt(t *testing.T) {
	out, err := Marshal(42)
	assert.NoError(t, err)
	assert.Contains(t, string(out), "42")
}

func TestMarshalInt64(t *testing.T) {
	out, err := Marshal(int64(-100))
	assert.NoError(t, err)
	assert.Contains(t, string(out), "-100")
}

func TestMarshalUint64(t *testing.T) {
	out, err := Marshal(uint64(999))
	assert.NoError(t, err)
	assert.Contains(t, string(out), "999")
}

func TestMarshalSlice(t *testing.T) {
	out, err := Marshal([]any{"a", float64(1.0), nil})
	assert.NoError(t, err)
	s := string(out)
	assert.Contains(t, s, "- a")
	assert.Contains(t, s, "- 1.0")
	assert.Contains(t, s, "- null")
}

func TestMarshalStringMap(t *testing.T) {
	out, err := Marshal(map[string]any{
		"name":  "test",
		"value": float64(42.0),
	})
	assert.NoError(t, err)
	s := string(out)
	assert.Contains(t, s, "name: test")
	assert.Contains(t, s, "value: 42.0")
}

func TestMarshalAnyMap(t *testing.T) {
	out, err := Marshal(map[any]any{
		"key": float64(10.0),
	})
	assert.NoError(t, err)
	s := string(out)
	assert.Contains(t, s, "key: 10.0")
}

func TestMarshalMapKeysAreSorted(t *testing.T) {
	out, err := Marshal(map[string]any{
		"z": 1,
		"a": 2,
		"m": 3,
	})
	assert.NoError(t, err)
	s := string(out)
	aPos := indexOf(s, "a:")
	mPos := indexOf(s, "m:")
	zPos := indexOf(s, "z:")
	assert.Equals(t, aPos < mPos, true)
	assert.Equals(t, mPos < zPos, true)
}

func TestMarshalNestedStructure(t *testing.T) {
	data := map[string]any{
		"output_data": map[string]any{
			"percentiles": []any{float64(99.0), float64(95.0), float64(97.5)},
		},
	}
	out, err := Marshal(data)
	assert.NoError(t, err)
	s := string(out)
	assert.Contains(t, s, "99.0")
	assert.Contains(t, s, "95.0")
	assert.Contains(t, s, "97.5")
}

func TestMarshalRoundTripPreservesFloatTypes(t *testing.T) {
	data := map[string]any{
		"values": []any{float64(99.0), float64(95.0), float64(97.5)},
	}
	out, err := Marshal(data)
	assert.NoError(t, err)

	var result map[string]any
	assert.NoError(t, goyaml.Unmarshal(out, &result))

	values := result["values"].([]any)
	for i, v := range values {
		_, ok := v.(float64)
		if !ok {
			t.Errorf("values[%d]: expected float64, got %T (value: %v)", i, v, v)
		}
	}
}

func TestMarshalEmptySlice(t *testing.T) {
	out, err := Marshal([]any{})
	assert.NoError(t, err)
	assert.Contains(t, string(out), "[]")
}

func TestMarshalEmptyMap(t *testing.T) {
	out, err := Marshal(map[string]any{})
	assert.NoError(t, err)
	assert.Contains(t, string(out), "{}")
}

func TestMarshalUnsupportedType(t *testing.T) {
	_, err := Marshal(struct{}{})
	assert.Error(t, err)
}

func indexOf(s, substr string) int {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return i
		}
	}
	return -1
}
