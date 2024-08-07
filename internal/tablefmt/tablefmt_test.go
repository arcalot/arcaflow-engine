package tablefmt_test

import (
	"go.arcalot.io/assert"
	"go.flow.arcalot.io/engine/internal/tablefmt"
	"sort"
	"testing"
)

func TestUnnestLongerSortedHappy(t *testing.T) {
	astromech := []string{"q7", "bb", "r2", "r4"}
	protocol := []string{"3po", "000", "chatty"}
	battle := []string{"b1", "ig"}
	probe := []string{"cobra"}
	astromechGroup := "astromech"
	protocolGroup := "protocol"
	battleGroup := "battle"
	probeGroup := "probe"
	astromechSorted := make([]string, len(astromech))
	protocolSorted := make([]string, len(protocol))
	battleSorted := make([]string, len(battle))
	probeSorted := make([]string, len(probe))
	sort.Strings(astromechSorted)
	sort.Strings(battleSorted)
	sort.Strings(probeSorted)
	sort.Strings(protocolSorted)
	expOut := [][]string{
		{"astromech", "bb"},
		{"astromech", "q7"},
		{"astromech", "r2"},
		{"astromech", "r4"},
		{"battle", "b1"},
		{"battle", "ig"},
		{"probe", "cobra"},
		{"protocol", "000"},
		{"protocol", "3po"},
		{"protocol", "chatty"},
	}
	input := map[string][]string{
		protocolGroup:  protocol,
		astromechGroup: astromech,
		battleGroup:    battle,
		probeGroup:     probe,
	}
	assert.Equals(t, tablefmt.UnnestLongerSorted(input), expOut)
}

func TestUnnestLongerSortedEmpty(t *testing.T) {
	input := map[string][]string{}
	assert.Equals(t, tablefmt.UnnestLongerSorted(input), [][]string{})
}

func TestUnnestLongerSortedEmptyGroup(t *testing.T) {
	input := map[string][]string{
		"G": {"g"},
		"D": {},
	}
	expOut := [][]string{
		{"G", "g"},
	}
	assert.Equals(t, tablefmt.UnnestLongerSorted(input), expOut)
}

func TestSwapColumnsHappy(t *testing.T) {
	testData := map[string]struct {
		input    [][]string
		expected [][]string
	}{
		"happy": {
			input: [][]string{{"fruit", "tomato"},
				{"fruit", "cucumber"},
				{"veggie", "spinach"},
				{"veggie", "carrot"},
			},
			expected: [][]string{
				{"tomato", "fruit"},
				{"cucumber", "fruit"},
				{"spinach", "veggie"},
				{"carrot", "veggie"},
			},
		},
		"row_with_zero_len": {
			input: [][]string{
				{"a", "b"},
				{},
				{"a", "c"},
			},
			expected: [][]string{
				{"b", "a"},
				{},
				{"c", "a"},
			},
		},
		"row_with_one_len": {
			input: [][]string{
				{"a", "b"},
				{"z"},
				{"a", "c"},
			},
			expected: [][]string{
				{"b", "a"},
				{"z"},
				{"c", "a"},
			},
		},
		"row_with_three_len": {
			input: [][]string{
				{"a", "b"},
				{"x", "y", "z"},
				{"a", "c"},
			},
			expected: [][]string{
				{"b", "a"},
				{"x", "y", "z"},
				{"c", "a"},
			},
		},
	}
	for _, tc := range testData {
		assert.Equals(t, tablefmt.SwapColumns(tc.input), tc.expected)
	}
}
