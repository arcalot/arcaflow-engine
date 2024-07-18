package util_test

import (
	"go.arcalot.io/assert"
	"go.flow.arcalot.io/engine/internal/util"
	"sort"
	"testing"
)

func TestUnnestLongerHappy(t *testing.T) {
	astromech := []string{"q7", "bb", "r2", "r4"}
	protocol := []string{"c3po", "000", "talky"}
	battle := []string{"b1", "ig"}
	probe := []string{"viper"}
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
		{"probe", "viper"},
		{"protocol", "000"},
		{"protocol", "c3po"},
		{"protocol", "talky"},
	}
	input := map[string][]string{
		protocolGroup:  protocol,
		astromechGroup: astromech,
		battleGroup:    battle,
		probeGroup:     probe,
	}
	assert.Equals(t, util.UnnestLongerSorted(input), expOut)
}
