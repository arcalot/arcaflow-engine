package util_test

import (
	"fmt"
	"go.flow.arcalot.io/engine/internal/util"
	"testing"
)

func TestUnnestLonger(t *testing.T) {
	//astromech := []string{"q7", "bb", "r2", "r4"}
	//protocol := []string{"c3po", "000", "hk47", "talky"}
	//battle := []string{"b1", "b2", "bx", "ig"}
	//robots := map[string][]string{
	//	"astromech": astromech,
	//	"protocol":  protocol,
	//	"battle":    battle,
	//}
	//slices.Sort(astromech)
	//fmt.Printf("%v\n", astromech)
	//robots2 := [][]string{
	//	{"astromech", "q7"},
	//	{"astromech", "r2"},
	//	{"astromech", "bb"},
	//	{"protocol", "c3po"},
	//	{"protocol", "000"},
	//	{"protocol", "hk47"},
	//	{"battle", "b2"},
	//	{"battle", "b1"},
	//	{"battle", "ig"},
	//}
	//slices.Sort(robots2)
	//
	//fmt.Printf("%v\n", robots)
	groupA := []string{"a", "a", "a"}
	groupB := []string{"b", "b"}
	groupC := []string{"c"}
	df0 := map[string][]string{
		"B": groupB, "C": groupC, "A": groupA,
	}
	fmt.Printf("%v\n", util.SwapColumns(util.UnnestLongerSorted(df0)))

}
