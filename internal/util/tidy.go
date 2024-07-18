package util

import "sort"

func UnnestLongerSorted(twoColDf map[string][]string) [][]string {
	df := [][]string{}
	groupNames := []string{}
	for name, _ := range twoColDf {
		groupNames = append(groupNames, name)
	}
	sort.Strings(groupNames)
	for _, name := range groupNames {
		groupRows := twoColDf[name]
		sort.Strings(groupRows)
		for _, rowValue := range groupRows {
			df = append(df, []string{name, rowValue})
		}
	}
	return df
}

func SwapColumns(df [][]string) [][]string {
	for k := range df {
		if len(df[k]) == 2 {
			df[k][0], df[k][1] = df[k][1], df[k][0]
		}
	}
	return df
}
