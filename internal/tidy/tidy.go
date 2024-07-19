// Package tidy provides functions to create tabular data where
//  1. Each variable is a column; each column is a variable.
//  2. Each observation is a row; each row is an observation.
//  3. Each value is a cell; each cell is a single value.
//
// Its behavior is inspired by the R package tidyr.
// https://tidyr.tidyverse.org/index.html
package tidy

import "sort"

// UnnestLongerSorted turns each element of a list-group
// into a row. Each key in the map represents a group and
// each group is associated with a list of values.
func UnnestLongerSorted(twoColDf map[string][]string) [][]string {
	df := [][]string{}
	groupNames := []string{}
	for name := range twoColDf {
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

// SwapColumns swaps the row values between the first and second column, if
// the row has a length of two.
func SwapColumns(df [][]string) [][]string {
	for k := range df {
		if len(df[k]) == 2 {
			df[k][0], df[k][1] = df[k][1], df[k][0]
		}
	}
	return df
}

// ExtractGroupedLists transforms a map of maps into a map of strings. The keys
// in the nested map become a list of values.
func ExtractGroupedLists[T any](data map[string]map[string]T) map[string][]string {
	groupLists := map[string][]string{}
	for namespace, objects := range data {
		for objName := range objects {
			groupLists[namespace] = append(groupLists[namespace], objName)
		}
	}
	return groupLists
}
