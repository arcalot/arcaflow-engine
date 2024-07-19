package util

import (
	"fmt"
	"go.arcalot.io/log/v2"
	"go.flow.arcalot.io/pluginsdk/schema"
	"io"
	"strings"
	"text/tabwriter"
)

const (
	tabwriterMinWidth = 6
	tabwriterWidth    = 4
	tabwriterPadding  = 3
	tabwriterPadChar  = ' '
	tabwriterFlags    = tabwriter.FilterHTML
)

// NewTabWriter returns a tabwriter that transforms tabbed columns into aligned
// text.
func NewTabWriter(output io.Writer) *tabwriter.Writer {
	return tabwriter.NewWriter(output, tabwriterMinWidth, tabwriterWidth, tabwriterPadding, tabwriterPadChar, tabwriterFlags)
}

func PrintTwoColumnTable(output io.Writer, headers []string, rows [][]string) {
	w := NewTabWriter(output)

	// write column headers
	for _, col := range headers {
		_, _ = fmt.Fprint(w, strings.ToUpper(col), "\t")
	}
	_, _ = fmt.Fprintln(w)

	// write rows
	for _, row := range rows {
		_, _ = fmt.Fprintln(w, row[0], "\t", row[1])
	}

	_ = w.Flush()
}

func PrintNamespaceResponse(output io.Writer, allNamespaces map[string]map[string]*schema.ObjectSchema, logger log.Logger) {
	if len(allNamespaces) == 0 {
		logger.Warningf("No namespaces found in workflow")
		return
	}
	groupLists := ExtractGroupedLists[*schema.ObjectSchema](allNamespaces)
	df := UnnestLongerSorted(groupLists)
	df = SwapColumns(df)
	PrintTwoColumnTable(output, []string{"object", "namespace"}, df)
}
