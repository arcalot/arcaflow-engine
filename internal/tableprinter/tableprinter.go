// Package tableprinter provides behavior to write tabular data to a given
// destination.
package tableprinter

import (
	"fmt"
	"go.arcalot.io/log/v2"
	"go.flow.arcalot.io/engine/internal/tidy"
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

// PrintTwoColumnTable writes a two column table with headers to a given
// output destination.
func PrintTwoColumnTable(output io.Writer, headers []string, rows [][]string) {
	w := NewTabWriter(output)

	// column headers are at the top, so they are written first
	for _, col := range headers {
		_, _ = fmt.Fprint(w, strings.ToUpper(col), "\t")
	}
	_, _ = fmt.Fprintln(w)

	// rows form the body of the table
	for _, row := range rows {
		_, _ = fmt.Fprintln(w, row[0], "\t", row[1])
	}

	_ = w.Flush()
}

// PrintNamespaceResponse constructs and writes a table of workflow Objects and
// their namespaces to the given output destination.
func PrintNamespaceResponse(output io.Writer, allNamespaces map[string]map[string]*schema.ObjectSchema, logger log.Logger) {
	if len(allNamespaces) == 0 {
		logger.Warningf("No namespaces found in workflow")
		return
	}
	groupLists := tidy.ExtractGroupedLists[*schema.ObjectSchema](allNamespaces)
	df := tidy.UnnestLongerSorted(groupLists)
	df = tidy.SwapColumns(df)
	PrintTwoColumnTable(output, []string{"object", "namespace"}, df)
}
