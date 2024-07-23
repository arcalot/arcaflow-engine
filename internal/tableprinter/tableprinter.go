// Package tableprinter provides behavior to write tabular data to a given
// destination.
package tableprinter

import (
	"fmt"
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

// newTabWriter returns a tabwriter that transforms tabbed columns into aligned
// text.
func newTabWriter(output io.Writer) *tabwriter.Writer {
	return tabwriter.NewWriter(output, tabwriterMinWidth, tabwriterWidth, tabwriterPadding, tabwriterPadChar, tabwriterFlags)
}

// PrintTwoColumnTable uses a list of two item records (rows) to write a two
// column table with headers to a given output destination.
func PrintTwoColumnTable(output io.Writer, headers []string, rows [][]string) {
	w := newTabWriter(output)

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
