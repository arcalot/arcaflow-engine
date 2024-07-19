package util

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

// NewTabWriter returns a tabwriter that transforms tabbed columns into aligned
// text.
func NewTabWriter(output io.Writer) *tabwriter.Writer {
	return tabwriter.NewWriter(output, tabwriterMinWidth, tabwriterWidth, tabwriterPadding, tabwriterPadChar, tabwriterFlags)
}

func PrintTwoColumnTable(output io.Writer, headers []string, rows [][]string) {
	w := NewTabWriter(output)

	// write header
	for _, col := range headers {
		_, _ = fmt.Fprint(w, strings.ToUpper(col), "\t")
	}
	_, _ = fmt.Fprintln(w)

	// write each row
	for _, row := range rows {
		_, _ = fmt.Fprintln(w, row[0], "\t", row[1])
	}

	_ = w.Flush()
}
