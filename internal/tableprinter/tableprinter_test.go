package tableprinter_test

import (
	"bytes"
	"fmt"
	"go.flow.arcalot.io/engine/internal/tableprinter"
	"testing"
)

func TestPrintTwoColumnTable(t *testing.T) {
	buf := bytes.NewBuffer(nil)
	headers := []string{"function", "model"}
	rows := [][]string{
		{"a", "1"},
		{"b", "2"},
		{"c", "3"},
	}
	tableprinter.PrintTwoColumnTable(buf, headers, rows)
	fmt.Printf("%s\n", buf.String())
}
