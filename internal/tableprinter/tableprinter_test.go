package tableprinter_test

import (
	"bytes"
	"go.arcalot.io/assert"
	"go.flow.arcalot.io/engine/internal/tableprinter"
	"testing"
)

const basicTwoColTable = `FUNCTION   MODEL   
a           1
b           2
c           3
`

func TestPrintTwoColumnTable(t *testing.T) {
	buf := bytes.NewBuffer(nil)
	headers := []string{"function", "model"}
	rows := [][]string{
		{"a", "1"},
		{"b", "2"},
		{"c", "3"},
	}
	tableprinter.PrintTwoColumnTable(buf, headers, rows)
	assert.Equals(t, buf.String(), basicTwoColTable)
}
