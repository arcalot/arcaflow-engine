package util

import (
	"encoding/json"

	"go.arcalot.io/lang"
)

// JSONEncode encodes a value as JSON or panics.
func JSONEncode(value any) string {
	return string(lang.Must2(json.Marshal(value)))
}
