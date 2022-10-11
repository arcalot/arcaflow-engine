package util

import (
	"encoding/base64"

	"go.arcalot.io/lang"
)

// Base64Decode ensures a value is base64 decoded into a string or panics.
func Base64Decode(value string) string {
	return string(lang.Must2(base64.StdEncoding.DecodeString(value)))
}
