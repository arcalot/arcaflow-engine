// Package loadfile provides functions to load files from a directory.
package loadfile

import (
	"fmt"
	"os"
	"path/filepath"
)

// LoadContext reads the contents at each file into a map where the key
// is the absolute filepath and file contents is the value.
func LoadContext(neededFilepaths []string) (map[string][]byte, error) {
	result := map[string][]byte{}
	var err error
	for _, filePath := range neededFilepaths {
		absPath, err := filepath.Abs(filePath)
		if err != nil {
			return nil, fmt.Errorf("failed to obtain absolute path of file %s (%w)", filepath.Base(filePath), err)
		}
		fileData, err := os.ReadFile(absPath) //nolint:gosec
		if err != nil {
			return nil, fmt.Errorf("failed to read file from context directory: %s (%w)", absPath, err)
		}
		result[absPath] = fileData
	}
	return result, err
}
