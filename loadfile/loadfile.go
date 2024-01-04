// Package loadfile provides functions to load files from a directory.
package loadfile

import (
	"fmt"
	"os"
	"path/filepath"
)

// LoadContext reads the content of each file into a map where the key
// is the absolute filepath and the file content is the value.
func LoadContext(neededFilepaths []string) (map[string][]byte, error) {
	result := map[string][]byte{}
	var err error
	for _, filePath := range neededFilepaths {
		absPath, err := filepath.Abs(filePath)
		if err != nil {
			return nil, fmt.Errorf("error obtaining absolute path of file %s (%w)",
				filePath, err)
		}
		fileData, err := os.ReadFile(filepath.Clean(absPath))
		if err != nil {
			return nil, fmt.Errorf("error reading file %s (%w)", absPath, err)
		}
		result[absPath] = fileData
	}
	return result, err
}

// AbsPathsWithContext creates a map of absolute filepaths. If a required
// file is not provided with an absolute path, then it is joined with the
// root directory.
func AbsPathsWithContext(rootDir string, requiredFiles map[string]string) (map[string]string, error) {
	absDir, err := filepath.Abs(rootDir)
	if err != nil {
		return nil, fmt.Errorf("error determining context directory absolute path %s (%w)", rootDir, err)
	}
	requiredFilesAbs := map[string]string{}
	for key, f := range requiredFiles {
		abspath := f
		if !filepath.IsAbs(f) {
			abspath = filepath.Join(absDir, f)
		}
		requiredFilesAbs[key] = abspath
	}
	return requiredFilesAbs, nil
}
