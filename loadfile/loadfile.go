// Package loadfile provides functions to load files from a directory.
package loadfile

import (
	"fmt"
	"os"
	"path/filepath"
)

type ContextFile struct {
	ID           string
	AbsolutePath string
	Content      []byte
}

type FileCache struct {
	RootDir string
	Files   map[string]ContextFile
}

// NewFileCache creates a mapping of files to their absolute paths and
// file contents. rootDir is the context directory (root directory) in
// which the files can be found. 'files' is a mapping of the desired file
// key to a relative or absolute file path.
func NewFileCache(rootDir string, files map[string]string) (*FileCache, error) {
	absDir, err := filepath.Abs(rootDir)
	if err != nil {
		return nil, fmt.Errorf("error determining context directory absolute path %s (%w)", rootDir, err)
	}
	filesAbsPaths := map[string]ContextFile{}
	for key, f := range files {
		abspath := f
		if !filepath.IsAbs(f) {
			abspath = filepath.Join(absDir, f)
		}
		filesAbsPaths[key] = ContextFile{
			ID:           f,
			AbsolutePath: abspath,
		}
	}
	return &FileCache{
		RootDir: absDir, Files: filesAbsPaths}, nil
}

// LoadContext reads the content of each context file into Content.
func (fc *FileCache) LoadContext() error {
	result := map[string]ContextFile{}
	var err error
	for key, cf := range fc.Files {
		absPath := cf.AbsolutePath
		fileData, err := os.ReadFile(filepath.Clean(absPath))
		if err != nil {
			return fmt.Errorf("error reading file %s (%w)", absPath, err)
		}
		result[key] = ContextFile{
			ID:           cf.ID,
			AbsolutePath: cf.AbsolutePath,
			Content:      fileData,
		}
	}
	fc.Files = result
	return err
}

// GetByKey asks if the file cache contains the given fileKey.
func (fc *FileCache) GetByKey(fileKey string) (*ContextFile, bool) {
	cf, ok := fc.Files[fileKey]
	return &cf, ok
}

// AbsPathByKey returns the absolute file path of a given file key,
// if it exists in the file cache, nil otherwise.
func (fc *FileCache) AbsPathByKey(fileKey string) *string {
	cf, ok := fc.GetByKey(fileKey)
	if !ok {
		return nil
	}
	return &cf.AbsolutePath
}

// ContentByKey returns the file content of a given file key, if it
// exists in the file cache, nil otherwise.
func (fc *FileCache) ContentByKey(fileKey string) []byte {
	cf, ok := fc.GetByKey(fileKey)
	if !ok {
		return nil
	}
	return cf.Content
}

// Contents return a mapping of the file cache's file keys to file Content.
func (fc *FileCache) Contents() map[string][]byte {
	result := map[string][]byte{}
	for key, f := range fc.Files {
		result[key] = f.Content
	}
	return result
}

// MergeFileCaches merges any number of file caches into one file cache.
// The new root directory will be the root directory of the last file cache
// argument. File keys found later in iteration will overwrite previously
// file keys, if there is a name clash.
func MergeFileCaches(fileCaches ...FileCache) *FileCache {
	cache := map[string]ContextFile{}
	rootDir := ""
	for _, fc := range fileCaches {
		for key, contextFile := range fc.Files {
			cache[key] = contextFile
		}
		rootDir = fc.RootDir
	}
	return &FileCache{
		RootDir: rootDir,
		Files:   cache,
	}
}

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
