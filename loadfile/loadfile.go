// Package loadfile provides functions to load files from a directory.
package loadfile

import (
	"fmt"
	"os"
	"path/filepath"
)

// ContextFile is a file whose absolute file path and content
// need to be referenced at some point during execution.
type ContextFile struct {
	ID           string
	AbsolutePath string
	Content      []byte
}

type fileCache struct {
	// Root directory for given relative filepaths
	rootDir string
	// Mapping of unique keys that reference a file needed for execution
	files map[string]ContextFile
}

// FileCache is a container of ContextFiles, and a context (root)
// directory path to be used in conjunction with files that do not
// provide an absolute file path.
type FileCache interface {
	RootDir() string
	LoadContext() error
	GetByKey(fileKey string) (*ContextFile, error)
	AbsPathByKey(fileKey string) (string, error)
	ContentByKey(fileKey string) ([]byte, error)
	Contents() map[string][]byte
	Files() map[string]ContextFile
}

// NewFileCacheUsingContext returns a FileCache object containing the provided root directory string and a
// new map based on the provided map. The new map uses the same keys as the input but their corresponding
// values contain absolute file paths. Relative file paths provided in the input map are prefixed with the root
// directory before being included in the output map.
func NewFileCacheUsingContext(rootDir string, files map[string]string) (FileCache, error) {
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
	return &fileCache{
		rootDir: absDir, files: filesAbsPaths}, nil
}

// NewFileCache returns a FileCache object containing provided
// root directory string and a new map based on the provided
// map. The new map uses the same keys as the input and the
// corresponding values include the contents from the input;
// the key value is also used as the ID and the absolute path for
// the entries.
func NewFileCache(rootDir string, fileContents map[string][]byte) FileCache {
	files := map[string]ContextFile{}
	for key, content := range fileContents {
		files[key] = ContextFile{
			ID:           key,
			AbsolutePath: key,
			Content:      content,
		}
	}
	return &fileCache{
		rootDir: rootDir,
		files:   files,
	}
}

// LoadContext reads the content of each context file into Content.
func (fc *fileCache) LoadContext() error {
	result := map[string]ContextFile{}
	var err error
	for key, cf := range fc.files {
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
	fc.files = result
	return err
}

// RootDir returns the root directory used by files with
// relative file paths.
func (fc *fileCache) RootDir() string {
	return fc.rootDir
}

// GetByKey asks if the file cache contains the given fileKey.
func (fc *fileCache) GetByKey(fileKey string) (*ContextFile, error) {
	cf, ok := fc.files[fileKey]
	if !ok {
		return nil, fmt.Errorf("file cache does not contain %q", fileKey)
	}
	return &cf, nil
}

// AbsPathByKey returns the absolute file path of a given file key,
// if it exists in the file cache, nil otherwise.
func (fc *fileCache) AbsPathByKey(fileKey string) (string, error) {
	cf, err := fc.GetByKey(fileKey)
	if err != nil {
		return "", err
	}
	return cf.AbsolutePath, nil
}

// ContentByKey returns the file content of a given file key, if it
// exists in the file cache, nil otherwise.
func (fc *fileCache) ContentByKey(fileKey string) ([]byte, error) {
	cf, err := fc.GetByKey(fileKey)
	if err != nil {
		return nil, err
	}
	return cf.Content, nil
}

// Contents returns a mapping of the file cache's file keys to file Content.
func (fc *fileCache) Contents() map[string][]byte {
	result := map[string][]byte{}
	for key, f := range fc.files {
		result[key] = f.Content
	}
	return result
}

// Files returns the mapping of file keys to their ContextFile.
func (fc *fileCache) Files() map[string]ContextFile {
	return fc.files
}

// MergeFileCaches merges any number of file caches into one file cache.
// The new root directory will be the root directory of the last file cache
// argument. File keys found later in iteration will overwrite previously
// file keys, if there is a name clash.
func MergeFileCaches(fileCaches ...FileCache) (FileCache, error) {
	cache := map[string]ContextFile{}
	rootDir := ""

	for _, fc := range fileCaches {
		for key, contextFile := range fc.Files() {
			cache[key] = contextFile
		}
		if rootDir != "" && rootDir != fc.RootDir() {
			return nil, fmt.Errorf("file caches have different root directory %q, %q", rootDir, fc.RootDir())
		}
		rootDir = fc.RootDir()
	}
	return &fileCache{
		rootDir: rootDir,
		files:   cache,
	}, nil
}
