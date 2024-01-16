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

type FileContext struct {
	RootDir string
	Files   map[string]ContextFile
	Content map[string][]byte
}

func NewFileContext(rootDir string, files map[string]string) (*FileContext, error) {
	absDir, err := filepath.Abs(rootDir)
	if err != nil {
		return nil, fmt.Errorf("error determining context directory absolute path %s (%w)", rootDir, err)
	}
	requiredFilesAbs := map[string]ContextFile{}
	for key, f := range files {
		abspath := f
		if !filepath.IsAbs(f) {
			abspath = filepath.Join(absDir, f)
		}
		requiredFilesAbs[key] = ContextFile{
			ID:           f,
			AbsolutePath: abspath,
		}
	}
	return &FileContext{
		RootDir: absDir, Files: requiredFilesAbs}, nil
}

// LoadContext reads the content of each file into a map where the key
// is the absolute filepath and the file content is the value.
func (fc *FileContext) LoadContext() error {
	result := map[string][]byte{}
	var err error
	for key, cf := range fc.Files {
		absPath := cf.AbsolutePath
		fileData, err := os.ReadFile(filepath.Clean(absPath))
		if err != nil {
			return fmt.Errorf("error reading file %s (%w)", absPath, err)
		}
		//result[key] = ContextFile{
		//	ID:           cf.ID,
		//	AbsolutePath: cf.AbsolutePath,
		//	Content:      fileData,
		//}
		result[key] = fileData
	}
	fc.Content = result
	return err
}

func (fc *FileContext) GetByID(fileID string) *ContextFile {
	for _, cf := range fc.Files {
		if cf.ID == fileID {
			return &cf
		}
	}
	return nil
}

func (fc *FileContext) GetByKey(fileKey string) *ContextFile {
	cf := fc.Files[fileKey]
	return &cf
}

func (fc *FileContext) AbsPathByKey(fileKey string) *string {
	cf := fc.GetByKey(fileKey)
	return &cf.AbsolutePath
}

func (fc *FileContext) ContentByKey(fileKey string) []byte {
	cf := fc.GetByKey(fileKey)
	return cf.Content
}

func (fc *FileContext) ContentByID(fileID string) []byte {
	cf := fc.GetByID(fileID)
	return cf.Content
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
