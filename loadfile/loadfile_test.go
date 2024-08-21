package loadfile_test

import (
	"go.arcalot.io/assert"
	"go.flow.arcalot.io/engine/loadfile"
	"log"
	"os"
	"path/filepath"
	"testing"
)

// This tests the functional behavior of LoadContext when it is given
// a directory of files that cover more than the directory file node
// type 'd' (as seen in *nix systems). Specifically, this test adds
// symbolic link files for a regular file and a directory file. The
// engine should only attempt to read files of a type that the os can
// read (i.e. not throw an error on a call to os.ReadFile()), and
// disregard (not throw an error) files with a type it cannot read.
func Test_LoadContext(t *testing.T) {
	testdir := filepath.Join(TestDir, "load-ctx")
	assert.NoError(t, os.MkdirAll(testdir, os.ModePerm))

	// create a directory
	dirPrefix := "mydir"
	dirPath, err := os.MkdirTemp(testdir, dirPrefix+"*")

	// create a file
	filenamePrefix := "myfile"
	f, err := os.CreateTemp(dirPath, filenamePrefix+"*")
	assert.NoError(t, err)
	filePath := f.Name()
	assert.NoError(t, f.Close())

	// create symlink to the directory
	symlinkDirname := dirPath + "_sym"
	assert.NoError(t, os.Symlink(dirPath, symlinkDirname))

	// create symlink to the file
	symlinkFilepath := filePath + "_sym"
	assert.NoError(t, os.Symlink(filePath, symlinkFilepath))

	neededFiles := map[string]string{
		filePath:        filePath,
		symlinkFilepath: symlinkFilepath,
	}
	fc, err := loadfile.NewFileCacheUsingContext(testdir, neededFiles)
	// assert no error on attempting to read files
	// that cannot be read
	assert.NoError(t, err)
	err = fc.LoadContext()
	assert.NoError(t, err)

	// assert only the regular and symlinked file are loaded
	filemapExp := map[string][]byte{
		filePath:        {},
		symlinkFilepath: {},
	}
	assert.Equals(t, fc.Contents(), filemapExp)

	errFileRead := "reading file"
	// error on loading a directory
	neededFiles = map[string]string{
		dirPath: dirPath,
	}
	fc, err = loadfile.NewFileCacheUsingContext(testdir, neededFiles)
	assert.NoError(t, err)
	err = fc.LoadContext()
	assert.Error(t, err)
	assert.Contains(t, err.Error(), errFileRead)

	// error on loading a symlink directory
	neededFiles = map[string]string{
		symlinkDirname: symlinkDirname,
	}
	fc, err = loadfile.NewFileCacheUsingContext(testdir, neededFiles)
	assert.NoError(t, err)
	err = fc.LoadContext()
	assert.Error(t, err)
	assert.Contains(t, err.Error(), errFileRead)
}

// This tests the construction of a new file cache, and the
// determination of the absolute file paths. The file cache
// joins relative paths with the context (root) directory,
// and passes through absolute paths unmodified.
func Test_NewFileCache(t *testing.T) {
	testdir, err := os.MkdirTemp(TestDir, "")
	assert.NoError(t, err)

	testFilepaths := map[string]string{
		"a": "a.yaml",
		"b": "/b.toml",
		"c": "rel/../subdir/c.txt",
	}

	absPathsExp := map[string]string{
		"a": filepath.Join(testdir, testFilepaths["a"]),
		"b": testFilepaths["b"],
		"c": filepath.Join(testdir, testFilepaths["c"]),
	}

	fc, err := loadfile.NewFileCacheUsingContext(testdir, testFilepaths)
	assert.NoError(t, err)
	absPathsGot := FileCacheAbsPaths(fc)
	assert.Equals(t, absPathsExp, absPathsGot)
	// test file key not in file cache returns nil
	_, err = fc.AbsPathByKey("")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "file cache does not contain")
}

// This tests that the merge file cache combines the contents
// of two file caches appropriately, where non-unique file keys
// are overwritten by a later file cache argument.
func Test_MergeFileCaches(t *testing.T) {
	content := []byte(`content`)
	replacedContent := []byte(`replaced content`)
	content1 := []byte(`content1`)
	content2 := []byte(`content2`)
	commonName := "robot"
	filename1 := "b"
	filename2 := "c"

	files1 := map[string][]byte{
		filename1:  content1,
		commonName: content,
	}
	expRootDir := "1"
	fc1 := loadfile.NewFileCache(expRootDir, files1)

	files2 := map[string][]byte{
		commonName: replacedContent,
		filename2:  content2,
	}

	fc2 := loadfile.NewFileCache(expRootDir, files2)

	expMergedFiles := map[string]loadfile.ContextFile{
		filename1: {
			ID:           filename1,
			AbsolutePath: filename1,
			Content:      content1,
		},
		filename2: {
			ID:           filename2,
			AbsolutePath: filename2,
			Content:      content2,
		},
		commonName: {
			ID:           commonName,
			AbsolutePath: commonName,
			Content:      replacedContent,
		},
	}

	fcMerged, err := loadfile.MergeFileCaches(fc1, fc2)
	assert.NoError(t, err)
	assert.Equals(t, fcMerged.RootDir(), expRootDir)
	assert.Equals(t, fcMerged.Files(), expMergedFiles)

	newRootDir := "3"
	filename3 := "d"
	content3 := []byte(`content3`)

	files3 := map[string][]byte{
		filename3: content3,
	}

	fc3 := loadfile.NewFileCache(newRootDir, files3)
	fcMerged2, err := loadfile.MergeFileCaches(fcMerged, fc3)
	assert.Error(t, err)
	assert.Nil(t, fcMerged2)

	// nil file caches should not be merged
	var fcNil loadfile.FileCache
	fcMerged3, err := loadfile.MergeFileCaches(fcMerged, fcNil, fcNil)
	assert.NoError(t, err)
	assert.Equals(t, fcMerged3.RootDir(), expRootDir)
	assert.Equals(t, fcMerged3.Files(), expMergedFiles)
}

func FileCacheAbsPaths(fc loadfile.FileCache) map[string]string {
	result := map[string]string{}
	for key, f := range fc.Files() {
		result[key] = f.AbsolutePath
	}
	return result
}

var TestDir = filepath.Join(os.TempDir(), "loadfile-tests")

func TestMain(m *testing.M) {
	// cleanup directory even if it's there
	_ = os.RemoveAll(TestDir)
	err := os.MkdirAll(TestDir, os.ModePerm)
	if err != nil {
		log.Fatalf("failed to make directory %s %v", TestDir, err)
	}
	exitCode := m.Run()
	_ = os.RemoveAll(TestDir)
	os.Exit(exitCode)
}
