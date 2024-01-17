package loadfile_test

import (
	"go.arcalot.io/assert"
	"go.flow.arcalot.io/engine/loadfile"
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
	testdir := "/tmp/loadfile-test"
	assert.NoError(t, os.MkdirAll(testdir, os.ModePerm))
	t.Cleanup(func() {
		assert.NoError(t, os.RemoveAll(testdir))
	})

	// create a directory
	dirname := "mydir"
	dirpath := filepath.Join(testdir, dirname)
	assert.NoError(t, os.MkdirAll(dirpath, os.ModePerm))

	// create a file
	filename := "myfile"
	filePath := filepath.Join(testdir, filename)
	f, err := os.Create(filepath.Clean(filePath))
	assert.NoError(t, err)
	assert.NoError(t, f.Close())

	// create symlink to the directory
	symlinkDirname := dirname + "_sym"
	symlinkDirpath := filepath.Join(testdir, symlinkDirname)
	assert.NoError(t, os.Symlink(dirpath, symlinkDirpath))

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

	// error on loading a directory
	neededFiles = map[string]string{
		dirpath: dirpath,
	}

	errFileRead := "reading file"
	fc, err = loadfile.NewFileCacheUsingContext(testdir, neededFiles)
	assert.NoError(t, err)
	err = fc.LoadContext()
	assert.Error(t, err)
	assert.Contains(t, err.Error(), errFileRead)
	assert.Nil(t, fc.ContentByKey(dirpath))

	// error on loading a symlink directory
	neededFiles = map[string]string{
		symlinkDirpath: symlinkDirpath,
	}
	fc, err = loadfile.NewFileCacheUsingContext(testdir, neededFiles)
	assert.NoError(t, err)
	err = fc.LoadContext()
	assert.Error(t, err)
	assert.Contains(t, err.Error(), errFileRead)
	assert.Nil(t, fc.ContentByKey(symlinkDirpath))
}

// This tests the construction of a new file cache, and the
// determination of the absolute file paths. The file cache
// joins relative paths with the context (root) directory,
// and passes through absolute paths unmodified.
func Test_NewFileCache(t *testing.T) {
	testdir, err := os.MkdirTemp(os.TempDir(), "")
	assert.NoError(t, err)
	t.Cleanup(func() {
		assert.NoError(t, os.RemoveAll(testdir))
	})

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
	absPathsGot := fc.AbsPaths()
	assert.Equals(t, absPathsExp, absPathsGot)
}
