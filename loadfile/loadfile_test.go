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
func TestLoadContext(t *testing.T) {
	testdir := "/tmp/loadfile-test"
	// cleanup directory even if it's there
	_ = os.RemoveAll(testdir)

	assert.NoError(t, os.MkdirAll(testdir, os.ModePerm))

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

	neededFiles := []string{
		filePath,
		symlinkFilepath,
	}
	filemap, err := loadfile.LoadContext(neededFiles)
	// assert no error on attempting to read files
	// that cannot be read
	assert.NoError(t, err)

	// assert only the regular and symlinked file are loaded
	filemapExp := map[string][]byte{
		filePath:        {},
		symlinkFilepath: {},
	}
	assert.Equals(t, filemap, filemapExp)

	// error on loading a directory
	neededFiles = []string{
		dirpath,
	}
	ctxFiles, err := loadfile.LoadContext(neededFiles)
	assert.Error(t, err)
	assert.Nil(t, ctxFiles)

	// error on loading a symlink directory
	neededFiles = []string{
		symlinkDirpath,
	}
	ctxFiles, err = loadfile.LoadContext(neededFiles)
	assert.Error(t, err)
	assert.Nil(t, ctxFiles)

	t.Cleanup(func() {
		assert.NoError(t, os.RemoveAll(testdir))
	})
}

// This tests AbsPathsWithContext joins relative paths with the
// context (root) directory, and passes through absolute paths
// unmodified.
func TestContextAbsFilepaths(t *testing.T) {
	testdir, err := os.MkdirTemp(os.TempDir(), "")
	assert.NoError(t, err)

	testFilepaths := map[string]string{
		"a": "a.yaml",
		"b": "/b.toml",
		"c": "../rel/subdir/c.txt",
	}

	absPathsExp := map[string]string{
		"a": filepath.Join(testdir, testFilepaths["a"]),
		// since the 'b' file has an absolute path, it should be unmodified
		"b": "/b.toml",
		"c": filepath.Join(testdir, testFilepaths["c"]),
	}

	absPathsGot, err := loadfile.AbsPathsWithContext(testdir, testFilepaths)
	assert.NoError(t, err)
	assert.Equals(t, absPathsExp, absPathsGot)

	t.Cleanup(func() {
		assert.NoError(t, os.RemoveAll(testdir))
	})
}
