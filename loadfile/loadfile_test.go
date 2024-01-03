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

	// create a directory and a file
	dirname := "mydir"
	dirpath := filepath.Join(testdir, dirname)
	filename := "myfile"
	assert.NoError(t, os.MkdirAll(dirpath, os.ModePerm))
	f, err := os.CreateTemp(testdir, filename)
	assert.NoError(t, err)
	tempfilepath := f.Name()
	assert.NoError(t, f.Close())

	// create symlinks to the above directory and file
	symlinkDirname := dirname + "_sym"
	symlinkFilepath := tempfilepath + "_sym"
	symlinkDirpath := filepath.Join(testdir, symlinkDirname)
	assert.NoError(t, os.Symlink(dirpath, symlinkDirpath))
	assert.NoError(t, os.Symlink(tempfilepath, symlinkFilepath))

	neededFiles := []string{
		tempfilepath,
		symlinkFilepath,
	}
	filemap, err := loadfile.LoadContext(neededFiles)
	filemapExp := map[string][]byte{
		tempfilepath:    {},
		symlinkFilepath: {},
	}
	// assert no error on attempting to read files
	// that cannot be read
	assert.NoError(t, err)

	// assert only the regular file will be loaded
	assert.Equals(t, filemap, filemapExp)

	t.Cleanup(func() {
		assert.NoError(t, os.RemoveAll(testdir))
	})
}
