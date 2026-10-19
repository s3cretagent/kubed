package ui

import (
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// emDash is U+2014, written as a number so this file passes its own test.
const emDash = 0x2014

// kubed talks like a person: no em-dashes anywhere in what it shows (or in
// its code, where the text lives).
func TestNoEmDashes(t *testing.T) {
	root := filepath.Join("..", "..")
	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() && (d.Name() == ".git" || d.Name() == "plans" || d.Name() == "dist") {
			return filepath.SkipDir
		}
		if d.IsDir() || (!strings.HasSuffix(path, ".go") && !strings.HasSuffix(path, ".md")) {
			return nil
		}
		data, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		for i, line := range strings.Split(string(data), "\n") {
			if strings.ContainsRune(line, emDash) {
				t.Errorf("%s:%d has an em-dash: %s", path, i+1, strings.TrimSpace(line))
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}
