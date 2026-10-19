package ui

import (
	"os"
	"testing"
)

// copied is what tests put on the clipboard: never the real one, which
// belongs to whoever runs the tests.
var copied string

func TestMain(m *testing.M) {
	copyText = func(text string) error {
		copied = text
		return nil
	}
	os.Exit(m.Run())
}
