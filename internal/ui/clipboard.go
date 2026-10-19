package ui

import (
	"os"
	"os/exec"
	"runtime"
	"strings"

	"github.com/muesli/termenv"
)

// copyText puts text on the clipboard. A variable so tests can't touch the
// clipboard of whoever runs them.
var copyText = systemCopy

// systemCopy puts text on the clipboard: pbcopy on macOS, wl-copy / xclip
// on Linux desktops, otherwise the OSC 52 escape (works over SSH and in
// most modern terminals).
func systemCopy(text string) error {
	var cmd *exec.Cmd
	switch {
	case runtime.GOOS == "darwin":
		cmd = exec.Command("pbcopy")
	case os.Getenv("WAYLAND_DISPLAY") != "" && haveCommand("wl-copy"):
		cmd = exec.Command("wl-copy")
	case os.Getenv("DISPLAY") != "" && haveCommand("xclip"):
		cmd = exec.Command("xclip", "-selection", "clipboard")
	default:
		termenv.Copy(text)
		return nil
	}
	cmd.Stdin = strings.NewReader(text)
	return cmd.Run()
}

func haveCommand(name string) bool {
	_, err := exec.LookPath(name)
	return err == nil
}
