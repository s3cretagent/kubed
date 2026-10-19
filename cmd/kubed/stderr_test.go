package main

import (
	"os"
	"path/filepath"
	"strconv"
	"testing"
)

// Logs of finished runs are tidied: a crash is kept and reported, a clean
// one is deleted, and a running kubed's log is left alone.
func TestSweepLogs(t *testing.T) {
	dir := t.TempDir()
	write := func(name, body string) string {
		p := filepath.Join(dir, name)
		if err := os.WriteFile(p, []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
		return p
	}
	write("stderr-999999.log", "panic: boom\n") // no such process: it crashed
	clean := write("stderr-999998.log", "aws: token refreshed\n")
	mine := write("stderr-"+strconv.Itoa(os.Getpid())+".log", "running\n")

	crashed := sweepLogs(dir)
	if crashed == "" {
		t.Fatal("a crashed run's log must be kept and reported")
	}
	if _, err := os.Stat(crashed); err != nil {
		t.Errorf("crash log missing: %v", err)
	}
	if _, err := os.Stat(clean); !os.IsNotExist(err) {
		t.Error("a clean run's log should be deleted")
	}
	if _, err := os.Stat(mine); err != nil {
		t.Error("a running kubed's log must be left alone")
	}
}
