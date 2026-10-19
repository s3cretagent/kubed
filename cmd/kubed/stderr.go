package main

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"golang.org/x/sys/unix"
)

// captureStderr points file descriptor 2 at a log file of this process's
// own while the UI runs. Kubeconfig credential helpers (aws eks get-token…)
// write to fd 2 directly, which would draw over the screen; their lines
// arrive on the returned channel so the UI can show them. A file (not a
// pipe) also keeps a crash's stack trace, which the process writes just
// before it dies.
//
// Each process has its own file (stderr-<pid>.log), so two kubed windows
// never show each other's messages. crashed is the path of a log left by an
// earlier run that panicked.
func captureStderr() (lines <-chan string, crashed string, restore func(runErr error), err error) {
	dir, err := os.UserCacheDir()
	if err != nil {
		return nil, "", nil, err
	}
	dir = filepath.Join(dir, "kubed")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, "", nil, err
	}
	crashed = sweepLogs(dir)

	logPath := filepath.Join(dir, fmt.Sprintf("stderr-%d.log", os.Getpid()))
	f, err := os.OpenFile(logPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return nil, "", nil, err
	}
	saved, err := unix.Dup(2)
	if err != nil {
		f.Close()
		return nil, "", nil, err
	}
	if err := unix.Dup2(int(f.Fd()), 2); err != nil {
		_ = unix.Close(saved)
		f.Close()
		return nil, "", nil, err
	}

	done := make(chan struct{})
	ch := make(chan string, 64)
	go tail(logPath, ch, done)

	restore = func(runErr error) {
		close(done)
		if runErr != nil {
			// Bubble Tea caught a crash and wrote its stack here: keep it.
			fmt.Fprintf(f, "\npanic: kubed stopped: %v\n", runErr)
		}
		_ = unix.Dup2(saved, 2)
		_ = unix.Close(saved)
		f.Close()
		if runErr != nil {
			kept := filepath.Join(dir, "crash-"+time.Now().Format("20060102-150405")+".log")
			if os.Rename(logPath, kept) == nil {
				fmt.Fprintln(os.Stderr, "kubed: details of what went wrong are in "+kept)
			}
			return
		}
		_ = os.Remove(logPath) // a clean exit leaves nothing behind
	}
	return ch, crashed, restore, nil
}

// sweepLogs tidies logs of kubed processes that are no longer running. One
// that ended in a crash is kept (renamed crash-<time>.log) and returned;
// the rest are deleted. Logs of running kubeds are left alone.
func sweepLogs(dir string) (crashed string) {
	_ = os.Remove(filepath.Join(dir, "stderr.log")) // older versions shared one file
	_ = os.Remove(filepath.Join(dir, "stderr.prev.log"))
	paths, _ := filepath.Glob(filepath.Join(dir, "stderr-*.log"))
	for _, path := range paths {
		pid, err := strconv.Atoi(strings.TrimSuffix(strings.TrimPrefix(filepath.Base(path), "stderr-"), ".log"))
		if err != nil || alive(pid) {
			continue
		}
		data, err := os.ReadFile(path)
		if err == nil && (bytes.Contains(data, []byte("panic:")) || bytes.Contains(data, []byte("fatal error:"))) {
			kept := filepath.Join(dir, "crash-"+time.Now().Format("20060102-150405")+".log")
			if os.Rename(path, kept) == nil {
				crashed = kept
				continue
			}
		}
		_ = os.Remove(path)
	}
	return crashed
}

// alive reports whether a process with this pid is running.
func alive(pid int) bool {
	if pid <= 0 || pid == os.Getpid() {
		return pid == os.Getpid()
	}
	err := unix.Kill(pid, 0)
	return err == nil || err == unix.EPERM
}

// tail follows the log file and sends each new non-empty line.
func tail(path string, out chan<- string, done <-chan struct{}) {
	defer close(out)
	r, err := os.Open(path)
	if err != nil {
		return
	}
	defer r.Close()
	br := bufio.NewReader(r)
	var partial string
	for {
		line, err := br.ReadString('\n')
		partial += line
		if err == io.EOF {
			select {
			case <-done:
				return
			case <-time.After(200 * time.Millisecond):
				continue
			}
		}
		if err != nil {
			return
		}
		if l := strings.TrimSpace(partial); l != "" {
			select {
			case out <- l:
			default: // UI not keeping up; drop rather than block
			}
		}
		partial = ""
	}
}
