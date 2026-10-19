package ui

import "testing"

// The error a run ended with, from the shapes logs come in.
func TestCrashError(t *testing.T) {
	cases := map[string][]string{
		// Python's traceback, plain.
		"ValueError: bad port": {"Starting", "Traceback (most recent call last):", `  File "/app/main.py", line 3, in <module>`, "ValueError: bad port"},
		// A traceback kept as a JSON list of escaped strings, escaped again.
		"django.core.exceptions.ImproperlyConfigured: The SECRET_KEY setting must not be empty.": {
			`{"level": "ERROR", "exc": "[\\"Traceback (most recent call last):\\\\n\\", \\"  File \\\\\\"/app/settings.py\\\\\\", line 9\\\\n\\", \\"django.core.exceptions.ImproperlyConfigured: The SECRET_KEY setting must not be empty.\\\\n\\"]"}`,
		},
		// Go.
		"panic: runtime error: index out of range [3] with length 3": {"listening on :8080", "panic: runtime error: index out of range [3] with length 3", "", "goroutine 1 [running]:", "main.main()"},
		// No exception line: the last error-looking line.
		"level=error msg=\"db unreachable\"": {"level=info msg=starting", `level=error msg="db unreachable"`, "level=info msg=exiting"},
		// A JSON error line: its message, quotes and all.
		`boom "quoted"`: {`{"level":"info","msg":"starting"}`, `{"level":"error","msg":"boom \"quoted\""}`},
		// A JSON line whose string ends right after the error.
		"KeyError: 'QA24_KEY'": {`{"level":"ERROR","exc":"Traceback (most recent call last):\n  File \"a.py\"\nKeyError: 'QA24_KEY'"}`},
	}
	for want, lines := range cases {
		if got, _ := crashError(lines); got != want {
			t.Errorf("crashError(%q)\n got %q\nwant %q", lines, got, want)
		}
	}
}

// Logs that couldn't be read aren't "no error": they're tried again.
func TestCrashErrorUnreadable(t *testing.T) {
	if _, ok := crashError([]string{"kubed: the logs from before the restart are gone"}); ok {
		t.Error("an unreadable log must not count as read")
	}
}
