package view

import (
	"testing"
	"time"
)

func TestCronWords(t *testing.T) {
	for in, want := range map[string]string{
		"*/5 * * * *": "every 5 minutes",
		"30 2 * * *":  "every day at 02:30",
		"0 * * * *":   "every hour at :00",
		"15 * * * *":  "every hour at :15",
		"0 9 * * 1":   "every Monday at 09:00",
		"0 9 * * 1-5": "weekdays at 09:00",
		"0 0 1 * *":   "on day 1 of every month at 00:00",
		"0 0 1 1 *":   "every 1 Jan at 00:00",
		"0 */6 * * *": "every 6 hours",
		"@daily":      "every day at 00:00",
		"0 0 30 2 *":  "never: 30 Feb doesn't exist",
		"5 4 * 2-3 *": "5 4 * 2-3 *",
	} {
		if got := cronWords(in); got != want {
			t.Errorf("cronWords(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestCronWeekdayRanges(t *testing.T) {
	for expr, want := range map[string]string{
		"29 18 * * 1-4": "every Monday to Thursday at 18:29",
		"0 9 * * 1,3,5": "every Monday, Wednesday and Friday at 09:00",
		"0 23 * * 1":    "every Monday at 23:00",
	} {
		if got := cronWords(expr); got != want {
			t.Errorf("%s: got %q, want %q", expr, got, want)
		}
	}
}

func TestNextRun(t *testing.T) {
	from := time.Date(2026, 10, 6, 10, 30, 0, 0, time.UTC) // a Tuesday
	for expr, want := range map[string]string{
		"*/15 * * * *":  "2026-10-06 10:45",
		"0 2 * * *":     "2026-10-07 02:00",
		"29 18 * * 1-4": "2026-10-06 18:29",
		"0 0 1 * *":     "2026-11-01 00:00",
		"@weekly":       "2026-10-11 00:00",
	} {
		got, ok := NextRun(expr, from, time.UTC)
		if !ok || got.Format("2006-01-02 15:04") != want {
			t.Errorf("%s: got %v %v, want %s", expr, got, ok, want)
		}
	}
	if _, ok := NextRun("not a schedule", from, time.UTC); ok {
		t.Error("nonsense should not parse")
	}
}

func TestMessageWords(t *testing.T) {
	for in, want := range map[string]string{
		`Readiness probe failed: Get "http://10.0.9.9:5000/health": context deadline exceeded (Client.Timeout exceeded while awaiting headers)`: "ready check failed: no answer in time on /health, port 5000",
		`Liveness probe failed: Get "http://10.0.0.1:8080/": dial tcp 10.0.0.1:8080: connect: connection refused`:                               "alive check failed: nothing listening on /, port 8080",
		`New size: 2; reason: All metrics below target`:                                       "scaled to 2 (load is low)",
		`Successfully assigned kubed-qa4/db-0 to ip-10-0-134-214.ap-south-1.compute.internal`: "placed on node ip-10-0-134-214",
	} {
		if got := MessageWords(in); got != want {
			t.Errorf("%s:\n got %q\nwant %q", in, got, want)
		}
	}
}
