package view

import "testing"

// QA round 126: an app whose deployment was made paused has no pod and
// asks for 1 copy; READY read 0/0 next to "asks for 1 copy but none
// exist", while gd showed 0/1. READY counts the copies it asks for.
func TestReadyCountsWantedCopiesWhenNoneExist(t *testing.T) {
	cases := []struct {
		a    App
		want string
	}{
		{App{wants: 1}, "0/1"},          // none made yet
		{App{wants: 3}, "0/3"},          // none made yet, several asked for
		{App{}, "0/0"},                  // asks for none (scaled to 0)
		{App{Pods: 2, Ready: 2}, "2/2"}, // pods kubed knows no workload for
	}
	for _, c := range cases {
		if got := readyCell(&c.a); got != c.want {
			t.Errorf("pods %d, wants %d: READY %s, want %s", c.a.Pods, c.a.wants, got, c.want)
		}
	}
}
