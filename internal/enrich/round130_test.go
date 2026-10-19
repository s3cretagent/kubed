package enrich

import (
	"testing"
	"time"
)

// QA round 130: goodold's first version (busybox:1.37, 21:48:49) went out
// with scaledold's first version (21:49:05), and its second (busybox:1.36)
// at 21:48:57. Grouped with scaledold, the 1.37 row came above 1.36, as if
// goodold went 1.36 -> 1.37. A workload's versions keep their order: its
// first version isn't grouped past its later one.
func TestGroupedRolloutKeepsAWorkloadsVersionsInOrder(t *testing.T) {
	at := func(hms string) time.Time {
		tm, err := time.Parse("2006-01-02 15:04:05", "2026-10-16 "+hms)
		if err != nil {
			t.Fatal(err)
		}
		return tm
	}
	in := []Change{
		{When: at("21:49:05"), What: "rollout", Namespace: "kubed-qa130", Object: "deployment/scaledold", Detail: "revision 1: busybox:1.37"},
		{When: at("21:48:57"), What: "rollout", Namespace: "kubed-qa130", Object: "deployment/goodold", Detail: "revision 2: busybox:1.36"},
		{When: at("21:48:49"), What: "rollout", Namespace: "kubed-qa130", Object: "deployment/goodold", Detail: "revision 1: busybox:1.37"},
	}
	out := groupRollouts(in)
	// Each workload's rows, top to bottom, must go back in time.
	last := map[string]time.Time{}
	lastRow := map[string]string{}
	for _, c := range out {
		objs := c.Objects
		if len(objs) == 0 {
			objs = []string{c.Object}
		}
		for _, o := range objs {
			// a member's own time: the input row of that workload with these images
			var own time.Time
			for _, i := range in {
				if i.Object == o && groupImages(i.Detail) == groupImages(c.Detail) {
					own = i.When
				}
			}
			if prev, ok := last[o]; ok && !own.Before(prev) {
				t.Errorf("%s: %q (made %s) is listed below %q (made %s)", o, c.Detail, own.Format("15:04:05"), lastRow[o], prev.Format("15:04:05"))
			}
			last[o], lastRow[o] = own, c.Detail
		}
	}
	if t.Failed() {
		for _, c := range out {
			t.Logf("%s  %s  %s", c.When.Format("15:04:05"), c.Object, c.Detail)
		}
	}
}
