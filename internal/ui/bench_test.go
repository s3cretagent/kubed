package ui

import (
	"fmt"
	"testing"

	"github.com/s3cretagent/kubed/internal/view"
)

func BenchmarkTableRefresh5000(b *testing.B) {
	rs := make([]view.Row, 5000)
	for i := range rs {
		r := rows(fmt.Sprintf("app-%d-7f6bcc6f86-762n4", i))[0]
		r.Cells = []string{"payment", r.Obj.GetName(), "1/1", "Running", "0", "12m", "171Mi", "1", "0", "134", "33", "10.0.134.235", "ip-10-0-133-237.ap-south-1.compute.internal", "4h"}
		rs[i] = r
	}
	header := []string{"NAMESPACE", "NAME", "READY", "STATUS", "RESTARTS", "CPU", "MEM", "%CPU/R", "%CPU/L", "%MEM/R", "%MEM/L", "IP", "NODE", "AGE"}
	var tb table
	b.ResetTimer()
	for range b.N {
		in := append([]view.Row(nil), rs...)
		tb.setRows(header, in, nil)
		_ = tb.render(170, 30)
	}
}
