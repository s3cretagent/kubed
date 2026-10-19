package kube

import (
	"bytes"
	"testing"
)

// QA round 142: S on a container without a shell printed "a shell in …
// Type exit to go back" before the exec failed. The words come with the
// shell's first output, so a shell that never starts doesn't promise one.
func TestShellBannerWaitsForTheShell(t *testing.T) {
	var b bytes.Buffer
	bannerFirst(&b, "kubed: a shell in x. Type exit to go back to kubed.\r\n")
	if b.Len() != 0 {
		t.Errorf("banner before the shell said anything: %q", b.String())
	}
	b.Reset()
	w := bannerFirst(&b, "banner\r\n")
	_, _ = w.Write([]byte("$ "))
	_, _ = w.Write([]byte("ls"))
	if got := b.String(); got != "banner\r\n$ ls" {
		t.Errorf("%q", got)
	}
}
