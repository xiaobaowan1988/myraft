package raft

import (
	"os"
	"regexp"
	"strings"
	"testing"
)

// TestCoreHasNoIO enforces the plan's rule that the state machine performs no
// I/O: no storage, channels, goroutines, locks, wall clock or OS access.
func TestCoreHasNoIO(t *testing.T) {
	src, err := os.ReadFile("core.go")
	if err != nil {
		t.Fatal(err)
	}
	var code []string
	for _, line := range strings.Split(string(src), "\n") {
		if i := strings.Index(line, "//"); i >= 0 {
			line = line[:i]
		}
		code = append(code, line)
	}
	text := strings.Join(code, "\n")
	forbidden := map[string]*regexp.Regexp{
		"Storage":      regexp.MustCompile(`\bStorage\b`),
		"channel":      regexp.MustCompile(`\bchan\b|<-`),
		"goroutine":    regexp.MustCompile(`\bgo\s+(func|[a-zA-Z_])`),
		"sync":         regexp.MustCompile(`\bsync\.`),
		"time.Now":     regexp.MustCompile(`\btime\.(Now|Since|Sleep|After|NewTimer|NewTicker|Tick)\b`),
		"os/io/net/fs": regexp.MustCompile(`\b(os|io|net|bufio|ioutil)\.`),
		"Transport":    regexp.MustCompile(`\bTransport\b`),
	}
	for name, re := range forbidden {
		if loc := re.FindStringIndex(text); loc != nil {
			t.Errorf("core.go must not use %s: found %q", name, text[loc[0]:loc[1]])
		}
	}
}
