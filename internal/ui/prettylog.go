package ui

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/charmbracelet/x/ansi"
)

// Most apps log one JSON object per line. prettyLog shows such a line the
// way people read it: time, level, the message, then the other fields.
// Anything that isn't a JSON object is returned as it is.
func prettyLog(line string) string { return prettyLine(line, logTagEnd(line)) }

// prettyLine is prettyLog for a line whose kubed pod tag ends at tag (0:
// no tag, as in one pod's logs, where a "│" is the app's own).
func prettyLine(line string, tag int) string {
	// t puts Kubernetes' own timestamp first: show it as a time of day.
	if tag < len(line) {
		if sp := strings.IndexByte(line[tag:], ' '); sp > 0 {
			if ts, err := time.Parse(time.RFC3339Nano, line[tag:tag+sp]); err == nil {
				// The date and time; when the line carries the same time
				// itself, only the date, so it isn't said twice.
				raw := line[tag+sp+1:]
				if startsWithTime(raw) { // its own time right after the cluster's: one is enough
					_, raw, _ = strings.Cut(raw, " ")
					return line[:tag] + styleMuted.Render(ts.Local().Format("2 Jan 15:04:05")) + " " + prettyLine(raw, bracketTagEnd(raw))
				}
				rest := prettyLine(raw, bracketTagEnd(raw))
				stamp := ts.Local().Format("2 Jan 15:04:05") // your time, like every other screen
				// The line has its own time (JSON, or a leading timestamp):
				// add only the date, so the time isn't said twice.
				if strings.HasPrefix(ansi.Strip(rest), ts.Local().Format("15:04:05")) {
					stamp = ts.Local().Format("2 Jan")
				}
				return line[:tag] + styleMuted.Render(stamp) + " " + rest
			}
		}
	}
	start, fields := parseLogLine(line, tag)
	if fields == nil {
		return line
	}
	take := func(keys ...string) string {
		for _, k := range keys {
			if v, ok := fields[k]; ok {
				delete(fields, k)
				return strings.TrimSpace(fmt.Sprint(v))
			}
		}
		return ""
	}
	// The time: the first of these fields that really holds a timestamp (a
	// "time" field can also be a duration, like "time": 18).
	when := ""
	for _, k := range []string{"@timestamp", "timestamp", "time", "ts", "t"} {
		if v, ok := fields[k].(string); ok {
			if t := shortTime(v); t != v || strings.Count(v, ":") == 2 {
				when = t
				delete(fields, k)
				break
			}
		}
	}
	level := strings.ToUpper(take("level", "lvl", "severity", "levelname", "log.level"))
	msg := take("msg", "message", "event", "error", "err")
	// Python's logging keeps the template in msg and the result in
	// message: show the sentence, not "%s: %s".
	if strings.Contains(msg, "%") {
		if full, ok := fields["message"].(string); ok && full != "" {
			msg = full
			delete(fields, "message")
		}
	}

	var b strings.Builder
	b.WriteString(line[:start]) // a pod tag like "[api-1] " stays in front
	if when != "" {
		b.WriteString(styleMuted.Render(when) + "  ")
	}
	if level != "" {
		lv := fmt.Sprintf("%-7s", level) // WARNING is the longest common level
		switch {
		case strings.HasPrefix(level, "ERR"), strings.HasPrefix(level, "FATAL"), strings.HasPrefix(level, "CRIT"), strings.HasPrefix(level, "PANIC"):
			lv = styleErr.Render(lv)
		case strings.HasPrefix(level, "WARN"):
			lv = styleWarn.Render(lv)
		default:
			lv = styleMuted.Render(lv)
		}
		b.WriteString(lv + "  ")
	}
	b.WriteString(msg)
	keys := make([]string, 0, len(fields))
	for k := range fields {
		if !logNoise[k] {
			keys = append(keys, k)
		}
	}
	sort.Strings(keys)
	for _, k := range keys {
		v := fields[k]
		var s string
		switch v := v.(type) {
		case string:
			s = v
		default:
			raw, _ := json.Marshal(v)
			s = string(raw)
		}
		if strings.ContainsAny(s, " \t") {
			s = fmt.Sprintf("%q", s)
		}
		b.WriteString("  " + styleMuted.Render(k+"=") + s)
	}
	return b.String()
}

// parseLogLine reads a JSON object, or key=value pairs (logfmt), after an
// optional tag. start is where the structured part begins; fields is nil
// when the line is neither.
func parseLogLine(line string, tag int) (int, map[string]any) {
	if brace := strings.IndexByte(line[tag:], '{'); brace >= 0 && strings.HasSuffix(strings.TrimSpace(line), "}") {
		var fields map[string]any
		if json.Unmarshal([]byte(line[tag+brace:]), &fields) == nil && len(fields) > 0 {
			return tag + brace, fields
		}
	}
	// logfmt: needs a msg= or level= to be worth tidying.
	start := tag
	rest := strings.TrimRight(line[start:], " \t\r")
	if !strings.Contains(rest, "msg=") && !strings.Contains(rest, "level=") {
		return 0, nil
	}
	fields := map[string]any{}
	for rest != "" {
		if rest = strings.TrimLeft(rest, " "); rest == "" {
			break
		}
		eq := strings.IndexByte(rest, '=')
		if eq <= 0 || strings.ContainsAny(rest[:eq], " \"") {
			return 0, nil // not key=value all the way: leave the line alone
		}
		key, rest2 := rest[:eq], rest[eq+1:]
		var val string
		if strings.HasPrefix(rest2, `"`) {
			end := 1
			for end < len(rest2) && (rest2[end] != '"' || rest2[end-1] == '\\') {
				end++
			}
			if end >= len(rest2) {
				return 0, nil
			}
			val = strings.ReplaceAll(rest2[1:end], `\"`, `"`)
			rest = rest2[end+1:]
		} else {
			sp := strings.IndexByte(rest2, ' ')
			if sp < 0 {
				sp = len(rest2)
			}
			val, rest = rest2[:sp], rest2[sp:]
		}
		fields[key] = val
	}
	if len(fields) == 0 {
		return 0, nil
	}
	return start, fields
}

// logNoise are bookkeeping fields loggers add to every line (Python's
// logstash formatter, Go's zap…); j shows them.
var logNoise = map[string]bool{
	"@version": true, "source_host": true, "host": true, "hostname": true, "levelno": true, "pathname": true,
	"filename": true, "lineno": true, "funcName": true, "module": true, "process": true, "processName": true,
	"thread": true, "threadName": true, "args": true, "stack_info": true, "caller": true, "logger_name": true,
	"thread_name": true, "level_value": true, "pid": true, "v": true, "created": true, "msecs": true,
	"relativeCreated": true, "exc_info": true, "taskName": true,
}

// logTagEnd is where a merged log line's pod tag ("web·bjj7k │ ") ends, or
// 0 when there is none.
func logTagEnd(line string) int {
	if i := strings.Index(line, "│"); i >= 0 && i < 200 {
		end := i + len("│")
		// Skip the colour reset and the space after the bar.
		for end < len(line) {
			switch line[end] {
			case ' ':
				end++
			case '\x1b':
				j := strings.IndexByte(line[end:], 'm')
				if j < 0 {
					return end
				}
				end += j + 1
			default:
				return end
			}
		}
		return end
	}
	return bracketTagEnd(line)
}

// bracketTagEnd is where a "[name] " an app puts first ends (0: none): a
// label to read past, in one pod's logs too.
func bracketTagEnd(line string) int {
	if strings.HasPrefix(line, "[") {
		if i := strings.Index(line, "] "); i > 0 {
			return i + 2
		}
	}
	return 0
}

// shortTime shows a log timestamp as the time of day.
func shortTime(s string) string {
	if s == "" {
		return ""
	}
	for _, layout := range []string{time.RFC3339Nano, time.RFC3339, "2006-01-02T15:04:05.000Z0700", "2006-01-02 15:04:05"} {
		if t, err := time.Parse(layout, s); err == nil {
			return t.Local().Format("15:04:05") // your time, like every other screen
		}
	}
	return s
}

// startsWithTime: the line begins with a timestamp of its own.
func startsWithTime(line string) bool {
	first, _, _ := strings.Cut(line, " ")
	for _, layout := range []string{time.RFC3339Nano, time.RFC3339} {
		if _, err := time.Parse(layout, first); err == nil {
			return true
		}
	}
	return false
}
