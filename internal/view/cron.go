package view

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

var weekdays = []string{"Sunday", "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday"}

var months = []string{"", "Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"}

// cronWords says a cron schedule the way people do: "every day at 02:30",
// "every 5 minutes". Schedules it can't say simply are shown as written.
// daysIn is the most days each month can have (February: 29).
var daysIn = [12]int{31, 29, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31}

func cronWords(expr string) string {
	f := strings.Fields(expr)
	switch {
	case len(f) == 1:
		if words, ok := cronShortcuts[f[0]]; ok {
			return words
		}
		return expr
	case len(f) != 5:
		return expr
	}
	minute, hour, dom, month, dow := f[0], f[1], f[2], f[3], f[4]
	if d, err1 := strconv.Atoi(dom); err1 == nil && dow == "*" {
		if mo, err2 := strconv.Atoi(month); err2 == nil && mo >= 1 && mo <= 12 && d > daysIn[mo-1] {
			return fmt.Sprintf("never: %d %s doesn't exist", d, months[mo])
		}
	}
	at := func() (string, bool) {
		h, err1 := strconv.Atoi(hour)
		m, err2 := strconv.Atoi(minute)
		if err1 != nil || err2 != nil {
			return "", false
		}
		return fmt.Sprintf("%02d:%02d", h, m), true
	}
	switch {
	case minute == "*" && hour == "*" && dom == "*" && month == "*" && dow == "*":
		return "every minute"
	case minute == "*/1" && hour == "*" && dom == "*" && month == "*" && dow == "*":
		return "every minute"
	case strings.HasPrefix(minute, "*/") && hour == "*" && dom == "*" && month == "*" && dow == "*":
		return "every " + strings.TrimPrefix(minute, "*/") + " minutes"
	case hour == "*/1" && dom == "*" && month == "*" && dow == "*" && minute == "0":
		return "every hour"
	case hour == "*" && dom == "*" && month == "*" && dow == "*":
		if _, err := strconv.Atoi(minute); err == nil {
			return fmt.Sprintf("every hour at :%02s", minute)
		}
	case strings.HasPrefix(hour, "*/") && dom == "*" && month == "*" && dow == "*" && minute == "0":
		return "every " + strings.TrimPrefix(hour, "*/") + " hours"
	case strings.HasPrefix(hour, "*/") && dom == "*" && month == "*" && dow == "*":
		if mm, err := strconv.Atoi(minute); err == nil {
			if n := strings.TrimPrefix(hour, "*/"); n != "1" {
				return fmt.Sprintf("every %s hours at :%02d", n, mm)
			}
			return fmt.Sprintf("every hour at :%02d", mm)
		}
	case strings.Contains(hour, ",") && dom == "*" && month == "*" && dow == "*":
		if mm, err := strconv.Atoi(minute); err == nil {
			var times []string
			for _, h := range strings.Split(hour, ",") {
				hh, err := strconv.Atoi(h)
				if err != nil {
					return expr
				}
				times = append(times, fmt.Sprintf("%02d:%02d", hh, mm))
			}
			return "every day at " + strings.Join(times[:len(times)-1], ", ") + " and " + times[len(times)-1]
		}
	}
	t, ok := at()
	if !ok {
		return expr
	}
	switch {
	case dom == "*" && month == "*" && dow == "*":
		return "every day at " + t
	case dom == "*" && month == "*":
		if dow == "1-5" || strings.EqualFold(dow, "MON-FRI") {
			return "weekdays at " + t
		}
		if days, ok := dowWords(dow); ok {
			return "every " + days + " at " + t
		}
	case month == "*" && dow == "*":
		if d, err := strconv.Atoi(dom); err == nil {
			return fmt.Sprintf("on day %d of every month at %s", d, t)
		}
	case dow == "*":
		d, err1 := strconv.Atoi(dom)
		mo, err2 := strconv.Atoi(month)
		if err1 == nil && err2 == nil && mo >= 1 && mo <= 12 {
			return fmt.Sprintf("every %d %s at %s", d, months[mo], t)
		}
	}
	return expr
}

// dowWords says a day-of-week field: "Monday", "Monday to Thursday",
// "Monday, Wednesday and Friday".
func dowWords(dow string) (string, bool) {
	day := func(s string) (int, bool) {
		if d, err := strconv.Atoi(s); err == nil && d >= 0 && d <= 7 {
			return d % 7, true
		}
		for i, w := range weekdays {
			if strings.EqualFold(s, w[:3]) {
				return i, true
			}
		}
		return 0, false
	}
	var parts []string
	for _, item := range strings.Split(dow, ",") {
		if from, to, isRange := strings.Cut(item, "-"); isRange {
			a, ok1 := day(from)
			b, ok2 := day(to)
			if !ok1 || !ok2 {
				return "", false
			}
			parts = append(parts, weekdays[a]+" to "+weekdays[b])
			continue
		}
		d, ok := day(item)
		if !ok {
			return "", false
		}
		parts = append(parts, weekdays[d])
	}
	if len(parts) > 1 {
		return strings.Join(parts[:len(parts)-1], ", ") + " and " + parts[len(parts)-1], true
	}
	return parts[0], true
}

// cronShortcuts are cron's named schedules.
var cronShortcuts = map[string]string{
	"@hourly": "every hour", "@daily": "every day at 00:00", "@midnight": "every day at 00:00",
	"@weekly": "every Sunday at 00:00", "@monthly": "on day 1 of every month at 00:00",
	"@yearly": "every 1 Jan at 00:00", "@annually": "every 1 Jan at 00:00",
}

// NextRun is when a cron schedule fires next after from, in loc. ok is
// false for schedules it can't read.
func NextRun(expr string, from time.Time, loc *time.Location) (time.Time, bool) {
	if words, ok := cronAliases[expr]; ok {
		expr = words
	}
	f := strings.Fields(expr)
	if len(f) != 5 {
		return time.Time{}, false
	}
	minutes, ok1 := cronField(f[0], 0, 59)
	hours, ok2 := cronField(f[1], 0, 23)
	doms, ok3 := cronField(f[2], 1, 31)
	months, ok4 := cronField(f[3], 1, 12)
	dows, ok5 := cronField(strings.ReplaceAll(f[4], "7", "0"), 0, 6)
	if !ok1 || !ok2 || !ok3 || !ok4 || !ok5 {
		return time.Time{}, false
	}
	domAny, dowAny := f[2] == "*", f[4] == "*"
	dayOK := func(t time.Time) bool {
		switch {
		case domAny && dowAny:
			return true
		case domAny:
			return dows[int(t.Weekday())]
		case dowAny:
			return doms[t.Day()]
		}
		return doms[t.Day()] || dows[int(t.Weekday())] // cron: either one
	}
	t := from.In(loc).Truncate(time.Minute).Add(time.Minute)
	for end := t.AddDate(1, 0, 1); t.Before(end); {
		switch {
		case !months[int(t.Month())] || !dayOK(t):
			t = time.Date(t.Year(), t.Month(), t.Day()+1, 0, 0, 0, 0, loc)
		case !hours[t.Hour()]:
			t = time.Date(t.Year(), t.Month(), t.Day(), t.Hour()+1, 0, 0, 0, loc)
		case !minutes[t.Minute()]:
			t = t.Add(time.Minute)
		default:
			return t, true
		}
	}
	return time.Time{}, false
}

var cronAliases = map[string]string{
	"@hourly": "0 * * * *", "@daily": "0 0 * * *", "@midnight": "0 0 * * *",
	"@weekly": "0 0 * * 0", "@monthly": "0 0 1 * *", "@yearly": "0 0 1 1 *", "@annually": "0 0 1 1 *",
}

// cronField reads one field (*, 5, 1-5, */15, 1,3,5, 9-17/2, MON) into the
// set of values it allows.
func cronField(field string, lo, hi int) (map[int]bool, bool) {
	set := map[int]bool{}
	names := map[string]int{"SUN": 0, "MON": 1, "TUE": 2, "WED": 3, "THU": 4, "FRI": 5, "SAT": 6,
		"JAN": 1, "FEB": 2, "MAR": 3, "APR": 4, "MAY": 5, "JUN": 6, "JUL": 7, "AUG": 8, "SEP": 9, "OCT": 10, "NOV": 11, "DEC": 12}
	num := func(s string) (int, bool) {
		if n, ok := names[strings.ToUpper(s)]; ok {
			return n, true
		}
		n, err := strconv.Atoi(s)
		return n, err == nil
	}
	for _, part := range strings.Split(field, ",") {
		rng, stepStr, hasStep := strings.Cut(part, "/")
		step := 1
		if hasStep {
			n, err := strconv.Atoi(stepStr)
			if err != nil || n <= 0 {
				return nil, false
			}
			step = n
		}
		from, to := lo, hi
		switch {
		case rng == "*" || rng == "?":
		case strings.Contains(rng, "-"):
			a, b, _ := strings.Cut(rng, "-")
			x, ok1 := num(a)
			y, ok2 := num(b)
			if !ok1 || !ok2 {
				return nil, false
			}
			from, to = x, y
		default:
			x, ok := num(rng)
			if !ok {
				return nil, false
			}
			from, to = x, x
			if hasStep {
				to = hi
			}
		}
		for v := from; v <= to; v += step {
			if v >= lo && v <= hi {
				set[v] = true
			}
		}
	}
	return set, len(set) > 0
}

// CronWords says a cron schedule in words ("every day at 02:30").
func CronWords(expr string) string { return cronWords(expr) }
