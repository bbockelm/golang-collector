package store

import "strings"

// AdName extracts an ad's Name attribute from old-ClassAd wire text, for
// identifying a rejected ad in a log line. It returns "" if there is no Name.
// This is a best-effort scan of the raw text (the ad may not parse at all), so it
// does not build a ClassAd.
func AdName(text string) string {
	for len(text) > 0 {
		nl := strings.IndexByte(text, '\n')
		var line string
		if nl < 0 {
			line, text = text, ""
		} else {
			line, text = text[:nl], text[nl+1:]
		}
		s := strings.TrimSpace(line)
		if !strings.HasPrefix(s, "Name") {
			continue
		}
		rest := strings.TrimSpace(s[len("Name"):])
		if !strings.HasPrefix(rest, "=") {
			continue
		}
		return strings.Trim(strings.TrimSpace(rest[1:]), `"`)
	}
	return ""
}

// AdExcerpt returns an ad's wire text capped for logging. A rejected ad is worth
// seeing in full (that is the whole point), but bound it so a pathological ad
// cannot flood the log.
func AdExcerpt(text string) string {
	const max = 8192
	if len(text) > max {
		return text[:max] + "\n...[truncated]"
	}
	return text
}
