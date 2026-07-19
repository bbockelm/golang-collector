package store

import (
	"strings"

	"github.com/bbockelm/cedar/message"
)

// AdName extracts an ad's Name attribute from old-ClassAd wire text, for
// identifying a rejected ad in a log line. It returns "" if there is no Name.
// This is a best-effort scan of the raw text (the ad may not parse at all), so it
// does not build a ClassAd. The Name of a machine/daemon ad is not a secret.
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

// AdExcerpt returns an ad's wire text, sanitized and capped, for logging a
// rejected ad. It redacts secret material by default (see SanitizeAdText): the
// excerpt is a debugging aid -- an operator needs the ad's structure and the
// offending line, never the claim ids/capabilities it may carry -- so private
// values never reach the log even when the ad is malformed.
func AdExcerpt(text string) string {
	text = SanitizeAdText(text)
	const max = 8192
	if len(text) > max {
		return text[:max] + "\n...[truncated]"
	}
	return text
}

// SanitizeAdText redacts private (secret) attributes from old-ClassAd wire text
// for safe logging. It works line by line (the text may not parse), and replaces
// only the VALUE of a line whose ATTRIBUTE NAME is a private attribute
// (message.ClassAdAttributeIsPrivateAny -- ClaimId, ClaimIds, Capability,
// ChildClaimIds, ...) with "<redacted>". It never inspects attribute values, so
// it stays cheap and touches nothing but the handful of known-secret attributes.
// Attribute names, structure, and every non-secret value are preserved, so the
// log still shows what and where the parse failed -- and which claim-id attribute
// was present, which is useful for debugging without exposing the id itself.
// A line with no '=' (e.g. a stray token that broke the parse) is kept as-is.
func SanitizeAdText(text string) string {
	var b strings.Builder
	b.Grow(len(text))
	for {
		nl := strings.IndexByte(text, '\n')
		line := text
		if nl >= 0 {
			line = text[:nl]
		}
		b.WriteString(sanitizeAdLine(line))
		if nl < 0 {
			break
		}
		b.WriteByte('\n')
		text = text[nl+1:]
	}
	return b.String()
}

func sanitizeAdLine(line string) string {
	eq := strings.IndexByte(line, '=')
	if eq < 0 {
		return line // no assignment: nothing to redact
	}
	if message.ClassAdAttributeIsPrivateAny(strings.TrimSpace(line[:eq])) {
		return line[:eq+1] + ` "<redacted>"`
	}
	return line
}
