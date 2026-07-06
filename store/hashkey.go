package store

import (
	"strconv"
	"strings"

	"github.com/PelicanPlatform/classad/classad"
)

// HashKey builds the identity key an ad is stored under, following the same
// rules as the C++ collector's make*AdHashKey functions: a (name, address)
// tuple, encoded here as name + '\0' + address. It returns false if the ad
// carries no usable name (in which case the collector rejects the update).
//
// The per-table differences:
//   - Startd: the name gains a ":<SlotID>" suffix when SlotID is present, and
//     the address falls back to StartdIpAddr.
//   - Schedd/Submitter: the name gains the ScheddName (so two submitters on
//     different schedds do not collide), and the address falls back to
//     ScheddIpAddr.
//   - Master: address-less (name only).
//   - everything else: (name, MyAddress).
func HashKey(t AdType, ad *classad.ClassAd) ([]byte, bool) {
	return keyFields(t, func(attr string) (string, bool) {
		if attr == attrSlotID {
			if v, ok := ad.EvaluateAttrInt(attr); ok {
				return strconv.FormatInt(v, 10), true
			}
			return "", false
		}
		return ad.EvaluateAttrString(attr)
	})
}

// hashKeyFromText builds the same key directly from old-ClassAd wire text,
// scanning only the handful of attributes the key needs -- so the ingest hot
// path never materializes a full ad just to compute a key.
func hashKeyFromText(t AdType, text string) ([]byte, bool) {
	attrs := scanKeyAttrs(text)
	return keyFields(t, func(attr string) (string, bool) {
		v, ok := attrs[strings.ToLower(attr)]
		return v, ok
	})
}

// keyFields builds the key using get to resolve each attribute to its string
// value ("" / false if absent). SlotID is resolved to its decimal text.
func keyFields(t AdType, get func(attr string) (string, bool)) ([]byte, bool) {
	name, ok := get(attrName)
	if !ok || name == "" {
		if name, ok = get(attrMachine); !ok || name == "" {
			return nil, false
		}
	}

	var addr string
	switch t {
	case StartdAd, StartdPvtAd:
		if slot, ok := get(attrSlotID); ok && slot != "" {
			name += ":" + slot
		}
		addr = firstOf(get, attrMyAddress, attrStartdIPAddr)
	case ScheddAd, SubmitterAd:
		if sn, ok := get(attrScheddName); ok && sn != "" {
			name += sn
		}
		addr = firstOf(get, attrMyAddress, attrScheddIPAddr)
	case MasterAd:
		addr = ""
	default:
		addr, _ = get(attrMyAddress)
	}

	key := make([]byte, 0, len(name)+1+len(addr))
	key = append(key, name...)
	key = append(key, 0)
	key = append(key, addr...)
	return key, true
}

func firstOf(get func(string) (string, bool), attrs ...string) string {
	for _, a := range attrs {
		if v, ok := get(a); ok && v != "" {
			return v
		}
	}
	return ""
}

// keyAttrs is the set of (lower-cased) attribute names any table's key may need.
var keyAttrs = func() map[string]bool {
	m := map[string]bool{}
	for _, a := range []string{
		attrName, attrMachine, attrMyAddress, attrSlotID,
		attrStartdIPAddr, attrScheddName, attrScheddIPAddr,
	} {
		m[strings.ToLower(a)] = true
	}
	return m
}()

// scanKeyAttrs pulls just the key-relevant attributes out of old-ClassAd text.
// String values are unquoted; other values (e.g. SlotID) are taken verbatim.
// First occurrence wins, matching the streaming encoder's first-wins lookup.
func scanKeyAttrs(text string) map[string]string {
	out := make(map[string]string, len(keyAttrs))
	for len(text) > 0 {
		var line string
		if nl := strings.IndexByte(text, '\n'); nl >= 0 {
			line, text = text[:nl], text[nl+1:]
		} else {
			line, text = text, ""
		}
		eq := strings.IndexByte(line, '=')
		if eq < 0 {
			continue
		}
		name := strings.ToLower(strings.TrimSpace(line[:eq]))
		if !keyAttrs[name] {
			continue
		}
		if _, dup := out[name]; dup {
			continue
		}
		val := strings.TrimSpace(line[eq+1:])
		if len(val) >= 2 && val[0] == '"' && val[len(val)-1] == '"' {
			val = val[1 : len(val)-1]
		}
		out[name] = val
	}
	return out
}
