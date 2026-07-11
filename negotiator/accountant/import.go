package accountant

import (
	"bufio"
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"
)

// This file is the Accountantnew.log IMPORTER: a format adapter that reads the
// C++ negotiator's ClassAdLog accountant journal and loads its final committed
// state into the Go-native Store. It exists so a Go negotiator can take over a
// running C++ pool's accumulated priority/usage in place, instead of resetting
// everyone's fair-share history (roadmap #4, NEGOTIATOR_CPP_DIFFERENCES.md §3).
//
// It is a pure adapter because the native Store already uses the C++ key and
// attribute SHAPES (namespaces "Accountant."/"Customer.<name>"/"Resource.<key>",
// verbatim attr names from Accountant.cpp:42-67); only the on-disk encoding
// differs.
//
// ON-DISK FORMAT (src/condor_utils/classad_log.cpp, log.cpp). The journal is a
// line-oriented transaction log. Each record is one '\n'-terminated line whose
// first whitespace-delimited token is a numeric opcode
// (src/condor_utils/ClassAdLogEntry.h:25-44), followed by opcode-specific
// fields. Tokens ("words") are read by LogRecord::readword (log.cpp:63): skip
// leading whitespace, read until the next whitespace, and CONSUME that single
// terminating whitespace. A SetAttribute value is read by LogRecord::readline
// (log.cpp:115): everything from the character after the name's terminating
// space up to the newline, verbatim (so a value may contain spaces).
//
//	101 <key> <mytype> <targettype>   NewClassAd     (LogNewClassAd::ReadBody, classad_log.cpp:680)
//	102 <key>                         DestroyClassAd (LogDestroyClassAd::ReadBody, :783)
//	103 <key> <attr> <value>          SetAttribute   (LogSetAttribute::ReadBody, :886)
//	104 <key> <attr>                  DeleteAttribute(LogDeleteAttribute::ReadBody, :1042)
//	105                               BeginTransaction (LogBeginTransaction::ReadBody, :987)
//	106                               EndTransaction   (LogEndTransaction::ReadBody, :1026)
//	107 <seq> CreationTimestamp <ts>  HistoricalSequenceNumber (:608) -- ignored here
//
// mytype "(empty)" is the EMPTY_CLASSAD_TYPE_NAME placeholder (classad_log.cpp:50)
// for an empty type; the negotiator writes "*" (ClassAdLogAccountantDB.cpp:75).
//
// VALUE ENCODING (ClassAdLogAccountantDB.cpp:70-112): ints via std::to_chars
// (bare, e.g. "5", "-1"); floats via snprintf("%f") (always a decimal point,
// e.g. "0.500000"); strings double-quoted (e.g. "alice@dom"). We therefore
// classify a bare integer token as int64, a token with a '.'/exponent as
// float64, and a "..."-quoted token as string -- faithful to the Store's three
// scalar kinds.
//
// TRANSACTIONS (ClassAdLog::ReadLog loop, classad_log.cpp:102-175): mutations
// inside a BeginTransaction/EndTransaction pair are buffered and applied only on
// EndTransaction (Commit); a mutation OUTSIDE any transaction is applied
// immediately (Play). A trailing transaction with no matching EndTransaction at
// EOF is aborted and ignored ("Detected unterminated transaction"), which also
// makes us robust to a truncated final line -- we stop at the first unparseable
// record and drop any in-flight transaction, matching the C++ "ignore the tail"
// recovery for corruption not bracketed by a completed transaction.

// C++ ClassAdLog namespace key prefixes (ClassAdLogAccountantDB.cpp:31-33). Each
// maps a full journal key onto a Store table plus the prefix-stripped key.
const (
	cppPrefixAcct     = "Accountant."
	cppPrefixCustomer = "Customer."
	cppPrefixResource = "Resource."
)

// ClassAdLog opcodes (src/condor_utils/ClassAdLogEntry.h:25-44).
const (
	opNewClassAd       = 101
	opDestroyClassAd   = 102
	opSetAttribute     = 103
	opDeleteAttribute  = 104
	opBeginTransaction = 105
	opEndTransaction   = 106
	opHistoricalSeqNum = 107
)

// ImportedAd is one ClassAd parsed from a C++ Accountantnew.log: its full
// namespaced journal key ("Customer.alice@dom", "Resource.slot1@ip",
// "Accountant.") and its surviving attributes, each value coerced to one of the
// Store's three scalar kinds (int64, float64, string).
type ImportedAd struct {
	// Key is the full C++ journal key, including the namespace prefix.
	Key string
	// Attrs maps attribute name to value (int64, float64, or string).
	Attrs map[string]any
}

// ImportAccountantLog parses a C++ negotiator Accountantnew.log (ClassAdLog
// journal) and returns its final committed state: a map from full journal key
// to the reconstructed ImportedAd. Committed transactions and bare
// (out-of-transaction) mutations are applied in order; a trailing uncommitted
// transaction, and any tail following the first unparseable record, are ignored
// exactly as the C++ ClassAdLog::ReadLog replay would.
func ImportAccountantLog(path string) (map[string]*ImportedAd, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("accountant import: %w", err)
	}
	defer f.Close()

	ads := map[string]*ImportedAd{}

	// applyMut applies one non-transactional mutation to ads (used both for bare
	// records and when committing a transaction's buffered mutations).
	applyMut := func(m mutation) {
		switch m.op {
		case opNewClassAd:
			if _, ok := ads[m.key]; !ok {
				ads[m.key] = &ImportedAd{Key: m.key, Attrs: map[string]any{}}
			}
		case opDestroyClassAd:
			delete(ads, m.key)
		case opSetAttribute:
			ad := ads[m.key]
			if ad == nil {
				// ClassAdLog SetAttribute on a missing ad is a no-op (its Play
				// returns -1 when the key is absent), but the negotiator always
				// emits a NewClassAd first (ClassAdLogAccountantDB.cpp:74); be
				// lenient and materialize the ad so no state is lost.
				ad = &ImportedAd{Key: m.key, Attrs: map[string]any{}}
				ads[m.key] = ad
			}
			if m.hasValue {
				ad.Attrs[m.attr] = m.value
			}
		case opDeleteAttribute:
			if ad := ads[m.key]; ad != nil {
				delete(ad.Attrs, m.attr)
			}
		}
	}

	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 0, 64*1024), 8*1024*1024)

	var (
		inTxn   bool
		pending []mutation
	)
	for sc.Scan() {
		line := sc.Text()
		if strings.TrimSpace(line) == "" {
			continue
		}
		m, kind, ok := parseLogLine(line)
		if !ok {
			// Unparseable record: stop and ignore the tail, dropping any
			// in-flight transaction (classad_log.cpp:1145-1154 fseek-to-EOF).
			break
		}
		switch kind {
		case opBeginTransaction:
			// Nested BeginTransaction is a warning in C++ that keeps the
			// existing transaction (classad_log.cpp:119-123); do the same.
			inTxn = true
		case opEndTransaction:
			if inTxn {
				for _, pm := range pending {
					applyMut(pm)
				}
			}
			// An unmatched EndTransaction is a warning and a no-op in C++.
			pending = pending[:0]
			inTxn = false
		case opHistoricalSeqNum:
			// Header-only bookkeeping record; nothing to apply.
		default: // 101/102/103/104
			if inTxn {
				pending = append(pending, m)
			} else {
				applyMut(m)
			}
		}
	}
	if err := sc.Err(); err != nil {
		return nil, fmt.Errorf("accountant import %s: %w", path, err)
	}
	// A trailing unterminated transaction is aborted (classad_log.cpp:165-175):
	// pending mutations are simply discarded.
	return ads, nil
}

// mutation is one parsed journal record's effect (opcodes 101-104).
type mutation struct {
	op       int
	key      string
	attr     string
	value    any
	hasValue bool
}

// parseLogLine parses one journal line. It returns the mutation (for 101-104),
// the opcode kind, and whether the line was well-formed. Structural opcodes
// (105/106/107) return a zero mutation with ok=true.
func parseLogLine(line string) (mutation, int, bool) {
	lr := lineReader{s: line}
	opTok, ok := lr.word()
	if !ok {
		return mutation{}, 0, false
	}
	op, err := strconv.Atoi(opTok)
	if err != nil {
		return mutation{}, 0, false
	}
	switch op {
	case opBeginTransaction, opEndTransaction, opHistoricalSeqNum:
		return mutation{}, op, true
	case opNewClassAd:
		key, ok := lr.word()
		if !ok {
			return mutation{}, 0, false
		}
		// mytype and the obsolete targettype follow but are not needed.
		return mutation{op: op, key: key}, op, true
	case opDestroyClassAd:
		key, ok := lr.word()
		if !ok {
			return mutation{}, 0, false
		}
		return mutation{op: op, key: key}, op, true
	case opDeleteAttribute:
		key, ok1 := lr.word()
		attr, ok2 := lr.word()
		if !ok1 || !ok2 {
			return mutation{}, 0, false
		}
		return mutation{op: op, key: key, attr: attr}, op, true
	case opSetAttribute:
		key, ok1 := lr.word()
		attr, ok2 := lr.word()
		if !ok1 || !ok2 {
			return mutation{}, 0, false
		}
		v, hasValue := parseValue(lr.rest())
		return mutation{op: op, key: key, attr: attr, value: v, hasValue: hasValue}, op, true
	default:
		return mutation{}, 0, false
	}
}

// lineReader walks a journal line word by word, replicating LogRecord::readword:
// skip leading intra-line whitespace, read until the next whitespace, and
// consume that one terminating whitespace character.
type lineReader struct {
	s   string
	pos int
}

func isLineSpace(b byte) bool {
	// Newlines are already stripped by bufio's line scanner, so only the
	// horizontal whitespace that can appear within a record matters.
	return b == ' ' || b == '\t' || b == '\v' || b == '\f'
}

func (lr *lineReader) word() (string, bool) {
	for lr.pos < len(lr.s) && isLineSpace(lr.s[lr.pos]) {
		lr.pos++
	}
	start := lr.pos
	for lr.pos < len(lr.s) && !isLineSpace(lr.s[lr.pos]) {
		lr.pos++
	}
	if start == lr.pos {
		return "", false
	}
	w := lr.s[start:lr.pos]
	if lr.pos < len(lr.s) {
		lr.pos++ // consume the single terminating whitespace (readword semantics)
	}
	return w, true
}

// rest returns the remainder of the line verbatim (readline semantics for a
// SetAttribute value).
func (lr *lineReader) rest() string {
	return lr.s[lr.pos:]
}

// parseValue coerces a ClassAd rvalue token from the journal into one of the
// Store's scalar kinds. It returns ok=false for an absent/UNDEFINED value (the
// C++ LogSetAttribute stores literal "UNDEFINED" when the value is blank or
// unparseable, and LookupInteger/Float/String then all miss), so we simply skip
// setting the attribute.
func parseValue(raw string) (any, bool) {
	v := strings.TrimSpace(raw)
	if v == "" {
		return nil, false
	}
	if s, ok := unquote(v); ok {
		return s, true
	}
	if strings.EqualFold(v, "UNDEFINED") || strings.EqualFold(v, "ERROR") {
		return nil, false
	}
	// Bare integer (std::to_chars) vs. real (%f always has a decimal point).
	if i, err := strconv.ParseInt(v, 10, 64); err == nil {
		return i, true
	}
	if f, err := strconv.ParseFloat(v, 64); err == nil {
		return f, true
	}
	if strings.EqualFold(v, "true") {
		return int64(1), true
	}
	if strings.EqualFold(v, "false") {
		return int64(0), true
	}
	// Anything else (a bare, unquoted token) is kept as a string so no state is
	// silently dropped.
	return v, true
}

// unquote parses a double-quoted ClassAd string literal, honoring backslash
// escapes. It reports ok=false when v does not begin with a quote or is
// unterminated.
func unquote(v string) (string, bool) {
	if len(v) < 2 || v[0] != '"' {
		return "", false
	}
	var b strings.Builder
	for i := 1; i < len(v); i++ {
		c := v[i]
		if c == '\\' && i+1 < len(v) {
			switch n := v[i+1]; n {
			case '"', '\\', '\'':
				b.WriteByte(n)
			case 'n':
				b.WriteByte('\n')
			case 't':
				b.WriteByte('\t')
			case 'r':
				b.WriteByte('\r')
			default:
				b.WriteByte(n)
			}
			i++
			continue
		}
		if c == '"' {
			return b.String(), true
		}
		b.WriteByte(c)
	}
	return "", false // unterminated string literal
}

// splitCppKey maps a full C++ journal key onto its Store table and the
// prefix-stripped key. It reports ok=false for a key in none of the three
// accountant namespaces.
func splitCppKey(full string) (table, string, bool) {
	switch {
	case strings.HasPrefix(full, cppPrefixCustomer):
		return tableCustomer, full[len(cppPrefixCustomer):], true
	case strings.HasPrefix(full, cppPrefixResource):
		return tableResource, full[len(cppPrefixResource):], true
	case strings.HasPrefix(full, cppPrefixAcct):
		return tableAcct, full[len(cppPrefixAcct):], true
	}
	return 0, "", false
}

// loadCppAccountantLog parses a C++ Accountantnew.log and writes its final
// committed state into this Store through the normal set* path, so the imported
// values are also appended to the native transaction log (making the import
// durable and idempotent across restarts). It returns the number of records
// loaded. Intended to run against a freshly opened, empty Store.
func (s *Store) loadCppAccountantLog(path string) (int, error) {
	ads, err := ImportAccountantLog(path)
	if err != nil {
		return 0, err
	}
	// Load in deterministic key order for a stable native log.
	keys := make([]string, 0, len(ads))
	for k := range ads {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	n := 0
	for _, full := range keys {
		tbl, key, ok := splitCppKey(full)
		if !ok {
			continue
		}
		ad := ads[full]
		s.ensure(tbl, key) // materialize even an attribute-less ad
		attrs := make([]string, 0, len(ad.Attrs))
		for a := range ad.Attrs {
			attrs = append(attrs, a)
		}
		sort.Strings(attrs)
		for _, attr := range attrs {
			switch v := ad.Attrs[attr].(type) {
			case int64:
				s.setInt(tbl, key, attr, v)
			case float64:
				s.setFloat(tbl, key, attr, v)
			case string:
				s.setString(tbl, key, attr, v)
			}
		}
		n++
	}
	return n, nil
}
