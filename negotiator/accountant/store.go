package accountant

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"sort"
)

// table enumerates the three key namespaces of the accountant state store,
// mirroring the C++ ClassAdLog AccountantTable exactly:
//
//   - tableAcct     "Accountant."      singleton record, key "" (LastUpdateTime)
//   - tableCustomer "Customer.<name>"  per submitter AND per bare group name
//   - tableResource "Resource.<key>"   per match, keyed "<startdName>@<ip>"
//
// Keeping this shape makes the deferred Accountantnew.log importer a pure
// format adapter.
type table uint8

const (
	tableAcct table = iota
	tableCustomer
	tableResource
	numTables
)

func (t table) String() string {
	switch t {
	case tableAcct:
		return "Accountant"
	case tableCustomer:
		return "Customer"
	case tableResource:
		return "Resource"
	}
	return "?"
}

// record is one stored ClassAd-like entry: a map of attribute name to a typed
// scalar value. Values are always one of int64, float64, or string, matching
// the three attribute kinds the accountant persists.
type record struct {
	attrs map[string]any
}

func newRecord() *record { return &record{attrs: make(map[string]any)} }

func (r *record) clone() *record {
	c := newRecord()
	for k, v := range r.attrs {
		c.attrs[k] = v
	}
	return c
}

func (r *record) getFloat(attr string) (float64, bool) {
	v, ok := r.attrs[attr]
	if !ok {
		return 0, false
	}
	switch x := v.(type) {
	case float64:
		return x, true
	case int64:
		return float64(x), true
	}
	return 0, false
}

func (r *record) getInt(attr string) (int64, bool) {
	v, ok := r.attrs[attr]
	if !ok {
		return 0, false
	}
	switch x := v.(type) {
	case int64:
		return x, true
	case float64:
		return int64(x), true
	}
	return 0, false
}

func (r *record) getString(attr string) (string, bool) {
	v, ok := r.attrs[attr]
	if !ok {
		return "", false
	}
	s, ok := v.(string)
	return s, ok
}

// has reports whether the attribute is present (any kind).
func (r *record) has(attr string) bool {
	_, ok := r.attrs[attr]
	return ok
}

// Store is the accountant's persistent state store: an in-memory map of records
// in the three C++ namespaces, backed by an append-only, line-oriented
// transaction log for durability.
//
// Log format ("native", documented): one JSON object per line, each describing
// a single mutation applied in order. On Open the log is replayed to
// reconstruct the in-memory maps, then the file is reopened for append.
//
//	{"op":"set","tbl":1,"key":"alice@pool","attr":"Priority","kind":"f","f":0.5}
//	{"op":"set","tbl":1,"key":"alice@pool","attr":"BeginUsageTime","kind":"i","i":123}
//	{"op":"del","tbl":1,"key":"alice@pool"}
//
//	op   : "set" (set one attribute) or "del" (delete an entire record)
//	tbl  : table index (0=Accountant, 1=Customer, 2=Resource)
//	kind : value kind for a "set" -- "i" int64, "f" float64, "s" string
//	i/f/s: the value, whichever matches kind
//
// A path of "" makes the store memory-only (no file), which the unit tests use.
//
// The Store is safe for concurrent use; callers that need a compound sequence
// of operations to be atomic (e.g. the accountant's AddMatch) hold their own
// higher-level lock -- the Store never calls back into its callers, so there is
// no lock cycle.
type Store struct {
	tables [numTables]map[string]*record
	path   string
	f      *os.File
	w      *bufio.Writer
}

type logEntry struct {
	Op   string  `json:"op"`
	Tbl  int     `json:"tbl"`
	Key  string  `json:"key"`
	Attr string  `json:"attr,omitempty"`
	Kind string  `json:"kind,omitempty"`
	I    int64   `json:"i,omitempty"`
	F    float64 `json:"f,omitempty"`
	S    string  `json:"s,omitempty"`
}

// OpenStore opens (or creates) a state store. When path is "" the store is
// memory-only: nothing is read or written and Close is a no-op.
func OpenStore(path string) (*Store, error) {
	s := &Store{path: path}
	for i := range s.tables {
		s.tables[i] = make(map[string]*record)
	}
	if path == "" {
		return s, nil
	}
	if err := s.replay(path); err != nil {
		return nil, err
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return nil, err
	}
	s.f = f
	s.w = bufio.NewWriter(f)
	return s, nil
}

func (s *Store) replay(path string) error {
	f, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 0, 64*1024), 8*1024*1024)
	line := 0
	for sc.Scan() {
		line++
		b := sc.Bytes()
		if len(b) == 0 {
			continue
		}
		var e logEntry
		if err := json.Unmarshal(b, &e); err != nil {
			return fmt.Errorf("accountant store: corrupt log line %d: %w", line, err)
		}
		if e.Tbl < 0 || e.Tbl >= int(numTables) {
			return fmt.Errorf("accountant store: bad table %d on log line %d", e.Tbl, line)
		}
		tbl := table(e.Tbl)
		switch e.Op {
		case "del":
			delete(s.tables[tbl], e.Key)
		case "set":
			r := s.tables[tbl][e.Key]
			if r == nil {
				r = newRecord()
				s.tables[tbl][e.Key] = r
			}
			switch e.Kind {
			case "i":
				r.attrs[e.Attr] = e.I
			case "f":
				r.attrs[e.Attr] = e.F
			case "s":
				r.attrs[e.Attr] = e.S
			default:
				return fmt.Errorf("accountant store: bad kind %q on log line %d", e.Kind, line)
			}
		default:
			return fmt.Errorf("accountant store: bad op %q on log line %d", e.Op, line)
		}
	}
	return sc.Err()
}

func (s *Store) append(e *logEntry) {
	if s.w == nil {
		return
	}
	b, err := json.Marshal(e)
	if err != nil {
		return
	}
	s.w.Write(b)
	s.w.WriteByte('\n')
	// Flush eagerly so a concurrent reopen (tests, crash recovery) sees the
	// committed state. The OS page cache absorbs the cost.
	s.w.Flush()
}

func (s *Store) ensure(tbl table, key string) *record {
	r := s.tables[tbl][key]
	if r == nil {
		r = newRecord()
		s.tables[tbl][key] = r
	}
	return r
}

// setFloat / setInt / setString update the in-memory record and append the
// mutation to the log.
func (s *Store) setFloat(tbl table, key, attr string, v float64) {
	s.ensure(tbl, key).attrs[attr] = v
	s.append(&logEntry{Op: "set", Tbl: int(tbl), Key: key, Attr: attr, Kind: "f", F: v})
}

func (s *Store) setInt(tbl table, key, attr string, v int64) {
	s.ensure(tbl, key).attrs[attr] = v
	s.append(&logEntry{Op: "set", Tbl: int(tbl), Key: key, Attr: attr, Kind: "i", I: v})
}

func (s *Store) setString(tbl table, key, attr, v string) {
	s.ensure(tbl, key).attrs[attr] = v
	s.append(&logEntry{Op: "set", Tbl: int(tbl), Key: key, Attr: attr, Kind: "s", S: v})
}

func (s *Store) getFloat(tbl table, key, attr string) (float64, bool) {
	if r := s.tables[tbl][key]; r != nil {
		return r.getFloat(attr)
	}
	return 0, false
}

func (s *Store) getInt(tbl table, key, attr string) (int64, bool) {
	if r := s.tables[tbl][key]; r != nil {
		return r.getInt(attr)
	}
	return 0, false
}

func (s *Store) getString(tbl table, key, attr string) (string, bool) {
	if r := s.tables[tbl][key]; r != nil {
		return r.getString(attr)
	}
	return "", false
}

func (s *Store) getRecord(tbl table, key string) (*record, bool) {
	r, ok := s.tables[tbl][key]
	return r, ok
}

func (s *Store) deleteRecord(tbl table, key string) {
	if _, ok := s.tables[tbl][key]; !ok {
		return
	}
	delete(s.tables[tbl], key)
	s.append(&logEntry{Op: "del", Tbl: int(tbl), Key: key})
}

// forEach visits every record in a table in deterministic (sorted-key) order so
// that ReportState's numbered attributes are stable across runs. The callback
// returns false to stop iteration early.
func (s *Store) forEach(tbl table, fn func(key string, r *record) bool) {
	m := s.tables[tbl]
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		if !fn(k, m[k]) {
			return
		}
	}
}

func (s *Store) count(tbl table) int { return len(s.tables[tbl]) }

// Close flushes and closes the transaction log. It is a no-op for a
// memory-only store.
func (s *Store) Close() error {
	if s.f == nil {
		return nil
	}
	if s.w != nil {
		if err := s.w.Flush(); err != nil {
			return err
		}
	}
	if err := s.f.Sync(); err != nil && err != io.EOF {
		// Sync failures on some filesystems are non-fatal for our purposes.
	}
	err := s.f.Close()
	s.f = nil
	s.w = nil
	return err
}
