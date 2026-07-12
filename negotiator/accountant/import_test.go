package accountant

import (
	"os"
	"path/filepath"
	"testing"
)

// fixtureLog is a hand-written C++ Accountantnew.log (ClassAdLog journal)
// exercising every opcode: NewClassAd (101), SetAttribute (103) for a Customer
// (Priority/PriorityFactor/WeightedAccumulatedUsage/Ceiling), a Resource
// (RemoteUser/SlotWeight/StartTime), the Accountant. singleton (LastUpdateTime),
// DeleteAttribute (104), DestroyClassAd (102), a committed transaction
// (105..106), and a trailing UNCOMMITTED transaction that must be ignored. It
// also opens with a HistoricalSequenceNumber header (107). Value encodings match
// ClassAdLogAccountantDB.cpp: bare ints, "%f" floats, double-quoted strings.
const fixtureLog = `107 1 CreationTimestamp 1700000000
101 Customer.alice@dom * (empty)
103 Customer.alice@dom Priority 10.000000
103 Customer.alice@dom PriorityFactor 1000.000000
103 Customer.alice@dom WeightedAccumulatedUsage 42.500000
103 Customer.alice@dom Ceiling 5
103 Customer.alice@dom TempAttr 99
104 Customer.alice@dom TempAttr
101 Resource.slot1@1.2.3.4 * (empty)
103 Resource.slot1@1.2.3.4 RemoteUser "alice@dom"
103 Resource.slot1@1.2.3.4 SlotWeight 2.000000
103 Resource.slot1@1.2.3.4 StartTime 1699999999
101 Accountant. * (empty)
103 Accountant. LastUpdateTime 1700000123
105
101 Customer.bob@dom * (empty)
103 Customer.bob@dom Priority 3.000000
103 Customer.bob@dom PriorityFactor 1000.000000
103 Customer.bob@dom Ceiling 9
106
101 Customer.carol@dom * (empty)
103 Customer.carol@dom Priority 7.000000
102 Customer.carol@dom
105
101 Customer.mallory@dom * (empty)
103 Customer.mallory@dom Priority 1.000000
`

func writeFixture(t *testing.T, contents string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "Accountantnew.log")
	if err := os.WriteFile(path, []byte(contents), 0o644); err != nil {
		t.Fatalf("write fixture: %v", err)
	}
	return path
}

func TestImportAccountantLog_Parse(t *testing.T) {
	ads, err := ImportAccountantLog(writeFixture(t, fixtureLog))
	if err != nil {
		t.Fatalf("ImportAccountantLog: %v", err)
	}

	alice := ads["Customer.alice@dom"]
	if alice == nil {
		t.Fatal("Customer.alice@dom missing")
	}
	// Numeric kinds: "%f" floats -> float64, bare ints -> int64.
	if v, ok := alice.Attrs["Priority"].(float64); !ok || v != 10.0 {
		t.Errorf("alice Priority = %v (%T), want float64 10", alice.Attrs["Priority"], alice.Attrs["Priority"])
	}
	if v, ok := alice.Attrs["PriorityFactor"].(float64); !ok || v != 1000.0 {
		t.Errorf("alice PriorityFactor = %v (%T), want float64 1000", alice.Attrs["PriorityFactor"], alice.Attrs["PriorityFactor"])
	}
	if v, ok := alice.Attrs["WeightedAccumulatedUsage"].(float64); !ok || v != 42.5 {
		t.Errorf("alice WeightedAccumulatedUsage = %v (%T), want float64 42.5", alice.Attrs["WeightedAccumulatedUsage"], alice.Attrs["WeightedAccumulatedUsage"])
	}
	if v, ok := alice.Attrs["Ceiling"].(int64); !ok || v != 5 {
		t.Errorf("alice Ceiling = %v (%T), want int64 5", alice.Attrs["Ceiling"], alice.Attrs["Ceiling"])
	}
	// DeleteAttribute (104) removed TempAttr.
	if _, present := alice.Attrs["TempAttr"]; present {
		t.Errorf("alice TempAttr should have been deleted, got %v", alice.Attrs["TempAttr"])
	}

	res := ads["Resource.slot1@1.2.3.4"]
	if res == nil {
		t.Fatal("Resource.slot1@1.2.3.4 missing")
	}
	if v, ok := res.Attrs["RemoteUser"].(string); !ok || v != "alice@dom" {
		t.Errorf("resource RemoteUser = %v (%T), want string alice@dom", res.Attrs["RemoteUser"], res.Attrs["RemoteUser"])
	}
	if v, ok := res.Attrs["SlotWeight"].(float64); !ok || v != 2.0 {
		t.Errorf("resource SlotWeight = %v (%T), want float64 2", res.Attrs["SlotWeight"], res.Attrs["SlotWeight"])
	}
	if v, ok := res.Attrs["StartTime"].(int64); !ok || v != 1699999999 {
		t.Errorf("resource StartTime = %v (%T), want int64 1699999999", res.Attrs["StartTime"], res.Attrs["StartTime"])
	}

	// Accountant. singleton (key keeps the trailing dot in journal form).
	acct := ads["Accountant."]
	if acct == nil {
		t.Fatal("Accountant. singleton missing")
	}
	if v, ok := acct.Attrs["LastUpdateTime"].(int64); !ok || v != 1700000123 {
		t.Errorf("Accountant LastUpdateTime = %v (%T), want int64 1700000123", acct.Attrs["LastUpdateTime"], acct.Attrs["LastUpdateTime"])
	}

	// Committed transaction (bob) applied.
	bob := ads["Customer.bob@dom"]
	if bob == nil {
		t.Fatal("Customer.bob@dom (committed transaction) missing")
	}
	if v, ok := bob.Attrs["Ceiling"].(int64); !ok || v != 9 {
		t.Errorf("bob Ceiling = %v (%T), want int64 9", bob.Attrs["Ceiling"], bob.Attrs["Ceiling"])
	}

	// DestroyClassAd (102) removed carol.
	if _, present := ads["Customer.carol@dom"]; present {
		t.Error("Customer.carol@dom should have been destroyed")
	}
	// Trailing UNCOMMITTED transaction (mallory) ignored.
	if _, present := ads["Customer.mallory@dom"]; present {
		t.Error("Customer.mallory@dom is in an uncommitted trailing transaction; must be ignored")
	}
}

func TestImportAccountantLog_RoundTripThroughStore(t *testing.T) {
	path := writeFixture(t, fixtureLog)
	cfg := DefaultConfig()
	cfg.ImportFrom = path // LogFile "" -> memory-only native store
	a, err := New(cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { a.Close() })

	// Imported submitter surfaces its imported priority (real 10) x factor
	// (1000), NOT the 0.5 new-submitter default.
	almost(t, "GetPriority(alice) effective", a.GetPriority("alice@dom"), 10.0*1000.0)
	almost(t, "GetPriorityFactor(alice)", a.GetPriorityFactor("alice@dom"), 1000.0)
	almost(t, "GetCeiling(alice)", a.GetCeiling("alice@dom"), 5.0)

	// WeightedResourcesUsed is reconciled at startup from the imported Resource
	// records attributed to alice (one slot, SlotWeight 2).
	almost(t, "GetWeightedResourcesUsed(alice)", a.GetWeightedResourcesUsed("alice@dom"), 2.0)

	// A submitter that was NOT imported still gets the fresh-submitter default.
	almost(t, "GetPriority(zoe) default", a.GetPriority("zoe@dom"), MinPriority*1000.0)
}

func TestImportAccountantLog_IdempotencyGuard(t *testing.T) {
	src := writeFixture(t, fixtureLog)
	dbPath := filepath.Join(t.TempDir(), "GoAccountant.log")

	// First run: imports into a fresh, file-backed native store.
	cfg := DefaultConfig()
	cfg.LogFile = dbPath
	cfg.ImportFrom = src
	a1, err := New(cfg)
	if err != nil {
		t.Fatalf("first New: %v", err)
	}
	almost(t, "run1 GetPriority(bob)", a1.GetPriority("bob@dom"), 3.0*1000.0)
	if err := a1.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Remove the source log. A second run whose native store is already
	// populated must SKIP the import (idempotency guard on count(Customer)==0),
	// so the missing source is never touched and startup still succeeds with the
	// previously imported state intact.
	if err := os.Remove(src); err != nil {
		t.Fatalf("remove source: %v", err)
	}
	a2, err := New(cfg)
	if err != nil {
		t.Fatalf("second New should skip import and succeed, got: %v", err)
	}
	t.Cleanup(func() { a2.Close() })
	almost(t, "run2 GetPriority(bob) persisted", a2.GetPriority("bob@dom"), 3.0*1000.0)
	almost(t, "run2 GetPriority(alice) persisted", a2.GetPriority("alice@dom"), 10.0*1000.0)
}

func TestImportAccountantLog_TruncatedTail(t *testing.T) {
	// A log whose final record is cut off mid-line: the good records before it
	// apply, and the truncated tail is ignored (no error, no panic), matching
	// the C++ ClassAdLog "ignore the tail" recovery.
	truncated := `101 Customer.alice@dom * (empty)
103 Customer.alice@dom Priority 4.000000
103 Customer.bob@dom Prior`
	ads, err := ImportAccountantLog(writeFixture(t, truncated))
	if err != nil {
		t.Fatalf("truncated import should not error: %v", err)
	}
	if ads["Customer.alice@dom"] == nil {
		t.Error("alice (before the truncation) should have been applied")
	}
	if v, ok := ads["Customer.alice@dom"].Attrs["Priority"].(float64); !ok || v != 4.0 {
		t.Errorf("alice Priority = %v, want 4", ads["Customer.alice@dom"].Attrs["Priority"])
	}
}

func TestImportAccountantLog_MalformedOpcode(t *testing.T) {
	// A garbage opcode line stops parsing gracefully; earlier records survive.
	garbage := `101 Customer.alice@dom * (empty)
103 Customer.alice@dom Priority 6.000000
999 this is not a real opcode
103 Customer.bob@dom Priority 1.000000
`
	ads, err := ImportAccountantLog(writeFixture(t, garbage))
	if err != nil {
		t.Fatalf("malformed import should not error: %v", err)
	}
	if ads["Customer.alice@dom"] == nil {
		t.Error("alice (before the bad line) should have been applied")
	}
	if _, present := ads["Customer.bob@dom"]; present {
		t.Error("records after the bad line should be ignored (tail dropped)")
	}
}

func TestImportAccountantLog_UncommittedThenCommitted(t *testing.T) {
	// A committed transaction after an aborted one still applies. Also confirms
	// a bare (out-of-transaction) SetAttribute applies immediately.
	log := `103 Customer.a@dom Priority 2.000000
105
103 Customer.b@dom Priority 3.000000
106
`
	ads, err := ImportAccountantLog(writeFixture(t, log))
	if err != nil {
		t.Fatalf("import: %v", err)
	}
	if ads["Customer.a@dom"] == nil || ads["Customer.a@dom"].Attrs["Priority"].(float64) != 2.0 {
		t.Errorf("bare SetAttribute should apply immediately: %+v", ads["Customer.a@dom"])
	}
	if ads["Customer.b@dom"] == nil || ads["Customer.b@dom"].Attrs["Priority"].(float64) != 3.0 {
		t.Errorf("committed transaction should apply: %+v", ads["Customer.b@dom"])
	}
}

func TestImportAccountantLog_MissingFile(t *testing.T) {
	if _, err := ImportAccountantLog(filepath.Join(t.TempDir(), "nope.log")); err == nil {
		t.Error("expected an error importing a nonexistent file")
	}
}
