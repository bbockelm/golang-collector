package store

import "testing"

func TestVersionFromText(t *testing.T) {
	cases := []struct {
		name       string
		text       string
		wantOK     bool
		start, seq int64
	}{
		{"both present", "Name = \"slot1@h\"\nDaemonStartTime = 1700000000\nUpdateSequenceNumber = 42\n", true, 1700000000, 42},
		{"order independent", "UpdateSequenceNumber = 7\nName = \"x\"\nDaemonStartTime = 100\n", true, 100, 7},
		{"missing seq", "Name = \"x\"\nDaemonStartTime = 100\n", false, 0, 0},
		{"missing start", "Name = \"x\"\nUpdateSequenceNumber = 7\n", false, 0, 0},
		{"non-integer", "DaemonStartTime = 100\nUpdateSequenceNumber = \"nope\"\n", false, 0, 0},
		{"case-insensitive name", "daemonstarttime = 5\nupdatesequencenumber = 9\n", true, 5, 9},
		{"no trailing newline", "DaemonStartTime = 3\nUpdateSequenceNumber = 4", true, 3, 4},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			v := versionFromText(tc.text)
			if v.ok != tc.wantOK {
				t.Fatalf("ok = %v, want %v", v.ok, tc.wantOK)
			}
			if tc.wantOK && (v.startTime != tc.start || v.seqNo != tc.seq) {
				t.Fatalf("got (%d,%d), want (%d,%d)", v.startTime, v.seqNo, tc.start, tc.seq)
			}
		})
	}
}

func TestAdVersionNewerThan(t *testing.T) {
	v := func(s, n int64) adVersion { return adVersion{startTime: s, seqNo: n, ok: true} }
	none := adVersion{}
	cases := []struct {
		name string
		a, b adVersion
		want bool
	}{
		{"higher seq same start", v(100, 5), v(100, 4), true},
		{"lower seq same start", v(100, 4), v(100, 5), false},
		{"equal is not newer", v(100, 5), v(100, 5), false},
		{"restart dominates wrapped seq", v(200, 0), v(100, 999999), true},
		{"older start loses despite high seq", v(100, 999999), v(200, 0), false},
		{"versioned beats unversioned stored", v(1, 1), none, true},
		{"unversioned incoming is never newer", none, v(1, 1), false},
		{"unversioned vs unversioned", none, none, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := tc.a.newerThan(tc.b); got != tc.want {
				t.Fatalf("newerThan = %v, want %v", got, tc.want)
			}
		})
	}
}
