package store

import (
	"strings"
	"testing"
)

// TestSanitizeAdTextRedactsPrivateAttrs verifies private (secret) attributes are
// redacted by NAME -- the value of ClaimId/Capability/ChildClaimIds is replaced
// while the attribute name and every non-secret line survive for debugging.
func TestSanitizeAdTextRedactsPrivateAttrs(t *testing.T) {
	in := `Name = "slot1@h"
MyType = "Machine"
MyAddress = "<128.104.100.17:9618?sock=startd_1>"
ClaimId = "<128.104.100.17:9618>#1783747566#19094#[Integrity=\"YES\";CryptoMethods=\"BLOWFISH\";]409352deadbeef883a"
Capability = "abc#123#[secret]cafef00d"
ChildClaimIds = "kid1,kid2,kid3"
LastHeardFrom = 1784498774`

	out := SanitizeAdText(in)

	// The claim id / capability values must be gone.
	for _, secret := range []string{"409352deadbeef883a", "BLOWFISH", "cafef00d", "kid1,kid2,kid3", "#1783747566#19094#"} {
		if strings.Contains(out, secret) {
			t.Errorf("sanitized ad still leaks %q:\n%s", secret, out)
		}
	}
	// The private attribute NAMES are kept (redacted value) -- useful to see that a
	// ClaimId was present without exposing it.
	for _, keep := range []string{
		`Name = "slot1@h"`,
		`MyType = "Machine"`,
		`MyAddress = "<128.104.100.17:9618?sock=startd_1>"`,
		"LastHeardFrom = 1784498774",
		"ClaimId =", "Capability =", "ChildClaimIds =",
	} {
		if !strings.Contains(out, keep) {
			t.Errorf("sanitized ad dropped %q:\n%s", keep, out)
		}
	}
	if !strings.Contains(out, "<redacted>") {
		t.Errorf("expected <redacted> markers:\n%s", out)
	}
}

// TestAdExcerptSanitizes guards that the logging entrypoint (used at every reject
// site) redacts -- not just the helper.
func TestAdExcerptSanitizes(t *testing.T) {
	in := `Name = "slot1@h"` + "\n" + `ClaimId = "<addr>#1#2#[x]topsecrethash"`
	out := AdExcerpt(in)
	if strings.Contains(out, "topsecrethash") {
		t.Errorf("AdExcerpt leaked the claim id:\n%s", out)
	}
	if !strings.Contains(out, `Name = "slot1@h"`) {
		t.Errorf("public Name should be kept:\n%s", out)
	}
}

// TestSanitizeAdTextKeepsMalformed checks that a line with no '=' (a stray token
// that broke a parse, e.g. "ZKM") is preserved as-is -- it carries no value to
// leak and is a useful clue. Redaction is name-based only; a secret smuggled into
// a non-private attribute's value by upstream corruption is out of scope here (it
// is prevented at the source -- see the cedar raw-ClassAd read guard).
func TestSanitizeAdTextKeepsMalformed(t *testing.T) {
	in := "Name = \"slot1@h\"\nZKM\nLastHeardFrom = 123"
	out := SanitizeAdText(in)
	for _, keep := range []string{`Name = "slot1@h"`, "ZKM", "LastHeardFrom = 123"} {
		if !strings.Contains(out, keep) {
			t.Errorf("dropped %q:\n%s", keep, out)
		}
	}
}
