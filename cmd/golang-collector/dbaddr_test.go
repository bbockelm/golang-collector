package main

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/bbockelm/golang-htcondor/config"
)

// The first-non-empty-line / re-read-on-every-call behavior of the address file itself is
// covered by golang-htcondor's locate_test.go. These tests cover the collector-specific
// precedence layered on top of those primitives.

func TestDBAddressFilePathPrecedence(t *testing.T) {
	// COLLECTOR_DB_ADDRESS_FILE is the collector override and wins outright.
	cfg := config.NewEmpty()
	cfg.Set("COLLECTOR_DB_ADDRESS_FILE", "/custom/db.addr")
	cfg.Set("HTCONDORDB_ADDRESS_FILE", "/other/htcondordb.addr")
	cfg.Set("LOG", "/var/log/condor")
	if got := dbAddressFilePath(cfg); got != "/custom/db.addr" {
		t.Errorf("override: got %q want /custom/db.addr", got)
	}

	// Without the override, defer to htcondordb's own knob.
	cfg = config.NewEmpty()
	cfg.Set("HTCONDORDB_ADDRESS_FILE", "/other/htcondordb.addr")
	if got := dbAddressFilePath(cfg); got != "/other/htcondordb.addr" {
		t.Errorf("htcondordb knob: got %q want /other/htcondordb.addr", got)
	}

	// Falling all the way through to the $(LOG)/.htcondordb_address default.
	cfg = config.NewEmpty()
	cfg.Set("LOG", "/var/log/condor")
	if got, want := dbAddressFilePath(cfg), "/var/log/condor/.htcondordb_address"; got != want {
		t.Errorf("LOG default: got %q want %q", got, want)
	}
	// Note: config.NewEmpty() still carries a compiled-in LOG default, so the path is
	// always resolvable in practice; there is no reliable "empty" case to assert here.
}

func TestDBAddrResolver(t *testing.T) {
	// COLLECTOR_DB_HOST is a static override -- no file, source names the knob.
	cfg := config.NewEmpty()
	cfg.Set("COLLECTOR_DB_HOST", "  db.example.org:9618  ")
	resolve, source, err := dbAddrResolver(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if source != "COLLECTOR_DB_HOST" {
		t.Errorf("static source: got %q", source)
	}
	if got, _ := resolve(); got != "db.example.org:9618" {
		t.Errorf("static host: got %q (want trimmed)", got)
	}

	// File-based: the resolver re-reads the current address on each call.
	dir := t.TempDir()
	af := filepath.Join(dir, ".htcondordb_address")
	if err := os.WriteFile(af, []byte("<127.0.0.1:9618?sock=first>\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg = config.NewEmpty()
	cfg.Set("COLLECTOR_DB_ADDRESS_FILE", af)
	resolve, source, err = dbAddrResolver(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if source != "address file "+af {
		t.Errorf("file source: got %q", source)
	}
	if got, _ := resolve(); got != "<127.0.0.1:9618?sock=first>" {
		t.Errorf("file resolve: got %q", got)
	}
	if err := os.WriteFile(af, []byte("<127.0.0.1:9618?sock=RESTARTED>\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if got, _ := resolve(); got != "<127.0.0.1:9618?sock=RESTARTED>" {
		t.Errorf("resolver did not re-read after restart: got %q", got)
	}
}
