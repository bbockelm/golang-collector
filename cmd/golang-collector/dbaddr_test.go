package main

import (
	"os"
	"path/filepath"
	"testing"
)

func TestReadDBAddressFile(t *testing.T) {
	dir := t.TempDir()
	p := filepath.Join(dir, ".htcondordb_address")
	if err := os.WriteFile(p, []byte("\n<127.0.0.1:9618?sock=htcondordb_10670_4f9c>\nsecond line\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	got, err := readDBAddressFile(p)
	if err != nil {
		t.Fatal(err)
	}
	if want := "<127.0.0.1:9618?sock=htcondordb_10670_4f9c>"; got != want {
		t.Fatalf("got %q want %q (first non-empty line, brackets kept)", got, want)
	}
	// Re-read reflects a rewrite (the restart-changes-address case).
	if err := os.WriteFile(p, []byte("<127.0.0.1:9618?sock=NEW>\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if got, _ := readDBAddressFile(p); got != "<127.0.0.1:9618?sock=NEW>" {
		t.Fatalf("re-read did not reflect the new address: %q", got)
	}
	// Missing + empty are errors.
	if _, err := readDBAddressFile(filepath.Join(dir, "nope")); err == nil {
		t.Error("missing file should error")
	}
	empty := filepath.Join(dir, "empty")
	_ = os.WriteFile(empty, []byte("\n  \n"), 0o644)
	if _, err := readDBAddressFile(empty); err == nil {
		t.Error("empty file should error")
	}
}
