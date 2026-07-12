package source

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/bbockelm/golang-collector/store"
)

func TestSplitAddrs(t *testing.T) {
	cases := []struct {
		in   string
		want []string
	}{
		{"", nil},
		{"   ", nil},
		{"host:9618", []string{"host:9618"}},
		{"a:1,b:2", []string{"a:1", "b:2"}},
		{" a:1 , b:2 ", []string{"a:1", "b:2"}},
		{"a:1 b:2", []string{"a:1", "b:2"}},
		{"a:1,,b:2", []string{"a:1", "b:2"}},
		// A sinful with space-separated CCB contacts inside the brackets must
		// stay intact (top-level whitespace only is a separator).
		{"<1.2.3.4:9618?a=b> <5.6.7.8:9618>", []string{"<1.2.3.4:9618?a=b>", "<5.6.7.8:9618>"}},
		{"<1.2.3.4:9618 ccb=x>,plain:9618", []string{"<1.2.3.4:9618 ccb=x>", "plain:9618"}},
	}
	for _, c := range cases {
		got := splitAddrs(c.in)
		if len(got) != len(c.want) {
			t.Errorf("splitAddrs(%q) = %v, want %v", c.in, got, c.want)
			continue
		}
		for i := range got {
			if got[i] != c.want[i] {
				t.Errorf("splitAddrs(%q)[%d] = %q, want %q", c.in, i, got[i], c.want[i])
			}
		}
	}
}

// startDeadCollector stands up a TCP listener that accepts and immediately
// closes every connection, so any handshake/query against it fails. Returns the
// address plus a stop func.
func startDeadCollector(t *testing.T) (string, func()) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			_ = conn.Close()
		}
	}()
	return ln.Addr().String(), func() { close(done); ln.Close() }
}

// TestQueryPrivateAdsFailover points a RemoteSource at a two-collector list
// whose first entry is dead and asserts the private-ad query fails over to the
// second (live) collector and still returns the seeded private ad.
func TestQueryPrivateAdsFailover(t *testing.T) {
	st := store.New()
	seedRemotePool(t, st)

	deadAddr, stopDead := startDeadCollector(t)
	defer stopDead()
	goodAddr, stopGood := startTestCollector(t, st)
	defer stopGood()

	src, err := NewRemote(Config{
		CollectorAddr: deadAddr + ", " + goodAddr,
		Security:      plaintextSec(),
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	ads, err := src.queryPrivateAds(ctx)
	if err != nil {
		t.Fatalf("queryPrivateAds with failover: %v", err)
	}
	if len(ads) != 1 {
		t.Fatalf("private ads = %d, want 1 (failed over to live collector)", len(ads))
	}
	if name, _ := ads[0].EvaluateAttrString("Name"); name != "slot1@big" {
		t.Errorf("private ad name = %q, want slot1@big", name)
	}
}

// TestQueryPrivateAdsAllDead asserts a list of only-dead collectors surfaces an
// error rather than hanging or panicking.
func TestQueryPrivateAdsAllDead(t *testing.T) {
	dead1, stop1 := startDeadCollector(t)
	defer stop1()
	dead2, stop2 := startDeadCollector(t)
	defer stop2()

	src, err := NewRemote(Config{
		CollectorAddr: dead1 + " " + dead2,
		Security:      plaintextSec(),
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if _, err := src.queryPrivateAds(ctx); err == nil {
		t.Fatal("queryPrivateAds over only-dead collectors: want error, got nil")
	}
}
