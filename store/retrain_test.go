package store

import (
	"fmt"
	"testing"

	"github.com/PelicanPlatform/classad/classad"
)

// TestRetrainDictCompresses verifies that training the dictionary actually
// shrinks the stored footprint: a fresh collection uses the identity (no
// compression) codec, and RetrainDict must switch it to a ZSTD dictionary and
// recompact to a smaller live size. This is the difference between a compact
// collector and one that stores ads uncompressed.
func TestRetrainDictCompresses(t *testing.T) {
	st := New()
	const n = 5000
	for i := 0; i < n; i++ {
		// Realistic startd ads: unique identity, but lots of shared structure and
		// long common strings (the shape of a real pool), which a dictionary
		// trained across them compresses well.
		text := fmt.Sprintf(`[MyType="Machine"; Name="slot%d@host%d.pool.example.org"; `+
			`MyAddress="<10.0.%d.%d:9618>"; State="Unclaimed"; Activity="Idle"; `+
			`Arch="X86_64"; OpSys="LINUX"; OpSysAndVer="AlmaLinux9"; `+
			`CondorVersion="$CondorVersion: 24.0.0 2026-01-01 BuildID: 1 $"; `+
			`CondorPlatform="$CondorPlatform: x86_64_AlmaLinux9 $"; `+
			`Cpus=8; Memory=%d; Disk=%d; TotalMemory=64000; HasFileTransfer=true; `+
			`StarterAbilityList="HasFileTransfer,HasVM,HasReconnect,HasTDP"]`,
			i, i%64, (i/256)%256, i%256, 2000+i%512, 1000000+i%100000)
		ad, err := classad.Parse(text)
		if err != nil {
			t.Fatalf("parse ad %d: %v", i, err)
		}
		if err := st.Update(StartdAd, ad); err != nil {
			t.Fatalf("update ad %d: %v", i, err)
		}
	}

	before := st.Stats()[StartdAd]
	st.RetrainDict(n)
	after := st.Stats()[StartdAd]

	if after.Ads != before.Ads {
		t.Fatalf("RetrainDict changed the ad count: %d -> %d", before.Ads, after.Ads)
	}
	if after.LiveBytes() >= before.LiveBytes() {
		t.Fatalf("RetrainDict did not compress: %d -> %d live bytes", before.LiveBytes(), after.LiveBytes())
	}
	t.Logf("RetrainDict compressed StartdAd: %d -> %d live bytes (%.1fx)",
		before.LiveBytes(), after.LiveBytes(), float64(before.LiveBytes())/float64(after.LiveBytes()))
}
