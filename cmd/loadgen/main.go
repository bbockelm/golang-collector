// Command loadgen drives a read workload against a collector and reports its
// throughput, so several loadgen processes can be run against one collector to
// measure the server's networked scaling with client CPU spread across processes
// (a single in-process client caps throughput by how fast it can consume ad bytes).
//
// It prints one machine-readable result line to stdout:
//
//	RESULT ads=<n> ops=<n> secs=<f>
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/PelicanPlatform/classad/classad"
	"github.com/bbockelm/cedar/client"
	"github.com/bbockelm/cedar/commands"
	"github.com/bbockelm/cedar/message"
	"github.com/bbockelm/cedar/security"
)

func main() {
	addr := flag.String("addr", "", "collector address host:port")
	conc := flag.Int("conc", 1, "concurrent connections in this process")
	dur := flag.Duration("dur", 5*time.Second, "run duration")
	query := flag.String("query", "true", "match constraint (Requirements)")
	flag.Parse()
	if *addr == "" {
		fmt.Fprintln(os.Stderr, "loadgen: -addr required")
		os.Exit(2)
	}

	queryAd := fmt.Sprintf(`[MyType="Query"; TargetType="Machine"; Requirements = %s]`, *query)
	deadline := time.Now().Add(*dur)

	var ads, ops int64
	start := time.Now()
	var wg sync.WaitGroup
	for w := 0; w < *conc; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for time.Now().Before(deadline) {
				n, err := drainQuery(*addr, queryAd)
				if err != nil {
					fmt.Fprintln(os.Stderr, "loadgen:", err)
					return
				}
				atomic.AddInt64(&ads, int64(n))
				atomic.AddInt64(&ops, 1)
			}
		}()
	}
	wg.Wait()
	secs := time.Since(start).Seconds()
	fmt.Printf("RESULT ads=%d ops=%d secs=%f\n", ads, ops, secs)
}

func plaintextSec() *security.SecurityConfig {
	return &security.SecurityConfig{
		AuthMethods:    []security.AuthMethod{},
		Authentication: security.SecurityNever,
		Encryption:     security.SecurityNever,
		Integrity:      security.SecurityNever,
	}
}

// drainQuery runs one QUERY_STARTD_ADS and drains the response ads with
// SkipClassAdRaw (no AST, no ad text materialized), returning the ad count.
func drainQuery(addr, queryAd string) (int, error) {
	ctx := context.Background()
	sec := plaintextSec()
	sec.Command = commands.QUERY_STARTD_ADS
	cl, err := client.ConnectAndAuthenticate(ctx, addr, sec)
	if err != nil {
		return 0, err
	}
	defer cl.Close()
	st := cl.GetStream()

	ad, err := classad.Parse(queryAd)
	if err != nil {
		return 0, err
	}
	req := message.NewMessageForStream(st)
	if err := req.PutClassAd(ctx, ad); err != nil {
		return 0, err
	}
	if err := req.FinishMessage(ctx); err != nil {
		return 0, err
	}

	resp := message.NewMessageFromStream(st)
	n := 0
	for {
		more, err := resp.GetInt(ctx)
		if err != nil {
			return n, err
		}
		if more == 0 {
			return n, nil
		}
		if err := resp.SkipClassAdRaw(ctx); err != nil {
			return n, err
		}
		n++
	}
}
