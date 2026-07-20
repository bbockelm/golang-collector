package store

import (
	"errors"
	"fmt"
	"testing"

	"github.com/PelicanPlatform/classad/dbrpc"
)

// TestNoSuchTxnIsTransient guards the retry decision behind the "no such
// transaction" fix: that error must be recognized and classified as NOT permanent,
// so withRetry drops the connection and replays the batch on a fresh transaction
// (instead of surfacing it or silently dropping the remaining ads). A different
// ServerError (a genuine ad-level rejection) stays permanent.
func TestNoSuchTxnIsTransient(t *testing.T) {
	noTxn := &dbrpc.ServerError{Msg: "no such transaction"}
	badAd := &dbrpc.ServerError{Msg: "parse error at line 1: bad value"}

	if !isNoSuchTxn(noTxn) {
		t.Error("isNoSuchTxn should recognize the server's no-such-transaction error")
	}
	if isNoSuchTxn(badAd) {
		t.Error("isNoSuchTxn should not match an ad-level rejection")
	}
	if isPermanent(noTxn) {
		t.Error("no-such-transaction must be transient (retried), not permanent")
	}
	if !isPermanent(badAd) {
		t.Error("an ad-level ServerError must stay permanent (surfaced, not retried)")
	}
	// Wrapped errors must still classify.
	wrapped := fmt.Errorf("putBatchTx: %w", noTxn)
	if !isNoSuchTxn(wrapped) || isPermanent(wrapped) {
		t.Error("wrapped no-such-transaction must classify as transient")
	}
	// A non-ServerError (transport) is neither.
	if isNoSuchTxn(errors.New("connection reset")) || isPermanent(errors.New("connection reset")) {
		t.Error("a transport error is neither no-such-txn nor permanent")
	}
}
