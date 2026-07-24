package store

import (
	"context"
	"errors"
	"strconv"
	"strings"

	"github.com/PelicanPlatform/classad/db"
	"github.com/PelicanPlatform/classad/dbrpc"
)

// adVersion orders daemon ad updates for conflict resolution: DaemonStartTime (bumped on
// restart, so it dominates a wrapped UpdateSequenceNumber) then UpdateSequenceNumber.
// Higher is fresher. ok is false when either attribute is absent or non-integer -- then
// the ad carries no usable version and a conflict on it is left to the concurrent winner
// (self-healing on the next advertise).
type adVersion struct {
	startTime int64
	seqNo     int64
	ok        bool
}

// newerThan reports whether a is a strictly fresher update than b. An unversioned b loses
// to any versioned a (a real sequence beats an ad that carries none); an unversioned a is
// never "newer" (its caller must not force a re-write it cannot justify).
func (a adVersion) newerThan(b adVersion) bool {
	if !a.ok {
		return false
	}
	if !b.ok {
		return true
	}
	if a.startTime != b.startTime {
		return a.startTime > b.startTime
	}
	return a.seqNo > b.seqNo
}

// versionFromText reads (DaemonStartTime, UpdateSequenceNumber) from an old-ClassAd body.
func versionFromText(text string) adVersion {
	st, ok1 := intAttrFromText(text, attrDaemonStartTime)
	sq, ok2 := intAttrFromText(text, attrUpdateSequenceNumber)
	return adVersion{startTime: st, seqNo: sq, ok: ok1 && ok2}
}

// intAttrFromText scans an old-ClassAd body for `name = <int>` (case-insensitive on the
// attribute name) and returns the first integer value. A cheap line scan run only on the
// rare conflict path, so it does not warrant a full parse.
func intAttrFromText(text, name string) (int64, bool) {
	lname := strings.ToLower(name)
	for len(text) > 0 {
		var line string
		if nl := strings.IndexByte(text, '\n'); nl >= 0 {
			line, text = text[:nl], text[nl+1:]
		} else {
			line, text = text, ""
		}
		eq := strings.IndexByte(line, '=')
		if eq < 0 {
			continue
		}
		if strings.ToLower(strings.TrimSpace(line[:eq])) != lname {
			continue
		}
		v, err := strconv.ParseInt(strings.TrimSpace(line[eq+1:]), 10, 64)
		if err != nil {
			return 0, false
		}
		return v, true
	}
	return 0, false
}

// resolveConflicts handles a batch-commit conflict by update sequence rather than dropping
// the conflicted keys. For each conflicted key it reads the currently-stored ad and
// re-commits ours only if ours is strictly newer (by adVersion); otherwise the concurrent
// winner stands. Best-effort: any lookup or re-commit failure simply leaves the stored
// value (the daemon re-advertises within an update cycle). The re-commit absorbs a fresh
// conflict rather than re-resolving, so resolution is bounded to a single round.
func (b *RPCBackend) resolveConflicts(ctx context.Context, table string, conflictErr error, items []keyedText) {
	var ce *db.ConflictError
	if !errors.As(conflictErr, &ce) || len(ce.Keys) == 0 {
		return
	}
	conflicted := make(map[string]struct{}, len(ce.Keys))
	for _, k := range ce.Keys {
		conflicted[k] = struct{}{}
	}
	ours := make(map[string]string, len(ce.Keys))
	for _, it := range items {
		if _, ok := conflicted[it.key]; ok {
			ours[it.key] = it.text
		}
	}
	var rewrite []keyedText
	for _, key := range ce.Keys {
		text, ok := ours[key]
		if !ok {
			continue // not in our batch (shouldn't happen); nothing to resolve
		}
		ourVer := versionFromText(text)
		if !ourVer.ok {
			continue // ours has no usable version -> leave the concurrent winner in place
		}
		stored, present := b.lookupText(ctx, table, key)
		if present && !ourVer.newerThan(versionFromText(stored)) {
			continue // stored is newer-or-equal -> keep it (drop ours)
		}
		rewrite = append(rewrite, keyedText{key: key, text: text})
	}
	if len(rewrite) == 0 {
		return // every conflict resolved in favor of the stored ad
	}
	// Re-commit only the ads that are strictly newer. resolve=false so a fresh conflict on
	// this small write is absorbed (counted + dropped), never recursively re-resolved.
	_ = b.withRetry(ctx, b.writeLane(), func(cl *dbrpc.Client) error {
		return b.putBatchTx(ctx, cl, table, rewrite, false)
	})
}

// lookupText returns the currently-stored ad body for key in table, on a read lane.
func (b *RPCBackend) lookupText(ctx context.Context, table, key string) (string, bool) {
	var text string
	var present bool
	_ = b.withRetry(ctx, b.readLane(), func(cl *dbrpc.Client) error {
		tx, err := cl.BeginTable(ctx, table)
		if err != nil {
			return err
		}
		defer abortDetached(ctx, tx)
		t, p, err := tx.LookupClassAd(ctx, key)
		if err != nil {
			return err
		}
		text, present = t, p
		return nil
	})
	return text, present
}
