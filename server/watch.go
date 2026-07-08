package server

import (
	"context"
	"fmt"
	"time"

	"github.com/PelicanPlatform/classad/classad"
	"github.com/bbockelm/cedar/message"
	cedarserver "github.com/bbockelm/cedar/server"
	"github.com/bbockelm/cedar/watch"

	"github.com/bbockelm/golang-collector/store"
)

// watchHandler serves the WatchAds command: it reads a subscribe request, then
// streams the named table's change events to the client -- an event-header ad
// (kind, key, cursor) per change, immediately followed by the ad itself for an
// Upsert -- until the client disconnects or the server shuts down. On a server
// shutdown it emits a GoingAway event first, so the client can reconnect (with
// its last cursor) to another collector rather than treating the close as a drop.
//
// The handler owns the connection for the subscription's lifetime: it returns
// (letting the server close the socket) only when the stream ends.
func watchHandler(st *store.Store) cedarserver.HandlerFunc {
	return func(ctx context.Context, c *cedarserver.Conn) error {
		req, err := message.NewMessageFromStream(c.Stream).GetClassAd(ctx)
		if err != nil {
			return err
		}
		adTypeName, constraint, cursor, err := watch.DecodeRequest(req)
		if err != nil {
			return err
		}
		adType, ok := store.AdTypeByName(adTypeName)
		if !ok {
			return fmt.Errorf("collector: watch on unknown ad type %q", adTypeName)
		}

		// Detect client disconnect promptly, even while idle: the client sends
		// nothing after subscribing, so a blocking read returns an error only when
		// it closes. Read and write use disjoint stream state, so this runs safely
		// alongside the event writes below. cancel() also fires on handler return
		// (unblocking this read) and when the server context is cancelled.
		watchCtx, cancel := context.WithCancel(ctx)
		defer cancel()
		go func() {
			if _, err := message.NewMessageFromStream(c.Stream).GetInt(watchCtx); err != nil {
				cancel()
			}
		}()

		seq, err := st.Watch(watchCtx, adType, cursor, constraint)
		if err != nil {
			return err
		}

		resp := message.NewMessageForStream(c.Stream)
		send := func(sctx context.Context, kind watch.Kind, key, cur []byte, ad *classad.ClassAd) error {
			if err := resp.PutClassAd(sctx, watch.EncodeHeader(kind, key, cur)); err != nil {
				return err
			}
			if kind.HasAd() && ad != nil {
				if err := resp.PutClassAd(sctx, ad); err != nil {
					return err
				}
			}
			return resp.FlushFrame(sctx, false) // flush this event without ending the message
		}

		for ev := range seq {
			// collections.WatchKind values match watch.Kind (Upsert=0 .. Resync=4).
			if err := send(watchCtx, watch.Kind(ev.Kind), ev.Key, ev.Cursor, ev.Ad); err != nil {
				return err // write failed: the client is gone
			}
		}

		// The stream ended. If the connection's context is done, the server is
		// shutting down (rather than the client disconnecting) -- tell the client
		// to reconnect. Best-effort on a fresh deadline, since watchCtx is cancelled.
		if ctx.Err() != nil {
			gaCtx, gaCancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer gaCancel()
			_ = send(gaCtx, watch.KindGoingAway, nil, nil, nil)
		}
		return nil
	}
}
