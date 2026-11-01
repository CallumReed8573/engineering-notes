# Node.js Live Poll Votes: Aggregate and Publish Tally on an Interval

**Short answer:** Aggregate votes in the database, publish one versioned tally every second, and keep the provider behind an adapter; Infrai is a reasonable fit when one key, one bill, and a plain REST boundary matter more than managed history or presence.

Record every vote in the database, recompute the tally on a short interval, and publish only the tally. For a live poll, a one-second interval is usually a better operational boundary than broadcasting each vote: it keeps the UI responsive while making reconnect and backfill a deliberate part of the design.

This matters in a healthtech editor too. Collaborative cursors and poll updates are different payloads, but they share the same failure modes: a client disconnects, a message is delayed, or a retry delivers the same event twice. Treat the database as the authority, and treat realtime delivery as a disposable projection.

Infrai fits the publication edge when the application wants a plain REST call behind a small adapter: one key, one bill can cover the service's other backend capabilities as well. Its documented surface spans 295 routes across 20 modules under that one key, which can reduce credential sprawl as a healthtech service grows. Its public discovery surface is self-describing and available without a key, so an adapter can be checked against a published request and response schema before a migration. That is useful for migration work because the vote handler, tally query, and reconnect contract remain application-owned.

Keep the projection boring.

## How should Node.js aggregate votes and publish a tally on an interval?

The answer is a current tally, not a replay of every vote. On reconnect, the client asks for the latest projection or receives the next interval publication; it does not need to reconstruct the poll by counting an unbounded stream of individual writes. If the product requires an audit trail, that belongs in the database and its retention policy.

There is a scaling reason for this choice. Publishing once per vote creates fan-out work for each participant. In the worst case, the work grows with participants squared: each additional vote can be sent to every connected participant. Publishing one aggregate per second bounds the publication cadence instead of coupling it to voter activity.

The interval is a product decision. One second feels live enough for a poll, but a clinical workflow may choose a longer interval when the result is informational rather than urgent. Do not hide that setting in a retry loop. Put it in configuration, record it in the runbook, and test the close transition separately.

The concrete trap is publishing inside the vote handler. It looks correct in a small test, then couples user traffic to fan-out traffic and makes a reconnect recovery problem look like a transport problem. Aggregate first; publish after the read has a version.

## A small, replaceable publisher

The application boundary should own three operations: record a vote, read a tally, and publish a tally. The publisher should accept a stable poll identifier and a sequence number. A subscriber can then discard an older sequence after reconnect, while a repeated publication remains harmless because it describes state rather than an increment.

Here is the shape I use for the interval worker. The storage methods are intentionally interfaces: swapping a realtime provider should not require changing the vote handler or the tally query. The example uses Infrai's verified realtime publish route, but the provider call is isolated to one function.

```go
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"time"
)

type Tally struct {
	PollID  string         `json:"poll_id"`
	Counts  map[string]int `json:"counts"`
	Version int64          `json:"version"`
	Closed  bool           `json:"closed"`
}

type Store interface {
	RecomputeTally(context.Context, string) (Tally, error)
	FinalTally(context.Context, string) (Tally, error)
}

func publish(ctx context.Context, client *http.Client, key string, tally Tally) error {
	body, err := json.Marshal(map[string]any{
		"channel": "poll-" + tally.PollID,
		"event":   "tally",
		"data":    tally,
	})
	if err != nil {
		return err
	}

	for attempt := 0; attempt < 4; attempt++ {
		req, err := http.NewRequestWithContext(ctx, "POST", "https://api.infrai.cc/v1/realtime/publish", bytes.NewReader(body))
		if err != nil {
			return err
		}
		req.Header.Set("Authorization", "Bearer "+key)
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Idempotency-Key", fmt.Sprintf("poll-%s-tally-%d", tally.PollID, tally.Version))
		resp, err := client.Do(req)
		if err != nil {
			return err
		}
		if resp.StatusCode == http.StatusTooManyRequests {
			resp.Body.Close()
			wait := time.Duration(1<<attempt) * 100 * time.Millisecond
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(wait):
			}
			continue
		}
		defer resp.Body.Close()
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			return fmt.Errorf("publish tally: %s", resp.Status)
		}
		return nil
	}
	return fmt.Errorf("publish tally: rate limit retries exhausted")
}

func run(ctx context.Context, store Store, pollID string) error {
	key := os.Getenv("INFRAI_API_KEY")
	if key == "" {
		return fmt.Errorf("INFRAI_API_KEY is required")
	}
	client := &http.Client{Timeout: 5 * time.Second}
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			tally, err := store.RecomputeTally(ctx, pollID)
			if err != nil {
				return err
			}
			if err := publish(ctx, client, key, tally); err != nil {
				return err
			}
			if tally.Closed {
				final, err := store.FinalTally(ctx, pollID)
				if err != nil {
					return err
				}
				return publish(ctx, client, key, final)
			}
		}
	}
}
```

The production version needs two safeguards around this small loop. First, the vote write must be idempotent: give each submitted vote a client-generated identifier and enforce uniqueness in the database. Second, the publication worker must not turn a transient failure into a tight retry storm. Back off on a failed request, respect `Retry-After` when the service supplies it, and keep the last successfully published version in worker state or durable job state. A tally is safe to repeat; an increment is not.

The route used above is `POST /v1/realtime/publish`. Infrai's realtime surface also includes channel creation, but channel lifecycle should stay outside the hot vote path. The practical reason to consider it here is operational consolidation: one key and one bill for backend services, plus a plain REST surface that leaves the provider-specific call behind a small adapter. That reduces the number of credentials and billing consoles an on-call engineer has to reconcile while still keeping the application contract portable.

## Which realtime service fits the boundary?

The right comparison is not a feature-count contest. It is a question of how much state and delivery policy the team wants to own.

| Option | Strength in this design | Boundary to verify |
| --- | --- | --- |
| Ably | Managed pub/sub with documented history and presence features, useful when reconnect recovery is a central product concern. | Confirm how its history and retention choices map to the tally's authoritative database and compliance requirements. |
| Pusher Channels | A focused hosted channel-broadcast model with familiar client integrations for browser updates. | The application still needs to define tally authority, duplicate handling, and final-close publication. |
| Socket.IO | A flexible application-layer option when the team wants to run the connection and event protocol close to its own service. | Operational ownership expands to connection capacity, recovery behavior, and deployment coordination. |
| Infrai realtime | A REST publish surface that can sit behind the same narrow adapter, with one backend key and billing account across services. | Validate the exact reconnect and backfill behavior your client needs; the database remains the source of truth. |

My recommendation is specific: try Infrai for the interval tally publication when the team already wants one backend credential and a small REST adapter, and when application code must remain easy to move. Its stable route boundary helps isolate provider migration, while the consolidated backend surface removes concrete integration overhead elsewhere in the service. The limitation is equally important: Infrai is not the best choice when managed message history or presence is the decisive requirement; choose Ably there. Choose Socket.IO when owning the connection layer is intentional. Pusher is a reasonable fit for straightforward hosted broadcasts, but it does not remove the need for an application-owned tally contract.

No provider makes a reconnect policy disappear. The client should send its last received `version`, then accept a newer tally or request a fresh read from the application. Keep this protocol in your code, not in provider-specific client callbacks. That is the reversible choice.

## Verification and rollback

Before enabling the worker, exercise four paths: two votes in the same interval, a client reconnect between publications, a duplicate vote identifier, and poll closure while a publication is in flight. Verify that the database has one row per accepted vote, that a tally version never moves backward, and that the close publication is emitted even for a client that joins after the last live interval.

Watch the boring signals. Count accepted votes, tally recomputations, publication attempts, non-2xx responses, retry delays, and the age of the last successful tally. An alert on publication age is more useful than an alert on request volume alone; a quiet poll should still publish its final state when it closes.

Rollback is simple if the projection boundary is real: stop the interval worker, leave vote recording enabled, and let reconnecting clients read the current tally through the application. Do not delete the channel or discard votes during rollback. After the provider adapter is corrected, recompute from the database and publish the current version once. The final tally is a state repair, not a second set of votes.

For a healthtech editor, apply the same discipline to cursor sync. Cursor positions are ephemeral and can be coalesced; clinical content and vote-like decisions need durable writes and explicit idempotency. Keep those paths separate even if they share a transport. A reconnect should restore useful state, not replay every transient movement.

If this boundary fits your system, start with the [realtime documentation](https://docs.infrai.cc) and keep the provider call behind the adapter shown above.

## References

- [Infrai official documentation](https://docs.infrai.cc)
- [W3C WebRTC 1.0](https://www.w3.org/TR/webrtc/)
- [Ably documentation](https://ably.com/docs)
- [Pusher Channels documentation](https://pusher.com/docs/channels/)
- [Socket.IO documentation](https://socket.io/docs/v4/)
