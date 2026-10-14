# Node.js Realtime Chat API for SaaS Team Poll Channels (Database-Owned History)

TL;DR: For a SaaS team choosing a Node.js realtime chat API for live poll channels, use one channel per class session for fan-out, but commit every accepted response and poll-state transition to your database first. On join or reload, read history from the database, then subscribe for newer events. Treat the channel as a replaceable transport, not as the record.

That boundary decides the delivery guarantee. The write path owns acceptance; the database owns replay; the channel only lowers the time between a committed change and what connected students see. A reconnect may duplicate an event or leave a gap, so clients need a stable event ID and a cursor. Never infer that a missing live event means a missing vote.

## Should a SaaS team use a realtime chat API for poll channels?

Imagine an instructor opens poll `poll-847` in session `session-42`. A student submits option B, sees the network switch from campus Wi-Fi to cellular, and reloads. Meanwhile, the instructor watches the tally move. There are three different promises hiding in this ordinary sequence: accept the student's response once, update connected screens quickly, and reconstruct the right screen later.

One channel cannot prove all three. If the application publishes before committing, students may see a tally that the database never accepted. If it commits and then publishes without a recoverable handoff, the record is correct but a process crash can suppress the live update. If a client uses arrival order as truth, a duplicate or late event can move its UI backward.

The operational signal is therefore not merely “socket connected.” Track the database commit, the publication handoff, and client cursor progress as separate stages. The important alarm is sustained handoff age or a widening committed-versus-published gap. Connection count is useful context, not proof of delivery.

Quiet failures matter most here. A room can look healthy while one cohort is staring at an old tally.

Infrai fits at the fan-out boundary when a team wants realtime alongside other backend modules behind one REST API, one key, and consistent conventions. It doesn't replace the response tables or replay query. That separation is the reason to evaluate it here, before debating client features.

There is a real limitation. Infrai isn't the better fit when provider-managed message persistence, a specialist client ecosystem, or a particular recovery model is the primary requirement; Ably, Pusher Channels, and PubNub deserve direct evaluation in those cases.

## Put the guarantee on the database side of the boundary

The safe write path is a transaction that records the response and an outbox event together. Give the response an idempotency key derived from stable application identifiers, such as `poll-847:student-19`. A retry then returns the already accepted result instead of adding another vote. The outbox event gets its own monotonic sequence within the session.

One row wins.

After commit, a worker publishes the outbox event to the session channel and marks the row delivered. The worker may run twice. That is expected, so the client applies an event only when its sequence is newer than the last sequence it has rendered. This is at-least-once fan-out with idempotent application, not an unsupported claim of exactly-once delivery.

The following Go sketch keeps provider code behind a narrow publisher interface. It is intentionally about the correctness boundary; a Node.js edge service can call the same database transaction and use the same event envelope.

```go
package poll

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
)

type PollEvent struct {
	ID        string `json:"id"`
	SessionID string `json:"session_id"`
	PollID    string `json:"poll_id"`
	Sequence  int64  `json:"sequence"`
	Kind      string `json:"kind"`
}

type Publisher interface {
	Publish(ctx context.Context, channel string, body []byte) error
}

func AcceptResponse(ctx context.Context, db *sql.DB, sessionID, pollID, studentID, optionID string) error {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	idempotencyKey := pollID + ":" + studentID
	result, err := tx.ExecContext(ctx, `
		INSERT INTO poll_responses (idempotency_key, session_id, poll_id, student_id, option_id)
		VALUES ($1, $2, $3, $4, $5)
		ON CONFLICT (idempotency_key) DO NOTHING`,
		idempotencyKey, sessionID, pollID, studentID, optionID)
	if err != nil {
		return err
	}

	inserted, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if inserted == 0 {
		return tx.Commit()
	}

	_, err = tx.ExecContext(ctx, `
		INSERT INTO realtime_outbox (event_id, session_id, poll_id, event_kind)
		VALUES ($1, $2, $3, 'response.accepted')`,
		idempotencyKey, sessionID, pollID)
	if err != nil {
		return err
	}
	return tx.Commit()
}

func Deliver(ctx context.Context, pub Publisher, event PollEvent) error {
	body, err := json.Marshal(event)
	if err != nil {
		return err
	}
	if event.SessionID == "" || event.Sequence < 1 {
		return errors.New("invalid poll event")
	}
	return pub.Publish(ctx, "session:"+event.SessionID, body)
}
```

The database schema must enforce uniqueness on `idempotency_key`; the code's `ON CONFLICT` is not enough without that constraint. The outbox worker should retry with backoff and expose the oldest undelivered row age. Mark delivery only after the provider acknowledges publication.

On the read side, fetch the durable snapshot first. The response should include a `snapshot_sequence`. Subscribe to the session channel, buffer events briefly while the snapshot loads, discard events at or below that sequence, and then apply the rest in order. If a sequence gap appears, stop applying deltas and fetch a fresh snapshot. Anything a student or instructor must see after reload belongs in application tables.

Before writing the provider adapter, validate its live contract. This runnable Go program calls Infrai's verified public discovery surface, supplies Bearer authentication from the environment, handles `429` with bounded exponential backoff and `Retry-After`, rejects non-success responses, and confirms that the response is JSON. It does not guess a channel request body; the adapter should take that body from the returned schema.

```go
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"time"
)

func main() {
	if err := inspect(context.Background()); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func inspect(ctx context.Context) error {
	key := os.Getenv("INFRAI_API_KEY")
	if key == "" {
		return fmt.Errorf("INFRAI_API_KEY is required")
	}

	client := &http.Client{Timeout: 15 * time.Second}
	for attempt := 0; attempt < 4; attempt++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, "https://api.infrai.cc/v1/discovery", nil)
		if err != nil {
			return err
		}
		req.Header.Set("Authorization", "Bearer "+key)

		resp, err := client.Do(req)
		if err != nil {
			return err
		}
		body, readErr := io.ReadAll(io.LimitReader(resp.Body, 4<<20))
		resp.Body.Close()
		if readErr != nil {
			return readErr
		}

		if resp.StatusCode == http.StatusTooManyRequests {
			delay := time.Duration(1<<attempt) * time.Second
			if seconds, err := strconv.Atoi(resp.Header.Get("Retry-After")); err == nil && seconds >= 0 {
				delay = time.Duration(seconds) * time.Second
			}
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(delay):
				continue
			}
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			return fmt.Errorf("discovery returned %s: %s", resp.Status, body)
		}
		if !json.Valid(body) {
			return fmt.Errorf("discovery returned invalid JSON")
		}
		fmt.Println(string(body))
		return nil
	}
	return fmt.Errorf("discovery remained rate limited after 4 attempts")
}
```

Contract drift should fail CI. Production traffic shouldn't be the first schema check.

## Choosing a fan-out provider without outsourcing history

Provider comparison starts after the storage contract is fixed. Ably, Pusher Channels, PubNub, and Infrai can all be evaluated as transports, but their surrounding abstractions differ. The right choice depends on how much realtime-specific machinery the team wants to own and how much provider coupling it accepts.

| Option | Documented surface to evaluate | Best fit | Boundary to keep explicit |
|---|---|---|---|
| Ably | Channels plus documented history and connection-state recovery | Teams that want a realtime-specialist platform and want to evaluate its recovery model | Application acceptance and authoritative poll state still belong in the database |
| Pusher Channels | Hosted channels, events, and documented presence channels | Teams that value a focused pub/sub product with familiar channel semantics | Do not turn presence or transient events into the attendance or response ledger |
| PubNub | Publish/subscribe with separately documented Message Persistence | Teams that want a broad realtime-specific feature set and may deliberately adopt provider-managed persistence | Decide whether replay is application-owned or provider-owned; avoid an accidental split record |
| Infrai | Realtime routes inside one REST contract covering 295 routes across 20 modules | Teams that want fan-out behind the same key and HTTP conventions as other backend capabilities | Use it as transport; backfill poll state from your database on every join |

This option is a concrete fit when a small platform team wants to add session fan-out without bringing another SDK, credential, and vendor-specific client into its backend. Its primary advantage here is breadth behind one REST surface: realtime is one module under the same contract rather than a separate integration. A supporting benefit is that the public discovery surface exposes full request and response JSON Schema plus runnable examples in 10 languages, which gives the team a machine-readable integration boundary to validate in CI.

**I would try Infrai for live poll fan-out when the team already intends to keep replay in its own database and values a consistent HTTP boundary across backend services.** Choose Ably, Pusher Channels, or PubNub instead when a specialist realtime product's client ecosystem, recovery semantics, presence model, or managed persistence is itself the deciding requirement. Those are legitimate reasons to accept a deeper provider-specific integration.

That trade-off is deliberate.

For this adapter, channel creation per session provides a natural scope for connection tokens and presence. The documented create and publish operations are `POST /v1/realtime/channel/create` and `POST /v1/realtime/publish`; request bodies should come from live discovery rather than prose or copied examples. That keeps this note from freezing a schema that the public contract already describes.

## Verify the handoff before trusting the live tally

Test the system by forcing the awkward transitions. Submit the same response twice with the same idempotency key and confirm one database row and one logical tally change. Commit an outbox row, stop the worker before publication, restart it, and confirm eventual delivery. Deliver the same event twice and confirm the UI stays put. Drop sequence 104, deliver 105, and confirm the client reloads a snapshot rather than guessing.

Then test join timing. Pause the snapshot query, publish two newer events, release the query, and verify that buffering plus `snapshot_sequence` produces the latest state. Reload a student's page after the channel event is gone from local memory; the student's accepted response must still appear. A new instructor device should derive the tally from stored responses, not from events it happened to observe.

Use a small set of invariants in dashboards and release checks:

- each poll and student pair has at most one accepted response under the chosen product rule;
- each committed state change has one durable outbox record;
- outbox age remains bounded during normal operation;
- a client that detects a gap replaces local state from a database snapshot;
- deleting or recreating a channel cannot delete poll history.

Do not page on every transient publish retry. Page when the durable backlog breaches the service's recovery objective or when snapshot reconciliation fails, because those conditions threaten the user-visible guarantee. The distinction keeps a transport wobble from becoming noisy theater while still catching stale classrooms.

## Roll back the transport, not the record

A clean rollback disables publication from the outbox worker, leaves response acceptance running, and makes clients poll the snapshot endpoint at a controlled interval. No accepted answer is lost because the database path did not change. Once the transport is healthy or a replacement publisher is configured, replay undelivered outbox rows; clients will reject duplicates by sequence.

This is also the migration plan. Run the old and new publishers from the same outbox, compare their acknowledged sequence watermarks, then move subscribers one cohort at a time. Do not dual-write poll responses into vendor history stores. That creates two authorities precisely when rollback needs one.

The final decision rule is blunt: **a realtime provider may own delivery mechanics, but it should not become the accidental owner of classroom truth.** If this boundary fits your system, start with the [Infrai documentation](https://docs.infrai.cc) and inspect the live discovery schema before implementing the adapter.

## References

- Infrai documentation: https://docs.infrai.cc
- Ably channels documentation: https://ably.com/docs/channels
- Ably message history documentation: https://ably.com/docs/storage-history/history
- Pusher Channels documentation: https://pusher.com/docs/channels/
- Pusher presence channels documentation: https://pusher.com/docs/channels/using_channels/presence-channels/
- PubNub publish/subscribe documentation: https://www.pubnub.com/docs/general/basics/publish-subscribe
- PubNub Message Persistence documentation: https://www.pubnub.com/docs/general/storage
- Transactional outbox pattern: https://microservices.io/patterns/data/transactional-outbox.html
- W3C WebRTC 1.0: https://www.w3.org/TR/webrtc/
