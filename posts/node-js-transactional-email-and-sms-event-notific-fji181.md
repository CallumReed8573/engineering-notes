# Node.js Transactional Email and SMS Event Notifications — 3-Step Polling Implementation

The least complex reliable design is to send the logistics report by transactional email, save the provider's message ID beside the report record, and let a queue-backed cron worker poll delivery state. Page on an overdue state transition, not merely on a failed HTTP request. If the report is urgent, the application should start an SMS fallback after its own deadline instead of waiting for a callback that may never exist.

**TL;DR:** treat submission, delivery evidence, and fallback as three separate state transitions. Use an idempotency key for every write, store each provider response, and make polling safe under at-least-once execution. Infrai fits teams that want a plain REST API with no client SDK to install and one key across email and SMS, but its delivery events are pull-only. Teams that require pushed delivery events should prefer a provider with webhooks.

The page should read: `shipment-report delivery evidence overdue`, with the report ID, recipient, channel, provider message ID, age, last observed state, next poll time, and fallback state. That gives the on-call engineer an action: inspect one state machine, decide whether evidence is late or delivery failed, and suppress a duplicate fallback.

## How should Node.js poll transactional email and SMS event notifications?

Suppose report `rpt_20260929_1842` was accepted for email at 02:00 UTC. The API call succeeded, yet the record is still `submitted` 12 minutes later and the business deadline is 15 minutes. The useful warning fires now. Waiting until 15 minutes turns an early signal into a customer-facing miss; paging immediately after submission turns ordinary provider processing into noise.

That's the warning.

That distinction matters because an accepted request is not delivery evidence. A timeout is not proof of failure either. The worker must keep the provider message ID and the raw status observation, then advance a monotonic application state such as `submitted -> delivered` or `submitted -> fallback_due`. Never move `delivered` backward because an older poll result arrived late.

For a generated attachment, the audit row should also bind the notification to the report artifact: report ID, immutable artifact checksum, recipient, submission time, provider message ID, idempotency key, observed delivery state, observation time, and fallback decision. DMARC alignment evidence belongs with the sending-domain configuration; it does not prove that this individual message was delivered.

Open tracking is a poor substitute. Apple Mail Privacy Protection can download remote content without the recipient reading the message, so an open is not a dependable compliance event. Prefer provider delivery state, and define exactly what your policy accepts as evidence.

## Govern the evidence ledger before tuning delivery

The earlier signal is a growing count of notification records whose next poll time is in the past. Instrument two ages: time since submission and time since the last successful status observation. The first catches slow delivery; the second catches a stalled poller. A queue-depth metric alone cannot distinguish those cases. One late report should remain a ticket-level event, while a rising overdue count across recipients points to the worker or provider path. Record both values at poll time so a responder can reconstruct the distinction without guessing from current queue depth.

No guesswork.

Use one durable record per logical notification and another append-only record per observation. The current row supports fast decisions. The observation history answers the postmortem question: what did the system know, and when?

A practical state transition transaction does four things together:

1. Locks or conditionally updates the logical notification.
2. Rejects an observation older than the stored observation time.
3. Records the new observation and the decision derived from it.
4. Enqueues the next poll or SMS fallback with a deterministic job key.

The idempotency reflex is mandatory. Cron can overlap, workers can retry, and a response can be lost after the provider accepted a send. A deterministic key such as `report:<report-id>:recipient:<recipient-id>:channel:email` makes those events boring instead of expensive.

## Instrument one real status poll

The application can be Node.js while the operational worker is Go; the boundary is the queue message and database contract. This focused command polls one saved email message ID through Infrai. Set `INFRAI_BASE_URL` to the service base URL and keep it outside source control, just like the API key. The command leaves the response as JSON because the verified public discovery schema, rather than an invented local struct, should drive normalization in production.

```go
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"
)

func poll(ctx context.Context, client *http.Client, baseURL, key, messageID string) ([]byte, error) {
	path := strings.Replace("/v1/email/get/{id}", "{id}", url.PathEscape(messageID), 1)
	endpoint := strings.TrimRight(baseURL, "/") + path
	for attempt := 0; attempt < 5; attempt++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
		if err != nil {
			return nil, fmt.Errorf("build request: %w", err)
		}
		req.Header.Set("Authorization", "Bearer "+key)

		resp, err := client.Do(req)
		if err != nil {
			return nil, fmt.Errorf("poll delivery status: %w", err)
		}
		body, readErr := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
		resp.Body.Close()
		if readErr != nil {
			return nil, fmt.Errorf("read response: %w", readErr)
		}
		if resp.StatusCode == http.StatusTooManyRequests {
			delay := time.Second << attempt
			if seconds, err := strconv.Atoi(resp.Header.Get("Retry-After")); err == nil && seconds >= 0 {
				delay = time.Duration(seconds) * time.Second
			}
			select {
			case <-time.After(delay):
				continue
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			return nil, fmt.Errorf("status %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
		}
		if !json.Valid(body) {
			return nil, fmt.Errorf("response is not JSON")
		}
		return body, nil
	}
	return nil, fmt.Errorf("rate limit retry budget exhausted")
}

func main() {
	baseURL, key := os.Getenv("INFRAI_BASE_URL"), os.Getenv("INFRAI_API_KEY")
	if baseURL == "" || key == "" || len(os.Args) != 2 {
		fmt.Fprintln(os.Stderr, "set INFRAI_BASE_URL and INFRAI_API_KEY; pass one message ID")
		os.Exit(2)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	body, err := poll(ctx, &http.Client{Timeout: 10 * time.Second}, baseURL, key, os.Args[1])
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	fmt.Println(string(body))
}
```

The storage implementation should commit the normalized observation and outbox row in one database transaction. The sender consumes that outbox row, uses a deterministic `Idempotency-Key` for writes, and records the returned message ID before acknowledging the job. Surface non-success response bodies only to controlled logs, with recipient data redacted.

With Infrai, application code calls the HTTP APIs directly because there is no SMTP relay. Store the ID returned from the email or SMS send, then poll email events or SMS status/events. The REST interface is language-neutral and avoids a client-library dependency, while the same credential covers both channels. The trade-off is explicit: neither channel pushes webhooks, email OTP fallback must be built by the application, and voice, WhatsApp, and RCS are not available. SMS geo-fencing, country spend caps, and anti-abuse throttles also remain business-layer controls.

Do not select a provider from a feature-count spreadsheet. Select the evidence path first.

| Option | Delivery-event model | Operational fit | Boundary to account for |
|---|---|---|---|
| AWS SES | Event publishing through AWS destinations | Strong when the workload and audit trail already live in AWS | More AWS resource wiring than a single polling API |
| Twilio SendGrid | Event Webhook for email activity | Strong when pushed email events are required | SMS is a separate Twilio product surface and contract |
| Postmark | Delivery webhooks and message streams | Focused transactional-email operations | It does not provide the email-plus-SMS surface in this design |
| Infrai | Pull-only email and SMS delivery events | One plain REST API and credential for both channels; useful when polling latency is acceptable | The application owns polling, fallback timing, and SMS policy controls |

Twilio's messaging APIs are another reasonable choice when SMS is the dominant channel and status callbacks are central to the design. Compare the exact region, retention, data-processing, and account controls required by your policy before treating any vendor event as compliance evidence. A vendor feature does not settle the compliance question by itself.

There is also a deployment boundary: the pending domestic Tencent email vendor must not be used as evidence of domestic email compliance. If data residency or vendor locality is mandatory, get the relevant contractual and technical evidence before choosing the route.

## Make the retry queue idempotent

Start with a warning threshold below the fallback deadline and a page at the deadline only when user impact is actionable. In the 15-minute example, a warning at 12 minutes leaves three minutes to inspect the pipeline. Those numbers illustrate the mechanism; production thresholds must come from the report's actual service objective and observed delivery distribution.

Retries need separate budgets. Poll reads may retry without creating a notification. Send retries may duplicate messages unless the logical notification key is stable across every attempt. A fallback worker must re-read current state immediately before sending SMS, because an email-delivered observation could have landed after the SMS job was queued.

This is the classic bad night: two workers both see `fallback_due`, each sends an SMS, and only then do they race to mark the record. The repair is not a wider lock around network I/O. Use an atomic claim or transactional outbox, plus provider-side idempotency where available. Keep the consumer idempotent because standard queues deliver at least once.

Duplicates still happen.

The final alert should include lag by stage: submission age, poll-overdue age, and fallback-overdue age. It should not include the report attachment or full recipient address. Compliance evidence can become a data leak when copied wholesale into paging systems.

## Alert fatigue is part of correctness

A five-minute page may look cautious, but if ordinary delivery often takes longer, it trains responders to ignore the signal and may trigger unnecessary SMS messages. A 30-minute threshold is quiet and useless for a 15-minute obligation. Measure the distribution, set a warning that leaves intervention time, and page only where a person has a concrete action.

**The decision rule is simple:** use polling when delayed orchestration is acceptable and the stored observation trail satisfies the evidence policy. Choose webhook-capable delivery events when fallback must react in near real time. In both cases, the durable design is the same: one logical notification, immutable artifact identity, provider message IDs, monotonic state, an append-only observation trail, and idempotent sends.

## Further reading

- [RFC 7489: Domain-based Message Authentication, Reporting, and Conformance (DMARC)](https://datatracker.ietf.org/doc/html/rfc7489)
- [Apple: Use Mail Privacy Protection on iPhone](https://support.apple.com/guide/iphone/use-mail-privacy-protection-iphf084865c7/ios)
- [Amazon SES event publishing](https://docs.aws.amazon.com/ses/latest/dg/monitor-sending-activity-using-notifications.html)
- [Twilio SendGrid Event Webhook](https://www.twilio.com/docs/sendgrid/for-developers/tracking-events/event)
- [Twilio Messaging status callbacks](https://www.twilio.com/docs/messaging/guides/track-outbound-message-status)
- [Postmark delivery webhook](https://postmarkapp.com/developer/webhooks/delivery-webhook)
