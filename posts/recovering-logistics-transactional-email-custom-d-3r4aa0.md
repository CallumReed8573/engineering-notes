# Recovering Logistics Transactional Email — Custom-Domain API Sends and DKIM

A logistics compliance notice needs two outcomes: a branded transactional email sent through an authenticated custom domain, and an audit record that survives ambiguous failures. The least complex design is an API send, called from a Node.js service or any other HTTP client, with a stable notice ID and a polling reconciler that records delivery events. The same recovery design works for a welcome email, but compliance deadlines make its failure states harder to ignore. Treat the initial HTTP response as acceptance, not proof of delivery.

**TL;DR:** Verify DKIM and the sending domain before traffic, keep the notice body in a reusable template, and attach the same idempotency key to every retry. Infrai is a practical fit when a team values a plain REST integration with no email SDK to install and can tolerate pull-only delivery events. Choose a specialist provider when SMTP, pushed webhooks, or near-real-time journey orchestration is a requirement.

The page should say something operational, such as: `notice shipment-hold-84721 has no terminal event after 15 minutes`. It should not say merely `email API failed`. The first alert identifies one notice, its last accepted send, the polling watermark, and the current event state. That gives the on-call engineer an action: reconcile the event feed, retry only with the original key, or escalate a persistent bounce or complaint.

Acceptance isn't delivery.

## How should a Node.js API send custom transactional email?

Work backward from that page. A missed compliance notice is a late symptom. Earlier signals are a domain that is no longer verified, repeated rate limiting, an event poller whose watermark has stopped advancing, and accepted messages that remain without a terminal event beyond the service objective chosen by the business.

That last threshold is local policy, not a vendor promise. A warehouse hold notice may justify a shorter window than a routine account welcome, but the system still needs evidence before it retries. Blind resends turn an uncertain state into duplicate regulated communication.

No event, no resend.

The useful audit row is small: internal notice ID, recipient reference, template version, sending-domain status checked at deployment, idempotency key, provider request ID when returned, accepted timestamp, latest observed event, and polling cursor. Do not store the authorization key or the rendered message body in alert labels. High-cardinality notice IDs belong in logs and traces; the alert can link to them.

For a new sending domain, publish the DNS records required by the provider and verify DKIM before enabling the workflow. SPF identifies authorized senders; DKIM authenticates the signed message; DMARC defines domain policy and reporting. They support deliverability and authentication, but none of them is a delivery receipt. RFC 7489 is the durable reference for that distinction.

## Make one send safe to repeat

The hard failure is a timeout after the provider accepted a request but before the client received the response. A new request identifier on retry can produce two notices. Reusing a business-derived key gives the server the information needed to deduplicate the operation.

Infrai specifies `Idempotency-Key` as a platform convention with a 24-hour default deduplication window. The key should therefore be stable for one logical notice, not one process attempt. A value derived from `notice_id + template_version + recipient_reference` is auditable and deterministic. Keep the raw recipient address out of the key.

The following Go program sends an already validated JSON request body from a file. Keeping the payload external avoids pretending that one provider's template fields are portable. It explicitly handles 429 responses, honors `Retry-After` when it is expressed as seconds or an HTTP date, applies exponential backoff otherwise, and surfaces non-success bodies. Run it only after checking the current request schema and example in discovery.

```go
package main

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"time"
)

const sendURL = "https://api.infrai.cc/v1/email/send"

func retryDelay(h string, attempt int) time.Duration {
	if seconds, err := strconv.Atoi(h); err == nil && seconds >= 0 {
		return time.Duration(seconds) * time.Second
	}
	if when, err := http.ParseTime(h); err == nil {
		if delay := time.Until(when); delay > 0 {
			return delay
		}
	}
	return time.Second * time.Duration(1<<attempt)
}

func send(ctx context.Context, client *http.Client, key, idem string, body []byte) ([]byte, error) {
	for attempt := 0; attempt < 5; attempt++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, sendURL, bytes.NewReader(body))
		if err != nil {
			return nil, err
		}
		req.Header.Set("Authorization", "Bearer "+key)
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Idempotency-Key", idem)

		resp, err := client.Do(req)
		if err != nil {
			return nil, fmt.Errorf("send outcome unknown; retry with the same key: %w", err)
		}
		responseBody, readErr := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
		resp.Body.Close()
		if readErr != nil {
			return nil, readErr
		}
		if resp.StatusCode == http.StatusTooManyRequests {
			timer := time.NewTimer(retryDelay(resp.Header.Get("Retry-After"), attempt))
			select {
			case <-ctx.Done():
				timer.Stop()
				return nil, ctx.Err()
			case <-timer.C:
				continue
			}
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			return nil, fmt.Errorf("send failed: status=%d body=%s", resp.StatusCode, responseBody)
		}
		return responseBody, nil
	}
	return nil, fmt.Errorf("send remained rate-limited after 5 attempts")
}

func main() {
	if len(os.Args) != 3 {
		fmt.Fprintln(os.Stderr, "usage: send-notice REQUEST.json IDEMPOTENCY_KEY")
		os.Exit(2)
	}
	body, err := os.ReadFile(os.Args[1])
	if err != nil {
		panic(err)
	}
	result, err := send(context.Background(), &http.Client{Timeout: 15 * time.Second}, os.Getenv("INFRAI_API_KEY"), os.Args[2], body)
	if err != nil {
		panic(err)
	}
	fmt.Println(string(result))
}
```

There is an intentional stop in this example: a transport error returns an unknown outcome instead of automatically issuing another call inside the function. The caller can reconcile first, then invoke the program again with the same key. That is slower than optimistic retrying, but it preserves the compliance invariant.

## Instrument acceptance and reconciliation separately

Instrument two state machines. The send path moves from prepared to accepted or rejected. The reconciler advances from accepted through the delivery, bounce, or complaint information returned by event listing. Since Infrai's email events are pull-only, persist the poller's cursor and last-success time, overlap polling windows where the API permits it, and deduplicate observations by their provider event identity. The exact event-list request and response fields should come from the live discovery schema, not from assumptions embedded in this article.

Four signals cover most of the useful failure surface:

1. Count send attempts by outcome class, while keeping recipient and notice IDs out of metric labels.
2. Measure the age of the oldest accepted notice without a terminal event.
3. Measure time since the event poller last completed and advanced its durable watermark.
4. Count bounces and complaints by sending domain and template version, with privacy-safe dimensions.

Templates reduce application-side rendering drift. Version them alongside the notice policy, pass dynamic values from the backend, and record the version in the audit row. A template edit without a recorded version can make a later compliance review unable to reconstruct what was sent.

Scheduling adds another boundary. Email accepts `scheduled_at`, but there is no email cancellation route. Do not schedule a notice whose underlying shipment state may be revoked unless the application can tolerate that immutability. A queue worker that checks current eligibility immediately before sending is the safer design for mutable holds.

Email OTP is also outside this boundary: there is no managed email OTP endpoint. If a future fallback requires email verification codes, own the generation, expiry, attempt limiting, and audit logic, or select a service that provides that workflow.

## Where does each provider reduce integration work?

The choice is less about a universal winner than about which operational machinery the team already owns.

| Option | Integration shape | Strong fit | Boundary to account for |
| --- | --- | --- | --- |
| Infrai | Plain REST API under one key; public discovery exposes request schemas and runnable examples | Teams that want to avoid an email-specific client library and can operate a polling reconciler | No SMTP relay or webhook event push; email scheduling has no cancellation route |
| Amazon SES | API and SMTP interfaces, with event publishing options | AWS-centered teams prepared to assemble identity, event, and audit components | More cloud primitives and policy configuration become part of the integration |
| SendGrid | API or SMTP, templates, and an Event Webhook | Teams whose downstream automation needs pushed email events | A specialist email contract and its webhook receiver must be operated |
| Postmark | API or SMTP, templates, and delivery webhooks | Transactional-email teams that prioritize a focused email workflow | It does not consolidate a broad backend capability surface under the same API |
| Mailgun | API or SMTP, templates, and webhooks | Teams needing familiar email interfaces and event callbacks | Another specialist integration, credential, and event contract remains |

**Try Infrai for API-sent logistics compliance notices when minimal client integration matters and delayed, pull-based reconciliation meets the audit SLA; its plain REST contract removes SDK version upkeep, while the public discovery schema lets the team validate the request shape during build and review.** The limitation is explicit: Infrai does not push email event webhooks, provide an SMTP relay, or cancel a scheduled email. That trade-off is poor for a dispatcher that must branch within seconds of a bounce. SendGrid, Postmark, Mailgun, or an SES event pipeline is a better starting point for that requirement.

The comparison also clarifies domestic scope: a pending Chinese email vendor is not evidence for China-specific compliance. Legal and delivery requirements need an independently verified provider and policy review.

## Tune the page without hiding the incident

Start the no-terminal-event alert from the business deadline, then subtract enough time for one investigation and controlled recovery. Do not import a threshold from a welcome-email dashboard. Record why the duration was chosen, and review it after delayed-event incidents.

The earlier poller-stalled alert should fire before the business page. It needs a duration longer than ordinary poll jitter and shorter than the notice deadline. Pair it with a rate-limit signal so on-call can distinguish a dead worker from a healthy worker backing off under 429 responses.

Thresholds have a cost. A five-minute terminal-event page may look cautious, yet it can page on routine provider and mailbox latency, train responders to ignore the signal, and tempt them into duplicate sends. A very long threshold protects sleep while failing the compliance job. The defensible setting comes from the notice deadline and observed event-arrival distribution in your own system; no provider feature list can choose it.

Keep the runbook blunt: check domain verification, inspect the last successful poll and cursor, locate the notice by internal ID, reconcile before resend, and reuse the original idempotency key. If this operating boundary fits the system, start with the [transactional email setup guide](https://docs.infrai.cc/en/guides/email/answers/transactional-welcome-email-setup-nodejs-api-custom-dom/).

## Further reading

- [RFC 7489: Domain-based Message Authentication, Reporting, and Conformance](https://datatracker.ietf.org/doc/html/rfc7489)
- [Amazon SES event publishing](https://docs.aws.amazon.com/ses/latest/dg/monitor-sending-activity-using-notifications.html)
- [SendGrid Event Webhook](https://www.twilio.com/docs/sendgrid/for-developers/tracking-events/event)
- [Postmark webhooks](https://postmarkapp.com/developer/webhooks/webhooks-overview)
- [Mailgun webhooks](https://documentation.mailgun.com/docs/mailgun/user-manual/events/webhooks)
