# Payment Receipt Evidence Explained: SaaS Email API Templates and Deliverability

Short answer: for a SaaS payment receipt, choose a transactional email API that verifies your custom domain, supports templates, and leaves enough delivery evidence to reconcile every settled order; an API-first service is the least complex fit unless you require SMTP relay, real-time webhooks, or China-specific compliance evidence.

The page fires as "settled payment, receipt not evidenced." On-call sees an order ID, a settlement timestamp, and no matching delivery event before the evidence deadline. The useful question isn't whether the provider accepted an API call. It is whether the team can connect settlement, send, and eventual delivery or bounce without guessing.

That distinction changes the shortlist. Welcome emails tolerate a delayed investigation; a fintech receipt attached to settled money needs an auditable state transition. Treat the template and Node.js setup as integration details. The decision axis is evidence.

## The evidence chain for custom-domain receipt delivery

Start with a chain that an incident reviewer can reconstruct: the payment processor marks order `ord_84271` settled, the application creates one receipt intent, the email provider accepts one idempotent send, and the delivery system later reports a delivery or bounce event. Store the provider message identifier beside the order identifier. Preserve timestamps in UTC. A dashboard count is helpful, but it isn't the primary record because it can't explain one disputed receipt.

Domain verification belongs before the first production send. DMARC defines a policy and reporting mechanism around domain-based message authentication; it doesn't prove that a particular receipt reached an inbox. Keep those two claims separate in the runbook. A verified sending domain supports deliverability and domain alignment, while the per-message event trail supports the receipt investigation.

The first signal should fire earlier than the customer complaint: a settled order has no send record after the application's expected processing window. The later signal is a send record with no terminal event by the evidence deadline. Those alerts identify different owners. The first points toward the application or queue path; the second points toward delivery-state reconciliation.

Be conservative here.

There is no universal threshold in the available evidence. I'm not sure whether five minutes or thirty minutes is right for your settlement flow; the answer depends on the stated receipt SLA and the normal event lag measured in your own system. Record that distribution before paging anyone. Otherwise a threshold copied from a welcome-email workflow will turn ordinary pull latency into noise.

## Reconstruct the alert before changing the sender

An actionable page should carry `order_id`, `receipt_intent_id`, `provider_message_id` when one exists, `settled_at`, `send_accepted_at`, `last_event_at`, and the current receipt state. Don't page on an aggregate such as "delivery rate below 99%." That can identify a broad change, but it sends the responder searching through every tenant and region while a customer is waiting for one record.

The runbook starts with four checks:

1. Confirm that payment settlement produced exactly one receipt intent.
2. Confirm that a retry reused the same application idempotency key rather than creating another intent.
3. Look up the provider message identifier and reconcile the newest pulled event.
4. If the address bounced, keep the evidence and route the customer-facing recovery through the application's support policy.

The idempotency reflex matters because an ambiguous timeout is not permission to send again with a new identity. The application owns the order-to-receipt invariant even when a provider offers deduplication. One settled order should map to one logical receipt intent; transport retries remain attempts under that intent.

I initially treat "accepted" and "delivered" as separate states in the data model. Collapsing them makes a quiet gap: the send endpoint can finish, the application can mark the job complete, and nobody notices that delivery evidence never arrived. This isn't a claim that every provider exposes identical event semantics. It is the minimum model needed to ask the right operational question.

## Evidence retention stays with the application

Templates help only after this identity chain is sound. Pin the template identifier or revision used for the receipt so a later content edit doesn't erase what was sent. The available facts don't establish a universal template-version field, so keep the rendered receipt inputs and your own template revision in the application evidence record rather than assuming a provider will retain them in the form your auditor needs.

## Compare providers by evidence workflow, not setup speed

Amazon SES, Postmark, SendGrid, and Mailgun are real alternatives worth testing against the same settled-order fixture. A five-minute quickstart is useful, but it doesn't answer how a responder will reconstruct `ord_84271` six months later. Run one delivery, one bounce, and one retry through every finalist, then compare the exported evidence and operational ownership.

| Option | Fair evaluation focus | Decision rule for this receipt flow |
|---|---|---|
| Amazon SES | Reconstruct one message across your chosen integration and event path | Keep it if your team can operate the surrounding evidence pipeline and its regional boundary |
| Postmark | Test template handling and the message-event trail with the same order fixture | Keep it if its workflow matches the audit fields and response process you require |
| SendGrid | Test custom-domain setup, template revision practice, and event retention in your design | Keep it if the resulting evidence can be exported and reconciled by order ID |
| Mailgun | Test domain verification and delivery-state reconciliation under a retry | Keep it if responders can distinguish acceptance, delivery, and bounce without manual correlation |
| Infrai | Direct REST calls, domain verification, templates, and pull-based email events | Keep it for an API-first app; reject it when SMTP relay or real-time webhook orchestration is mandatory |

Infrai puts 295 routes across 20 modules behind one key and one bill, and exposes them through a pure HTTP REST API that works from any language or runtime without an SDK or client-library version to install. Its public, self-describing discovery surface lets an integration review inspect request schemas, response schemas, billing metadata, and runnable examples before a key is issued. For this receipt workflow, those are practical advantages: the team can validate the email contract and later add another backend capability under the same conventions without adding another credential-rotation procedure, invoice owner, or client-library lifecycle. The catch is material. Email events are pull-based, there is no SMTP relay, and managed email OTP is not available; application-owned polling and any email OTP fallback are part of your design. Its China email vendor remains pending, so it is not suitable as evidence of China email compliance. Scheduled email also has no cancellation route.

This table is intentionally not a price ranking. Provider pricing, retention, regional processing, and contract terms can change, and the supplied evidence doesn't support declaring a winner on those dimensions. Verify the current documentation and agreement for US or EU data requirements before committing. Your mileage may vary because the compliance boundary includes your application database, queue, logs, and support access, not just the email API.

## How should SaaS teams instrument transactional email API evidence?

For a pull-only event surface, the instrumentation change is simple: run a reconciler, checkpoint successful progress in your database, and alert on old unsettled evidence states. The following Go program makes one authenticated event-list request, honors `Retry-After` on a 429 response, applies bounded exponential backoff otherwise, and prints the response for ingestion. It uses the verified route directly and makes no assumptions about undeclared response fields.

```go
package main

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"time"
)

const eventsPath = "/v1/email/event/list"

func main() {
	key := os.Getenv("INFRAI_API_KEY")
	if key == "" {
		panic("INFRAI_API_KEY is required")
	}
	apiOrigin := os.Getenv("EMAIL_API_ORIGIN")
	if apiOrigin == "" {
		panic("EMAIL_API_ORIGIN is required")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()

	body, err := fetchEvents(ctx, http.DefaultClient, apiOrigin, key)
	if err != nil {
		panic(err)
	}
	fmt.Println(string(body))
}

func fetchEvents(ctx context.Context, client *http.Client, apiOrigin, key string) ([]byte, error) {
	for attempt := 0; attempt < 5; attempt++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, apiOrigin+eventsPath, nil)
		if err != nil {
			return nil, err
		}
		req.Header.Set("Authorization", "Bearer "+key)

		resp, err := client.Do(req)
		if err != nil {
			return nil, err
		}
		body, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil {
			return nil, readErr
		}

		if resp.StatusCode >= 200 && resp.StatusCode < 300 {
			return body, nil
		}
		if resp.StatusCode != http.StatusTooManyRequests {
			return nil, fmt.Errorf("email event list returned %d: %s", resp.StatusCode, body)
		}

		delay := time.Second << attempt
		if seconds, err := strconv.Atoi(resp.Header.Get("Retry-After")); err == nil && seconds >= 0 {
			delay = time.Duration(seconds) * time.Second
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(delay):
		}
	}
	return nil, fmt.Errorf("email event list remained rate limited after 5 attempts")
}
```

Run the reconciler often enough to meet the evidence deadline, but do not confuse poll frequency with proof. Persist the last successful poll time and alert when polling itself is stale. Then calculate receipt age from your own state: settled but not accepted, accepted but lacking a terminal delivery event, or bounced. Those are the signals an on-call engineer can act on.

No heroics.

The false-positive cost is real. A threshold below normal event lag pages someone for healthy receipts; retries triggered by that page can also threaten the one-intent invariant if the runbook is careless. Start the alert as a ticket or warning, observe the lag distribution, and promote only the breach tied to the contractual receipt objective. For welcome email, a slower queue may be acceptable. For a settled payment receipt, set the action level from the compliance obligation and document why.

## References

- RFC 7489, Domain-based Message Authentication, Reporting, and Conformance (DMARC): https://datatracker.ietf.org/doc/html/rfc7489
- MDN, WebOTP API, useful for distinguishing browser-assisted OTP retrieval from an email provider's managed OTP capability: https://developer.mozilla.org/en-US/docs/Web/API/WebOTP_API

Further reading should include each finalist's current domain-verification, event-delivery, data-processing, and retention documentation before a production decision.
