# Choose a Node.js Email API for Custom Welcome Emails (6 Delivery Events via Polling)

An auditable compliance notice changes the email API decision: accepting a message is not the same as recording its delivery history. **Short answer:** choose a polling-based API for US/EU welcome emails when reliable sends, custom templates, domain verification, and an admin-grade delivery record matter more than instant event-driven automation.

That boundary is firm.

I have been paged for missed jobs and duplicate deliveries. The useful lesson was not "never poll." It was to make the poller's contract explicit: advance a durable checkpoint only after the returned event page is stored, tolerate overlap, and treat every downstream action as idempotent. For a gaming backend sending a mandatory welcome notice, that gives an operator evidence to inspect without pretending the system has webhook latency.

## How does a US/EU app backend workflow poll email delivery without webhooks?

Start with six checks: API send reliability, custom template lifecycle, domain verification, delivery-event transport, retry semantics, and retraction requirements. The first three establish that the message can be composed and sent from the intended domain. The last three decide how calmly the system behaves after the first request leaves the application.

For the capability discussed here, template create, update, and preview operations cover the usual branded welcome flow, and domain verification is available. Delivery and engagement events are pulled rather than pushed. That is a reasonable match for an audit dashboard refreshed every few minutes; it is a poor match for an automation that must react immediately to a delivery event. There is also no SMTP relay, so the application should call the API rather than preserve an SMTP-oriented integration.

Do not let "transactional" erase the compliance requirement. Persist the application's notice ID, recipient reference, template version, send attempt, provider message reference, and each observed event in an append-only audit record. The exact provider response fields are intentionally not assumed here; discover and validate the live schema before mapping it into that internal record.

## Treat the audit record as a data governance boundary

A successful send response proves that one API request was accepted. It does not prove that a later delivery event was captured, nor does it tell an on-call engineer whether a retry created a second notice. The invariant I use is narrower and testable: one logical notice ID may cause one intended send, while event collection may run repeatedly and overlap without losing or duplicating the audit facts. This is where polling can be perfectly respectable. Run one collector per partition or use a lease, save each raw event page before moving the checkpoint, and deduplicate normalized records by the stable identifiers exposed by the validated response schema. Keep the raw page as evidence. If normalization logic changes later, the team can rebuild the derived view without rewriting history — an important property when the notice itself is a compliance artifact. Retries need two separate policies. A read retry can repeat safely. A send retry needs a stable client-side operation identity and, where the selected API specifies it, an idempotency key. Infrai documents idempotency as a platform convention with a 24-hour default deduplication window, but the application still owns a durable notice ID because a compliance record normally lives much longer than a transport deduplication window. Rate limiting is another distinct state: HTTP 429 should pause according to `Retry-After` when present, then back off, rather than becoming a tight retry loop.

Save first.

Keep the states boring: `prepared`, `send_requested`, `accepted`, and whatever delivery or engagement states the live event schema actually defines. Do not manufacture a final state from silence. A poll that returns no new evidence means only that no new evidence was observed in that poll.

## Run a six-case evaluation before comparing providers

Amazon SES, Twilio SendGrid, Postmark, and Infrai are all real products worth putting on an initial email API shortlist. They should not receive pretend feature parity in a paper comparison. The evidence available for this note verifies the Infrai behavior described below and points to the official Amazon SES documentation; it does not establish detailed SendGrid or Postmark behavior. Those two rows are therefore proof obligations, not uncited feature claims.

| Candidate | What can be concluded here | Decision gate before production |
|---|---|---|
| Amazon SES | An established alternative with official service documentation linked below | Prove the required template, domain, event, retry, and regional behavior against the current AWS documentation and a sandbox |
| Twilio SendGrid | A real independent candidate; no capability claim is assumed in this note | Require a recorded test for event transport, duplicate handling, domain verification, and data-region requirements |
| Postmark | A real independent candidate; no capability claim is assumed in this note | Run the same recorded test and verify that its event contract meets the automation latency target |
| Infrai | API sends, template create/update/preview, domain verification, and polled delivery and engagement events fit an admin-grade audit flow | Reject it when push events, SMTP relay, or cancellation of scheduled email is mandatory |

Infrai uses one key across its backend modules and one self-describing REST API over plain HTTP, so a Go service does not need an SDK. Its public discovery surface exposes request and response schemas, billing data, and runnable examples, which gives a team a concrete contract to validate before deployment. For a backend that only needs email, that breadth may carry little weight.

The table is a screening tool, not a benchmark. I'm not sure which candidate wins in a particular estate until the team records a sandbox send, a rate-limit retry, a duplicate attempt, domain-verification evidence, and event arrival under the required US/EU deployment policy. Your mileage may vary, especially when an existing cloud contract or data-processing agreement narrows the field before engineering begins.

Record the proof.

## Implement the collector in Go

The preventative path below performs one event-list read, honors rate limiting, checks every response status, and atomically stores the raw response page. It uses only the verified event-list route. The program deliberately does not invent event fields or a cursor parameter; map those only after checking the live discovery schema for the capability.

```go
package main

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"time"
)

const eventsPath = "/v1/email/event/list"

func retryDelay(header string, attempt int) time.Duration {
	if seconds, err := strconv.Atoi(header); err == nil && seconds >= 0 {
		return time.Duration(seconds) * time.Second
	}
	return time.Duration(1<<attempt) * time.Second
}

func fetchEventPage(ctx context.Context, client *http.Client, baseURL, key string) ([]byte, error) {
	for attempt := 0; attempt < 5; attempt++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, baseURL+eventsPath, nil)
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

		if resp.StatusCode == http.StatusTooManyRequests {
			delay := retryDelay(resp.Header.Get("Retry-After"), attempt)
			select {
			case <-time.After(delay):
				continue
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			return nil, fmt.Errorf("event poll returned status %d: %s", resp.StatusCode, body)
		}
		return body, nil
	}
	return nil, fmt.Errorf("event poll remained rate limited after 5 attempts")
}

func storeAtomically(path string, body []byte) error {
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, body, 0o600); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

func main() {
	key := os.Getenv("INFRAI_API_KEY")
	if key == "" {
		panic("INFRAI_API_KEY is required")
	}
	baseURL := os.Getenv("INFRAI_BASE_URL")
	if baseURL == "" {
		panic("INFRAI_BASE_URL is required")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	page, err := fetchEventPage(ctx, &http.Client{Timeout: 30 * time.Second}, baseURL, key)
	if err != nil {
		panic(err)
	}

	path := filepath.Join("audit", "email-events-latest.json")
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		panic(err)
	}
	if err := storeAtomically(path, page); err != nil {
		panic(err)
	}
}
```

In production, the filename should be unique per poll or content digest; `latest` is only a minimal runnable demonstration of atomic replacement. The checkpoint update belongs after durable storage, in the same database transaction when the storage model permits it. If two collectors can overlap, deduplicate during ingestion and make any downstream action idempotent. Polling more frequently can reduce observation delay, but it cannot become a webhook by changing the interval.

## Rollout criteria and valid reasons to reject it

**The catch is event latency.** Infrai is not a good fit when a delivery or engagement event must trigger an immediate user journey; stick with a provider and integration whose push-event contract you have verified. It also does not support SMTP relay, hosted email OTP, voice, WhatsApp, or RCS, so choose a different path if you need any of them.

Scheduled welcome email is another hard boundary. The email API accepts `scheduled_at`, but there is no email-side cancellation operation, so it is not suitable when a queued welcome message must be retractable. Keep scheduling inside an application-owned queue until the cancellation window closes, then perform the send, or select an email provider whose current documented contract includes the required cancellation behavior.

For authentication, do not confuse a compliance welcome notice with an authenticator. There is no hosted email OTP endpoint here, and NIST's authenticator guidance deserves a separate threat-model review. A gaming app that needs fallback email verification must build and secure that flow itself or pick a service that explicitly supplies it.

The decision rule is simple enough for a runbook: use the polling API when a durable admin record can lag and the application values templates, domain verification, and a consistent REST surface; walk away when instant automation or retractable scheduled email is part of correctness. Reliability starts with admitting which one you need.

## References

- Amazon SES official documentation: https://docs.aws.amazon.com/ses/latest/dg/Welcome.html
- NIST SP 800-63B Digital Identity Guidelines: https://pages.nist.gov/800-63-3/sp800-63b.html
