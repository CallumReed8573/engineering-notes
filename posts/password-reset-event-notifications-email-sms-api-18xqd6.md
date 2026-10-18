# Password Reset Event Notifications: Email SMS API Polling Across 6 Template Boundaries

The page says password-reset event notifications have exceeded their error budget. The email and SMS API accepted requests, but status polling still shows nonterminal records near expiry. What the page cannot answer is more important: whether those learners received a usable message before the reset token expired.

TL;DR: keep token expiry and message intent in the identity service, keep channel rendering in one explicitly owned template package, and treat provider delivery state as evidence rather than truth. Poll only nonterminal records, honor `Retry-After` after a 429 response, add jitter to capped exponential backoff, and stop when either the provider reaches a terminal state or the reset expires. Alert on expiring, unresolved resets; a raw 429 count is an early signal, not the page.

## How should email and SMS API polling handle event notifications?

Work backward from the learner's failed action. A password-reset message has a short useful life, so the operational deadline comes from the reset record, not from an arbitrary polling timeout. The identity service should persist an opaque reset identifier, `expires_at`, intended channels, and a stable idempotency key before enqueueing work. It must not put a reset token in logs, metric labels, queue names, or delivery-status requests. NIST treats out-of-band authentication carefully and says secrets must be protected in transit; the same restraint belongs in recovery telemetry.

The expiry is the deadline.

The first useful warning is deadline pressure: count nonterminal attempts whose next permitted poll is approaching `expires_at`. Acceptance by an email or SMS API only proves that the downstream system accepted a request. It does not prove inbox placement, handset delivery, or that the learner used the link. A DMARC pass is useful evidence about authenticated email handling, but RFC 7489 does not turn it into proof of human receipt.

Page on the ratio of reset intents that expire without a terminal outcome, split by channel and template version. Ticket on sustained polling delay. Record 429 responses as saturation signals. This ordering keeps the page tied to user impact while leaving enough breadcrumbs to diagnose a rate-limit event.

## Who owns the words and the clock?

Template ownership is the fault line. For an edtech reset flow, the identity team should own the security semantics: why the message was sent, the expiry timestamp, the destination context that is safe to reveal, and the rule that a reset can be consumed once. A communications team may own HTML, SMS-length editing, localization, and accessibility. The handoff must be a versioned data contract, not an unstructured map that lets a template invent security policy.

I use six boundaries in the runbook:

1. Identity creates the reset and its expiry.
2. The outbox creates one immutable message intent with an idempotency key.
3. The template package renders a specific version for email or SMS.
4. A channel adapter submits the rendered payload and stores the remote message identifier.
5. A status worker polls only due, nonterminal attempts.
6. The reset service decides whether the credential-recovery action is still valid.

That split has a deliberate cost: changing expiry wording may require coordinated deployment of the identity contract and template version. I accept that friction because independently changing the displayed lifetime and the enforced lifetime is a worse failure. The message can say "10 minutes" only when its input carries the actual expiry needed to render that claim correctly; no template should hard-code it.

Keep old template versions reproducible until their outstanding messages have expired. A retry must render the same semantic message, not silently pick up today's copy. Otherwise an incident review cannot tell what a learner actually received.

This architecture has limitations. Coordinated template contracts add release work, polling consumes quota, and an internal state machine can lag a channel's newly introduced status. It is not suitable when a single process sends a tiny volume and can obtain authoritative terminal events through an authenticated callback without adding another failure domain. In that case, choose the callback and retain a slow reconciliation poll. Choose polling when callbacks cannot reach the deployment, when status gaps must be repaired, or when the receiver cannot yet verify and durably deduplicate incoming events. The trade-off is operational control against more scheduler state and API pressure; neither transport proves that a person read the message.

## Polling without turning 429 into a retry storm

The worker needs very little cleverness. Persist `attempt`, `next_poll_at`, the last normalized state, and the provider's opaque message identifier. Claim due rows with a lease so two workers cannot poll the same attempt concurrently. On a terminal response, write the state once and stop. On a transient failure, schedule another poll.

RFC 6585 defines 429 as "Too Many Requests" and allows a response to include `Retry-After`. RFC 9110 defines that header as either an HTTP date or a delay in seconds. Honor a valid value as the earliest retry time. When it is absent or invalid, use capped exponential backoff with full jitter. Do not sleep while holding a queue lease; persist the next time and release the work.

```go
package polling

import (
	"math/rand/v2"
	"time"
)

func NextPoll(now, expiresAt time.Time, attempt uint, retryAfter *time.Time) (time.Time, bool) {
	if !now.Before(expiresAt) {
		return time.Time{}, false
	}

	capDelay := 30 * time.Second
	base := 500 * time.Millisecond
	delay := base << min(attempt, 6)
	if delay > capDelay {
		delay = capDelay
	}

	next := now.Add(time.Duration(rand.Int64N(int64(delay) + 1)))
	if retryAfter != nil && retryAfter.After(next) {
		next = *retryAfter
	}
	if !next.Before(expiresAt) {
		return time.Time{}, false
	}
	return next, true
}
```

The `min(attempt, 6)` cap prevents a shift overflow and bounds the backoff input; the separate 30-second cap makes the policy visible. Those values are examples, not universal defaults. Derive production values from the reset lifetime, published API limits, queue latency, and the amount of status traffic the system can safely create.

Idempotency applies twice. Submission deduplication prevents a worker retry from creating another email or SMS. Poll deduplication prevents duplicate observations from reopening a terminal record. Use a monotonic state transition rule, retain the raw response outside high-cardinality metrics, and quarantine an unrecognized status instead of guessing that it means failure.

No guessing.

## The instrumentation change

The useful trace begins at `reset_intent_id` and crosses the outbox, render, submit, poll, and terminal transition. Put that identifier in structured logs and traces, but keep email addresses, phone numbers, reset tokens, and message bodies out. Metrics should use bounded labels such as channel, normalized state, template version, and error class. Remote message identifiers belong in logs or storage, not metric labels.

Add four measurements: time from intent creation to submission, age of the oldest due poll, unresolved attempts by seconds until expiry, and terminal outcomes by template version. Then add one invariant metric: a single intent producing more than one remote submission. Zero is the target. This catches the duplicate-delivery path that a success-rate dashboard hides.

The alert trace should make one failure chain legible without requiring a responder to join records by hand: an immutable intent enters the outbox, a named template version renders it, one channel submission receives a remote identifier, each poll advances or preserves a monotonic state, and expiry closes the attempt even if a late downstream event arrives. A gap in that chain is itself a signal. Missing render metadata points to contract or deployment trouble; repeated submission timestamps with one idempotency key point to a lease or deduplication failure; growing `next_poll_at` values alongside 429 responses point to constrained status capacity; and a terminal delivery after expiry explains the notification system without pretending the learner could still use the reset. This is why one success counter cannot support the page.

A deployment should canary a template and its contract together. Render golden cases for every locale, verify that email and SMS show the same enforced expiry, and test late status responses after the reset is already invalid. Exercise a 429 response with both forms of `Retry-After`, a missing header, malformed status data, a worker crash after submission, and two workers claiming the same row. The last two tests are where idempotency claims become evidence.

For email, separately monitor domain-authentication results and aggregate delivery outcomes. DMARC policy and alignment help receivers evaluate authenticated mail; they do not replace application-level correlation. For SMS, normalize channel-specific status values into a small internal state machine while retaining the original value for diagnosis. The adapter owns that translation, which prevents provider vocabulary from leaking into identity logic.

## When does the alert become noise?

A threshold set too close to intent creation pages during ordinary queue and carrier variance. Set it too close to expiry and on-call has no time to act. The useful warning window is therefore an operational choice: measure normal end-to-end latency, reserve enough time for at least one controlled retry, and alert when the unresolved tail consumes that reserve. Revisit it whenever the reset lifetime or polling policy changes.

Do not page on every 429. A brief burst can be handled by `Retry-After` and the scheduler; paging on it teaches responders to ignore the channel. Page when rate limiting creates deadline risk, when the oldest due poll keeps aging, or when duplicate submissions breach the invariant. Keep the raw 429 rate on the dashboard so the earlier signal remains visible during diagnosis.

The final runbook action is intentionally plain: pause new polling pressure if it threatens submissions, confirm that leases and idempotency keys are holding, and let expired resets terminate cleanly. Never extend a credential's validity merely to make a delivery metric green. The security clock owns the outcome.

## Further reading

- https://datatracker.ietf.org/doc/html/rfc6585#section-4
- https://www.rfc-editor.org/rfc/rfc9110.html#name-retry-after
- https://datatracker.ietf.org/doc/html/rfc7489
- https://pages.nist.gov/800-63-3/sp800-63b.html
