# Seller Alerts via Transactional Email API — SaaS Setup, DKIM, Bounce Suppression, Event Polling

The operational constraint is ownership: the team that can be paged for a malformed or misleading seller notification needs a controlled way to change it. **Short answer:** choose an HTTP transactional-email API only after deciding where templates live, then require authenticated sending domains, durable suppression state, signed delivery events, and a reconciliation path. A provider-hosted template is useful when non-engineers must edit copy quickly; an application-owned template is safer when each release needs review, reproducibility, and rollback. For a marketplace new-order message, I would keep the transactional contract and rendering tests in the application, while allowing narrowly bounded content fields to have separate ownership.

That answer matters more than a feature-count ranking. SPF, DKIM, and DMARC establish identity and policy, but they cannot rescue a broken amount, an unsafe substitution, or a duplicate send. A polished control plane can still leave the on-call engineer unable to reproduce what a seller received.

## How should SaaS teams set up a transactional email API for deliverability and bounce suppression?

I have been paged for missed jobs and duplicate deliveries. I first assumed the queue acknowledgment would be enough to reconstruct what happened. It was not. The incident pattern is familiar: the first dashboard shows an API request, the queue shows an acknowledgment, and neither record proves which rendered message reached the recipient. The invariant I take from those incidents is blunt: one immutable notification intent must map to one stable idempotency key, one template revision, and an auditable sequence of delivery states.

Consider `order-7f2c` becoming ready for seller notification. The intent should contain the seller address, locale, order reference, template revision, and facts needed for rendering. It should not contain an arbitrary HTML document supplied by a browser. Persist that intent before calling the delivery API. A worker can then retry after a timeout without creating a new logical notification.

This is where the ownership models diverge. With application-owned templates, source review and deployment pin the exact revision, local tests can render fixture data, and rollback follows the normal release path. The cost is that a copy edit waits for engineering delivery. With provider-hosted templates, an operations or support team can change approved content without an application release, but the application must persist the remote template identifier and revision or snapshot enough rendered evidence for an investigation. A hybrid model can work, provided the boundary is explicit: engineers own structure and required variables; authorized content editors own limited copy fields; every change produces a version.

Do not infer success from an HTTP 2xx response. That response can acknowledge acceptance while later delivery events report a bounce, complaint, or rejection. Acceptance and delivery are different states.

Keep that distinction visible.

## The control plane starts in DNS

Use a dedicated sending subdomain for marketplace notifications so its identity and reputation boundary are understandable. Publish the records the selected service requires, verify them before enabling production traffic, and monitor for later drift. SPF authorizes hosts for a domain, DKIM attaches a verifiable signature, and DMARC tells receivers how to evaluate alignment and report authentication results. DMARC alignment concerns the visible From domain and the authenticated SPF or DKIM domain; merely having all three records is not enough.

Roll out enforcement deliberately. RFC 7489 defines `p=none`, `quarantine`, and `reject`, and its `pct` tag applies policy to a percentage of messages. Reports collected during a monitoring phase help reveal legitimate senders that were omitted from the inventory. The end state should match the organization's policy, but changing directly to rejection without accounting for every sender can block valid mail.

US and EU operation does not reduce to a region checkbox. Record where recipient data, message content, event payloads, logs, backups, and support access are processed; then compare those paths with the organization's legal and retention requirements. The API selection evidence is a data-flow diagram and contract review, not a marketing label.

## Make suppression a local safety invariant

A provider suppression list is necessary operational input, not the system of record for notification eligibility. Store normalized suppression state locally and update it from authenticated events. Before every attempt, check the current state. A hard bounce or complaint should stop future sends according to the policy your organization has defined; transient failures need a bounded retry schedule rather than permanent suppression.

The dangerous race is an event arriving while another worker already holds a queued retry. Resolve it transactionally: lock the notification and recipient state, recheck suppression immediately before dispatch, and record the provider request identifier after acceptance. Assume events are duplicated and out of order. Event handlers therefore need their own deduplication key and a state transition table that rejects regressions, such as moving from delivered back to accepted because an older event arrived late.

Polling can reconcile gaps, but it is a backstop. Signed webhooks usually provide the timely path; a scheduled reconciler queries unresolved accepted messages after a defined age and closes missing transitions. Keep the polling interface narrow so a provider change does not rewrite the order service.

## Put the retry contract in code

The preventive path below is deliberately generic. It renders a pinned revision, checks suppression inside the send transaction, and derives the idempotency key from the business event rather than from an individual queue attempt. The transport is an HTTP interface, so no SMTP relay is required.

```go
package notify

import (
    "context"
    "crypto/sha256"
    "encoding/hex"
    "errors"
)

type OrderNotice struct {
    OrderID          string
    SellerID         string
    SellerEmail      string
    Locale           string
    TemplateRevision string
}

type Sender interface {
    Send(ctx context.Context, message Message, idempotencyKey string) (requestID string, err error)
}

type Store interface {
    WithRecipientLock(ctx context.Context, email string, fn func(State) error) error
    MarkAccepted(ctx context.Context, orderID, requestID, revision string) error
}

type Renderer interface {
    RenderNewOrder(notice OrderNotice) (Message, error)
}

type State struct{ Suppressed bool }
type Message struct{ To, Subject, HTML, Text string }

func Deliver(ctx context.Context, notice OrderNotice, store Store, render Renderer, sender Sender) error {
    message, err := render.RenderNewOrder(notice)
    if err != nil {
        return err
    }

    sum := sha256.Sum256([]byte("seller-new-order:" + notice.OrderID + ":" + notice.SellerID))
    key := hex.EncodeToString(sum[:])

    return store.WithRecipientLock(ctx, notice.SellerEmail, func(state State) error {
        if state.Suppressed {
            return errors.New("recipient is suppressed")
        }
        requestID, err := sender.Send(ctx, message, key)
        if err != nil {
            return err
        }
        return store.MarkAccepted(ctx, notice.OrderID, requestID, notice.TemplateRevision)
    })
}
```

The example leaves retry classification outside `Deliver` on purpose. The queue worker needs to distinguish transport timeouts and rate limits from validation failures, then apply bounded backoff and a dead-letter policy. It must reuse the same key on every retry. A random UUID generated per attempt defeats the protection.

There is one subtle failure window: the remote service can accept a request just before the local acceptance write fails. The stable key makes replay safe only if the chosen API honors idempotent submission with documented scope and retention. If it does not, record an outbox entry in the same transaction as the order transition and reconcile ambiguous results before sending again. Exactly-once delivery is not a credible external guarantee; controlled at-least-once processing plus deduplication is.

## Evaluate evidence, not the demo

Run a proof with a domain reserved for evaluation and a test recipient set you control. Verify DNS ownership, DKIM signing, SPF and DMARC alignment, bounce ingestion, complaint ingestion, suppression checks, webhook signature validation, event replay, and reconciliation. Then disable or alter a test record and confirm that monitoring notices verification drift. No production recipient should be part of this exercise.

The selection record should answer a small set of operational questions:

- Can the API accept a caller-supplied idempotency key, and what are its scope and retention semantics?
- Can an event be verified cryptographically, replayed for recovery, and correlated to the original request?
- Can the team export suppression state and delivery evidence without scraping a dashboard?
- Does template revision history identify who changed what, and can a send pin a revision?
- Are data location, retention, deletion, and support-access boundaries documented for every required region?
- Do rate limits, retry guidance, and status semantics let the queue distinguish retryable from terminal failure?

Use the answers to compare candidates; do not average them into a cosmetic score. Missing idempotency or unverifiable events can be a veto for an order workflow, while a slower template editor may be tolerable. Cost belongs in the capacity plan after these controls pass.

This approach has real limitations. It does not apply unchanged to bulk campaigns, where consent, segmentation, scheduling, and unsubscribe workflows dominate, and application-owned rendering is a poor fit when a regulated content team must control every word without waiting for a software release. Choose externally managed templates in that case, but pin revisions and archive the evidence needed to reconstruct each send. The trade-off is more configuration drift in exchange for independent editorial control. The invariant survives the ownership choice.

## Sources

- https://datatracker.ietf.org/doc/html/rfc7208
- https://datatracker.ietf.org/doc/html/rfc6376
- https://datatracker.ietf.org/doc/html/rfc7489
- https://www.rfc-editor.org/rfc/rfc9110
- https://cheatsheetseries.owasp.org/cheatsheets/Webhook_Security_Guidelines_Cheat_Sheet.html
