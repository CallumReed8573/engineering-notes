# Support Notification Events Explained — 4 Controls for Email and SMS

Route a marketplace contact form to a support queue before scheduling its email or SMS notification. Give that queue a named template owner, and preserve the routing decision with the accepted submission. A notification retry should continue the same support workflow, including the same approved template revision.

TL;DR: For a Node.js SaaS application, use four controls: durable acceptance, versioned template ownership, idempotent dispatch, and reconciliation. Treat notification handlers as workers consuming recorded intent. The form's success response should mean that its support request and dispatch intent have been committed; delivery status belongs to a separate lifecycle.

The operational question is concrete: if a seller submits a payout question and a worker restarts, can an operator determine which queue owns the request, which copy was approved, and whether another send is safe? Design around that investigation. A generic event name such as `contact.received` cannot answer it by itself.

## How should email and SMS notification events inherit support template ownership?

Start with a routing contract. For this example, suppose a marketplace routes `payout_question` to `seller-support` and `listing_question` to `catalog-support`. These are illustrative application values, not an external taxonomy. Assign each queue an accountable team for its email and SMS templates. The application team owns the event schema, required variables, and dispatch state machine. The queue's team approves the words and escalation instructions that recipients see.

Keep those responsibilities visible in one versioned template record: queue, channel, template ID, revision, owner, required variables, and approval status. A template revision should become eligible for new submissions only after its required variables match the event contract. Once selected, record its identity in the dispatch intent. That is the main ownership decision: who can approve a message that speaks for this support queue?

Freeze the decision. Keep the evidence.

Resolving the latest template during every retry creates an avoidable trap: one accepted contact request can acquire different instructions after a template deployment. Pinning the revision makes replay easier to explain, but requires retaining old revisions until their outstanding intents have been resolved. Choose that trade-off deliberately. For urgent corrections, provide an explicit migration or cancellation operation that records the old revision, replacement revision, reason, and approving owner.

Consider a replay drill with 2 attempts and 1 template deployment between them. The first attempt selects revision 4 for `seller-support`, submits the email, and loses its response. Before recovery, the queue owner approves revision 5 with a different escalation destination. If the recovering worker looks up the latest copy, the second attempt now changes both the delivery outcome and the instructions being sent. An operator cannot resolve those two questions from a single success flag. Preserve revision 4 on the intent, record the first attempt as uncertain, and require reconciliation before another send. The downside is explicit: retaining revision 4 prolongs the life of old content. If its instructions are unsafe, the owner must cancel or migrate the pending intent through an auditable operation. This is a proposed failure-injection exercise, not a claim about an observed incident.

For a contact form, notification content should also respect the destination. Prefer a request reference and an authenticated link over copying the entire free-text submission into an SMS. Make fallback between channels a policy owned by the workflow; a failed email attempt should not silently authorize a new SMS recipient.

## Read the signals before replaying work

Separate three questions in the runbook: was the support request accepted, did the dispatcher attempt the notification, and what delivery evidence exists? Use distinct states for each. A successful form submission must not become evidence that a message reached a recipient. Likewise, a submission without a recorded queue is a routing defect to resolve before dispatch.

Watch the age of the oldest eligible intent, unresolved routing decisions, template validation failures, and attempts whose outcome is unknown. Break these signals down by queue, channel, and template revision. Use request and intent identifiers for trace lookup; keep recipient addresses and contact-form text out of metric labels. Set alert thresholds from the support response objective rather than inventing a universal number of seconds.

The awkward case deserves its own state. Uncertainty isn't success.

A worker can lose its connection after submitting a message but before recording the result. Mark that attempt as uncertain. An automatic retry is safe only to the extent that the receiving interface can recognize the same operation or supply evidence that resolves the uncertainty. A local deduplication record alone cannot establish what happened outside its transaction.

For email investigations, keep authentication separate from workflow state. RFC 7208 defines SPF authorization checks for hosts using a domain in SMTP identities. An SPF result does not establish that the support queue is correct or that a notification reached a person's inbox. Add authentication evidence to the investigation without treating it as a delivery receipt.

## Record intent before a worker sends anything

For a Node.js implementation, make request acceptance an awaited durable operation, then let workers process its recorded intents. Keep process-local event hooks useful for coordination, but assign recovery responsibility to stored state. The following Go example expresses the acceptance contract independently of the web framework. All identifiers and revisions are example data.

```go
package routing

import (
    "context"
    "errors"
)

type Template struct {
    ID       string
    Revision int
    Owner    string
}

type Contact struct {
    SubmissionID string
    Queue        string
    RequestRef   string
}

type Intent struct {
    Key      string
    Channel  string
    Template Template
    Contact  Contact
}

// Implementations must atomically commit the contact and its intents.
// A repeated submission ID with the same payload returns the prior result.
// A repeated ID with a different payload must return a conflict.
// Intent keys must have a durable uniqueness constraint.
type Store interface {
    Accept(context.Context, Contact, []Intent) error
}

func AcceptPayoutQuestion(
    ctx context.Context, store Store, submissionID, requestRef string,
) error {
    if submissionID == "" || requestRef == "" {
        return errors.New("submission ID and request reference are required")
    }
    contact := Contact{submissionID, "seller-support", requestRef}
    intents := []Intent{
        {
            Key: submissionID + ":email:ack", Channel: "email",
            Template: Template{"payout-ack-email", 4, "seller-support"},
            Contact: contact,
        },
        {
            Key: submissionID + ":sms:ack", Channel: "sms",
            Template: Template{"payout-ack-sms", 2, "seller-support"},
            Contact: contact,
        },
    }
    return store.Accept(ctx, contact, intents)
}
```

This example assumes the workflow has already authorized both channels and validated recipient destinations. It deliberately leaves storage behind an interface: the comments are requirements for a transactional implementation, not guarantees supplied by the function. Validate the selected template's required variables and approval status before committing. Resolve destinations through an authorized recipient record, using the intent's reference rather than trusting a browser-supplied queue or recipient.

Notice that the intent key excludes the template revision. Updating copy should not create a second acknowledgment for the same submission and channel. An intentional repeat delivery needs a separately authorized action identifier. Give the originating submission a stable identifier across retries too, and reject conflicting payloads so deduplication cannot silently attach new text to an old request.

Workers should claim eligible intents with recoverable leases, preserve the intent key across retries, and record attempt history separately from the logical notification. Use bounded retry delays and distinguish retryable failures from terminal validation errors. After the retry budget is exhausted, retain the intent for investigation. Deleting it would erase the evidence needed to reconcile the contact request.

## Verify the boundaries that can lose or duplicate a message

Test failure points, not just successful sends. Start with duplicates.

Submit the same request twice and verify one accepted support request with one intent per authorized channel. Reuse its identifier with changed content and verify rejection. Interrupt acceptance before commit and confirm there is no partial request. Then stop a worker after claiming an intent and verify that lease recovery makes the work eligible again without changing its template revision.

The hardest test is the uncertain external result. Simulate acceptance by the sending interface followed by a lost response. Verify that the runbook reconciles through the interface's documented idempotency or status mechanism before permitting another attempt. Where that evidence is unavailable, keep the uncertainty explicit and assign a human decision; the system should not label a guess as confirmed delivery.

Deploy template revisions first to a controlled test queue. Check missing variables, incorrect queue ownership, an unauthorized SMS path, and the authenticated link's destination. Compare the rendered email and SMS against the intended support action. Template approval is incomplete if the wording is correct but the recipient is directed to the wrong queue.

## Pause dispatch and roll back with the evidence intact

When a bad template or routing rule reaches dispatch, pause the affected queue and channel. Keep accepting requests only if their routing and durable storage remain valid; expose backlog age to the operator. Roll new submissions back to a previously approved revision. Preserve the pinned revision on existing intents until their owners decide to resume, migrate, or cancel them.

Do not replay everything. Check uncertain attempts.

Reconcile uncertain attempts first, then resume the smallest affected scope and check its queue assignments and delivery evidence. Keep a record of who authorized any repeat delivery.

This design adds retained template revisions, attempt records, and reconciliation work. Those have storage and operational costs, while independent email and SMS policies add review work for support teams. Accept that overhead when a missed or duplicated contact acknowledgment would make the support record ambiguous. The useful outcome is an accepted request whose queue, approved message, and dispatch history remain explainable after a restart or rollback.

## References

- RFC 7208: Sender Policy Framework (SPF), including its authorization scope: https://datatracker.ietf.org/doc/html/rfc7208
