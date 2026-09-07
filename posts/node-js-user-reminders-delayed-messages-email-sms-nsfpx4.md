# Node.js User Reminders: Delayed Messages, Email/SMS, Time Zones, and Public Webhooks

The operational constraint is simple: a weekly digest must not become a second digest when a worker retries. **Short answer: store each reminder as a UTC due time plus its named time zone, let a small cron scan enqueue work, and make the delivery ledger idempotent before sending email or SMS.** A public webhook should trigger the scan, not perform delivery.

This is a healthtech workflow, so the useful measure is not how quickly a timer fires. It is whether an active customer receives one complete digest, at the intended local time, with an audit trail that an operator can explain. The scheduler finds due records. The queue absorbs work and redelivery. The worker owns provider calls. Keep those responsibilities separate.

## Start with a delivery record, not a timer

Start with a reminder record containing a customer identifier, channel, local wall-clock choice, IANA time-zone name, a UTC `due_at`, and a version. Add a delivery record with a stable operation key before the first enqueue. This makes the database the answer to “should this customer get a digest?” instead of leaving that decision inside a timer or worker. Recalculate `due_at` when the customer edits the schedule. A fixed offset is not a time zone: daylight-saving changes can move a local reminder by an hour.

For reminders that can be slightly late, a periodic due scan is easier to reason about than a timer per customer. Delayed messages can reduce scan work for near-term deliveries, but they do not remove the database record or the idempotency check. Keep a durable record until the business retention policy says the delivery audit may be removed.

The queue is a transport, not the system of record.

## How should Node.js reminders handle time zones and delayed messages?

Time-zone policy needs an explicit product decision. A local time can be nonexistent or repeated around a daylight-saving transition. Pick and document the behavior, then test it with the named zone rather than an offset. I'm not sure which policy a particular healthtech product will choose; that is a product and support decision, not something a queue can infer safely.

The public webhook endpoint belongs on the scan side of this boundary. It should authenticate the scheduler, claim a bounded page of due records, enqueue jobs, and return. It should not render a digest or call email and SMS providers while the request is open. A public endpoint needs authentication, rate limits, request logging, and a short timeout; reachability is not trust.

## What does a queue retry mean after email or SMS acceptance?

Queues commonly provide at-least-once delivery. A worker can successfully submit an email, lose its acknowledgement, and receive the same job again. RabbitMQ's acknowledgement documentation describes this boundary clearly: acknowledgement is a consumer protocol event, not proof that an external side effect happened exactly once.

That is where duplicate reminders come from. The fix is a stable operation key, for example `digest:<customer_id>:<week>:<channel>:<version>`, with a unique constraint in a durable delivery ledger. The worker claims that key, performs the provider operation, records the result, and acknowledges the queue message only after the state is durable. A redelivered job that finds a completed key becomes an acknowledged no-op. I've been paged for missed jobs and duplicate deliveries; the second case often looks healthy in queue metrics because every retry is technically succeeding, while the customer sees two messages.

Do not generate the key inside the worker from a random UUID.

A retry would look like a new delivery.

The ledger also needs an explicit state model: pending, accepted, retryable, and terminal failure are different facts. A rate-limit response should follow the provider's retry guidance and remain retryable. A permanent recipient or address error should be visible to support and should not spin forever. Set a retry ceiling and alert on the dead-letter path. “Retry forever” is not resilience; it is an outage with a quiet dashboard.

| Signal | Worker action | Durable evidence |
|---|---|---|
| Provider accepted | Record success, then acknowledge | Provider response and operation key |
| Rate limited or transient failure | Keep available with bounded backoff | Attempt count and next-attempt time |
| Permanent recipient failure | Stop retrying and alert support | Terminal error and operation key |

The article is aimed at a Node.js team, but the contract is language-neutral. This Go fixture shows the boundary that deserves a test: duplicate queue deliveries produce one logical send. The in-memory ledger is only a focused example; production state belongs in a durable store with a unique constraint.

```go
package main

import (
	"fmt"
	"log"
	"sync"
)

type Job struct {
	CustomerID string
	Week       string
	Channel    string
	Version    int
}

type Ledger struct {
	mu   sync.Mutex
	done map[string]bool
}

func (l *Ledger) Deliver(job Job) bool {
	key := fmt.Sprintf("digest:%s:%s:%s:%d", job.CustomerID, job.Week, job.Channel, job.Version)

	l.mu.Lock()
	defer l.mu.Unlock()
	if l.done[key] {
		return false
	}

	// In production, commit the claim and provider result durably.
	log.Printf("send customer=%s channel=%s week=%s", job.CustomerID, job.Channel, job.Week)
	l.done[key] = true
	return true
}

func main() {
	ledger := &Ledger{done: make(map[string]bool)}
	job := Job{CustomerID: "cust-1042", Week: "2026-W32", Channel: "email", Version: 1}

	ledger.Deliver(job)
	ledger.Deliver(job) // Simulated at-least-once redelivery.
	log.Println("second delivery was suppressed")
}
```

The important test is the state transition, not the log line. Run the fixture, then repeat the same job with the same version. One send should be recorded. Change the version only when the digest's intended content or schedule is a new operation. A new queue message for the same operation must not bypass the ledger.

## Verification, rollout, and rollback

Before real delivery, send synthetic records through email and SMS paths and capture scheduled, enqueued, worker-started, and provider-accepted timestamps. Test a customer in a named time zone on both sides of a daylight-saving transition. Publish the same job twice. Confirm one provider call and one ledger entry. Small test. Big payoff.

Also test a lost acknowledgement, a retryable provider response, a permanent recipient error, an expired job, and an unavailable public webhook. The cron manual documents scheduling semantics and cautions that cron is a trigger, not a transaction log. A missed trigger must therefore be recoverable from the due records and a stored watermark.

Roll out in shadow mode: scan and enqueue, but point workers at a sink that records intended deliveries. Compare due records with unique operation keys. Enable one internal cohort next, then watch queue age, duplicate suppressions, terminal failures, and end-to-end lag. The rollback switch should stop new scans and pause consumers while preserving reminder records and the ledger. After recovery, run a bounded catch-up scan from the last successful watermark.

Keep the watermark.

The catch is that this design is not suitable when the job is a multi-step workflow needing joins, long-running human approvals, or event replay across many independent consumers. Use a workflow engine or event log when those are real requirements. Stick with a due-record scan and a delivery queue when the job is an independent weekly notification; adding orchestration machinery creates another operating surface without improving duplicate control.

## References

- https://man7.org/linux/man-pages/man5/crontab.5.html
- https://www.rabbitmq.com/docs/confirms
