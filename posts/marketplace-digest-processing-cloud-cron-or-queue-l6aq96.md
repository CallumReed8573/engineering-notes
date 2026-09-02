# Marketplace Digest Processing: Cloud Cron or Queue for Rate-Limited APIs?

Short answer: use cron only to open the weekly run, then put each digest task behind a queue when the upstream API is rate limited. That costs more operational machinery than one scheduled loop, but it gives latency a measurable budget and keeps retries from turning into duplicate customer updates. For a small, bounded digest, a single scheduled worker is the cheaper and easier choice.

I've been paged for missed jobs and duplicate deliveries. The lesson was not “always use a queue.” It was to decide where work is allowed to wait, and to make that waiting visible.

## Set the weekly deadline before choosing a cloud primitive

Imagine a marketplace with active customers, each needing a weekly digest assembled from a partner API. The scheduler wakes once, reads customers, calls the API, and sends the messages. That shape is attractive because it has few moving parts. It also puts scheduling, pacing, retry state, delivery state, and customer selection inside one execution. In a cloud deployment, a Vercel cron trigger or a GitHub Actions cron workflow can start that execution, but neither choice changes who owns the backlog or the API quota.

When the partner responds with a rate limit such as HTTP 429, the loop has two bad choices: rush and receive more refusals, or sleep while its execution window is consumed. A restart can repeat completed calls. A second invocation can process the same customer at the same time. The durable unit of work should therefore be a customer-digest item, identified by a stable key such as `customer_id:week`, rather than an in-memory cursor. A Node.js implementation has the same boundary as one written in Go: the language changes the client code, not the ownership model.

That is the invariant: the scheduler owns “start this run,” while the worker owns “process this item once, or make a repeat harmless.” A queue is useful because a consumer can claim an item temporarily, and the item can become visible again when the consumer does not finish. The visibility timeout is a lease, not proof of success; the SQS documentation describes the same operational concern directly.

Keep it boring.

## How should a cloud cron hand a rate-limited API batch to a queue worker?

Start with the latency budget. If the digest must be ready within 20 minutes and the partner allows 60 requests per minute, the maximum useful concurrency is constrained by that quota, not by the number of worker processes. The exact limit must come from the partner's current documentation. I'm not sure a fixed interval is right until that limit, the response cost, and the retry policy are known.

The scheduler creates a run record and enqueues work. The worker claims one item, waits for its shared rate limiter, calls the API, writes the result, and acknowledges the item only after the write and delivery decision are durable. A timeout should exceed the normal processing time but remain short enough that abandoned work is eventually retried. If processing can exceed that timeout, extend the lease deliberately; do not assume the broker will infer progress.

The cost comparison is mostly about failure handling. A scheduled loop has a lower setup cost and fewer components, but recovery logic becomes application code. A queue has broker and worker overhead, yet it makes backlog, retry count, and age inspectable. For a weekly job with a small customer set and a generous upstream quota, keep the loop. For an unpredictable customer count, strict quota, or expensive duplicate delivery, pay for the queue-shaped control point.

Here is the decision in operational terms:

| Condition | Start with | Reason |
|---|---|---|
| Small, bounded batch and tolerant deadline | Cloud cron plus one worker | Fewer components and a short recovery path |
| Large or variable batch with a strict API rate limit | Cron plus queue workers | Backlog and retry state become visible |
| Multiple dependent stages | Workflow orchestrator | The dependency graph needs durable coordination |

The table is a starting point, not a benchmark.

## Put the rate limit at the API boundary

The worker below shows the boundary that matters. It spaces calls in one process, honors a numeric `Retry-After` value, and sends an idempotency key derived from the durable work item. The queue adapter is intentionally omitted: its acknowledge operation belongs after `process` succeeds, and its visibility lease must be renewed for longer work.

```go
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"time"
)

type DigestItem struct {
	Key     string          `json:"key"`
	Payload json.RawMessage `json:"payload"`
}

func process(ctx context.Context, client *http.Client, item DigestItem) error {
	for attempt := 0; attempt < 5; attempt++ {
		req, err := http.NewRequestWithContext(
			ctx,
			"POST",
			os.Getenv("PARTNER_API_URL"),
			bytes.NewReader(item.Payload),
		)
		if err != nil {
			return err
		}
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Idempotency-Key", item.Key)

		resp, err := client.Do(req)
		if err != nil {
			return err
		}
		body, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil {
			return readErr
		}

		if resp.StatusCode == http.StatusTooManyRequests {
			wait := time.Duration(1<<attempt) * time.Second
			if seconds, parseErr := strconv.Atoi(resp.Header.Get("Retry-After")); parseErr == nil && seconds > 0 {
				wait = time.Duration(seconds) * time.Second
			}
			select {
			case <-time.After(wait):
			case <-ctx.Done():
				return ctx.Err()
			}
			continue
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			return fmt.Errorf("partner request returned %d: %s", resp.StatusCode, body)
		}
		return nil
	}
	return fmt.Errorf("rate limit persisted for %s", item.Key)
}
```

One process makes the interval easy to reason about. Several replicas do not: each local ticker can spend the same account quota. Use a shared limiter or assign an explicit quota slice to each worker. Also record request latency, response status, retry delay, queue age, item age, and the count of duplicate idempotency keys. Those signals tell an on-call engineer whether the problem is an upstream quota, a growing backlog, or a delivery transaction.

Priority is a policy choice, not a throughput fix. RabbitMQ's priority queue documentation notes that priorities affect ordering, while consumers and prefetch still shape what can be delivered. In a digest system, high-priority customers may reduce the latency of their own items while making ordinary items older. Define fairness and starvation limits before adding a priority field.

## When does a queue stop being the cheapest tool?

The catch is that a queue is not automatically suitable. If the weekly digest is a tiny, bounded job that can be retried as a whole and can tolerate a missed run, a scheduled process is easier to operate. Keep the simpler design when its worst-case duration fits the execution limit and its side effects are idempotent.

Choose an orchestrator when the work is a dependency graph: fetch several datasets, wait for all of them, then render and send one digest. Choose an event log when consumers need independent replay history. A basic work queue is a poor substitute for either requirement, because acknowledgement normally removes the item and a visibility timeout only governs temporary ownership.

Do not use price as the decision rule. Estimate the cost of the broker, workers, storage, and on-call time against the cost of duplicate API calls and late customer mail. Your mileage may vary because the decisive inputs are the partner quota, active-customer count, digest deadline, and recovery expectations.

The durable rule is modest: schedule the opening edge, queue the independently retryable work, and measure the delay between those two points. If that delay has no budget, no owner, and no alert, the architecture is incomplete regardless of which scheduler or broker starts it.

## Sources

- https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-visibility-timeout.html
- https://www.rabbitmq.com/docs/priority
