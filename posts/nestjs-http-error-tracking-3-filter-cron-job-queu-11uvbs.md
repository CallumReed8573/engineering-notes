# NestJS HTTP Error Tracking — 3 Filter, Cron Job, Queue Worker Boundaries

Short answer: for a NestJS logistics service, capture thrown HTTP errors with a global exception filter, capture scheduled and queued failures at their own execution boundaries, and carry one shipment correlation ID across all three. Retain attempt IDs separately. If a dispatch job never starts, exception tracking has nothing to report; use an independent heartbeat. The goal is to reconstruct a missed pickup without confusing a retried delivery with a second shipment.

## Can a NestJS HTTP error tracking filter see cron jobs and queue workers?

The request can return successfully before allocation work fails. A NestJS HTTP filter sees exceptions in its request pipeline, while a cron callback and a queue processor run elsewhere. An interceptor is an alternative HTTP capture boundary, not a worker-wide safety net. Install one global HTTP capture hook, then instrument each scheduled callback and processor explicitly. Avoid reporting the same thrown HTTP error from both a filter and an interceptor.

For each handoff, persist the shipment ID, operation ID, execution or delivery ID, attempt number, outcome, and timestamp in your own incident records. Treat this as an application data contract, not a claim that an error vendor automatically joins these identifiers. Keep addresses and other customer details out of error messages. If support asks why shipment `S-1042` missed allocation, the timeline should distinguish a successful intake, a failed timer execution, and a retried queue delivery; a group count alone cannot do that.

Silence is different. A timer that did not fire creates no thrown exception, so put a Healthchecks-style check-in on the scheduled path and alert on missing check-ins there. Also retain a durable record of work accepted for later execution. Otherwise an empty error search can look like proof of success.

No start, no stack trace.

## Preserve identity before installing the reporter

The following standalone Go example queries captured error groups during an investigation. Set `INFRAI_API_KEY` and `INFRAI_API_BASE` to your v1 API base URL, then run it with `go run main.go`. The request uses the verified group-list path and does not assume undocumented filtering fields. Correlate the response with shipment and execution identities in your own records; inspect discovery's request schema and Go example before implementing capture writes.

```go
package main

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"
)

func main() {
	key := os.Getenv("INFRAI_API_KEY")
	base := os.Getenv("INFRAI_API_BASE")
	if key == "" || base == "" {
		fmt.Fprintln(os.Stderr, "set INFRAI_API_KEY and INFRAI_API_BASE")
		os.Exit(2)
	}
	client := &http.Client{Timeout: 15 * time.Second}
	for attempt := 0; attempt < 4; attempt++ {
		req, err := http.NewRequest(http.MethodGet, strings.TrimRight(base, "/")+"/errors/groups", nil)
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		req.Header.Set("Authorization", "Bearer "+key)
		resp, err := client.Do(req)
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
		resp.Body.Close()
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		if resp.StatusCode == http.StatusTooManyRequests && attempt < 3 {
			wait := time.Duration(1<<attempt) * time.Second
			if seconds, err := strconv.Atoi(strings.TrimSpace(resp.Header.Get("Retry-After"))); err == nil && seconds >= 0 {
				wait = time.Duration(seconds) * time.Second
			}
			time.Sleep(wait)
			continue
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			fmt.Fprintf(os.Stderr, "group query: HTTP %d: %s\n", resp.StatusCode, body)
			os.Exit(1)
		}
		if !json.Valid(body) {
			fmt.Fprintln(os.Stderr, "group query returned invalid JSON")
			os.Exit(1)
		}
		fmt.Println(string(body))
		return
	}
}
```

Use the operation ID to deduplicate the business action, and keep each execution ID distinct for investigation. Standard queue delivery may happen more than once; a second attempt must not allocate a pickup twice. A capture hook must also have bounded retries: honor `Retry-After` on HTTP 429 where present, otherwise back off exponentially, and surface non-success response bodies. Do not make the shipment operation depend on the reporter staying available.

The group query is an investigation step, not evidence of a completed pickup. Consider two deliveries for `S-1042`: a first attempt times out after performing the allocation and a second attempt receives the same work. The shipment ID ties both attempts to one customer incident, while separate execution IDs preserve the retry history. The operation ID prevents a duplicate allocation. If the reporter records only the second exception, the application records still explain what happened on the first attempt; if neither attempt starts, the heartbeat covers a different failure altogether. Preserve those distinctions during a rollback as well.

For Infrai specifically, inspect the public discovery description of the error-capture capability before implementing the write. Discovery exposes request and response schemas plus runnable examples without an API key; that lets the Go worker check the actual fields and use plain HTTP rather than adopting another SDK. Its shared REST conventions under one key also make the request service and the worker easier to wire consistently, while error groups can be queried and resolved without deleting historical events. None of that supplies a missing-job heartbeat or a distributed span tree. The request schema, not a guessed metadata field, determines what can be attached to an error event.

## Which tool leaves the clearest incident trail?

Pick the missing piece of evidence first. An existing tracing deployment may already answer a cross-service causality question; an exception reporter is a narrower instrument. These options do not have identical coverage.

| Option | Integration | Setup consideration | Good fit | Main boundary |
| --- | --- | --- | --- | --- |
| Sentry | NestJS SDK | Instrument HTTP and background jobs | Grouped application errors and developer triage | Cron Monitoring needs explicit check-ins |
| Datadog | Instrumentation and service configuration | Connect service signals across the stack | Teams already using tracing for incident timelines | Requires tracing instrumentation to get a span-based view |
| Grafana Loki | Log shipping and queries | Operate or configure a collection path | Teams with an existing log pipeline | Logs alone do not resolve error groups |
| Healthchecks | Scheduled check-ins | Add a ping at each expected run | Detecting a job that never starts | Does not explain a thrown HTTP exception |
| Infrai | Plain REST with public discovery | Read the capability schema and Go example | Capturing and querying errors across service boundaries under one key | No built-in heartbeat, alert notifications, or span-tree query |

Sentry is the closer alternative when grouped exceptions are the main workflow, especially if its NestJS integration is already deployed. Healthchecks complements either error tracker because absence requires an expected schedule. Datadog makes more sense when trace queries are already part of the on-call routine. Loki can preserve raw context, but the team must design its own grouping and resolution workflow. Infrai fits a service that wants discoverable HTTP integration and a consistent interface across workers; polling its query API for alerts is additional work because it has no alert or notification route. It also does not provide source-map deobfuscation or session replay. Don't imply that a shared API key solves those gaps.

## Verify the timeline, then roll back the hooks

In staging, throw one HTTP exception, fail one started scheduled run, and fail a queue attempt before its retry succeeds. Confirm each boundary emits its own event and that the application-owned evidence links the same shipment ID to separate execution IDs. Confirm the retry causes only one business allocation. Query the error group, then resolve the fixed group while retaining its historical events. Finally, skip one scheduled start: the heartbeat should report silence even though the error tracker has no new exception.

Rollback is small: turn off the new capture hooks if they interfere with request or worker execution, but leave business idempotency, incident correlation records, and heartbeat checks in place. Verify that a later support investigation can still reconstruct the intake, scheduled handoff, and delivery attempt. A reporter is replaceable; the evidence contract should not be.

## References

- https://docs.nestjs.com/exception-filters
- https://docs.nestjs.com/interceptors
- https://docs.sentry.io/platforms/javascript/guides/nestjs/
- https://docs.sentry.io/product/crons/
- https://healthchecks.io/docs/
- https://docs.datadoghq.com/tracing/
- https://grafana.com/docs/loki/latest/
