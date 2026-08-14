# Node.js Docker Kubernetes Probe Budgets: Startup, Readiness, and Liveness Monitoring

Short answer: give startup, readiness, and liveness different budgets and different consequences, then connect each decision to low-cardinality metrics and transition logs. For a small marketplace SaaS, this is the simplest way to monitor notification delivery failures without turning every probe timeout into a page.

The useful unit is not the endpoint. It is the action a signal permits.

Measure first.

I have been paged for missed jobs and duplicate deliveries. In both cases, a green process was weak evidence: the Node.js process was alive, but the work that mattered was not moving safely. That distinction changes how I design health monitoring. A probe should answer one operational question, and its telemetry should let the on-call engineer reconstruct the decision without guessing.

## What should Node.js Docker and Kubernetes probes say about app health?

Use three contracts. A startup probe answers, “Has initialization finished?” A readiness probe answers, “Should this instance receive traffic?” A liveness probe answers, “Is restarting this process an appropriate recovery action?” Kubernetes documents these as separate purposes, and Docker can provide the container-level execution environment; neither platform can infer what a notification service considers safe to receive.

That separation prevents a common failure mode. If readiness shares the liveness rule, a temporary dependency slowdown can trigger restarts across replicas. The restart may erase useful in-flight state, increase duplicate delivery risk, and leave the queue with fewer consumers. If liveness is merely a copy of readiness, the platform may restart a process that is healthy but intentionally draining. Those are different failure domains.

For a notification service, readiness can require that the process has loaded routing configuration and can accept work into its durable handoff. It should not claim that every downstream provider is healthy if the service has a bounded retry path and can safely queue the notification. Liveness should remain narrower: the process can answer its own health check and its event loop is making progress. Startup gets the long cold-start allowance; liveness gets a short, conservative observation window only after startup has completed.

There is no universal timeout or threshold. Measure initialization, dependency recovery, and queue-drain behavior under load, then set the Kubernetes periods and failure thresholds around those observations. A two-second HTTP client timeout in an example is not a production policy.

## How can a simple SaaS example turn probe results into useful metrics and logs?

Start with a small signal vocabulary. Export a readiness gauge with values `1` and `0`. Export a counter for failed observations, labeled by service and probe type. Log a record when a probe changes state, with the probe name, new state, reason, service, instance, and UTC timestamp. Keep request IDs, user IDs, and provider message IDs in structured log fields, not metric labels; those values create unbounded cardinality and make the metric store noisy.

The distinction between a counter and a gauge matters during an incident. A counter tells me that failures accumulated. A gauge tells me whether the instance is usable now. An alert based only on the counter can fire after recovery; an alert based only on the gauge can miss a short outage that caused delivery retries. Pair them with a delivery outcome metric, such as `notification_delivery_total` labeled by bounded outcome categories like `accepted`, `retryable`, and `permanent_failure`.

Here is a deliberately small Go observer. It checks three URLs, emits a state-change log, and keeps the readiness state separate from cumulative failures. In a real Node.js service, the URLs would be the app's own endpoints, while the observer or the app would expose these values to the chosen metrics system.

```go
package main

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"os"
	"time"
)

type probe struct {
	Name string
	URL  string
}

type stateEvent struct {
	Time    string `json:"time"`
	Service string `json:"service"`
	Probe   string `json:"probe"`
	Healthy bool   `json:"healthy"`
	Reason  string `json:"reason"`
}

func check(ctx context.Context, client *http.Client, p probe) (bool, string) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, p.URL, nil)
	if err != nil {
		return false, "request_build_failed"
	}
	resp, err := client.Do(req)
	if err != nil {
		return false, "request_failed"
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return false, "non_success_status"
	}
	return true, "ok"
}

func main() {
	service := os.Getenv("SERVICE_NAME")
	if service == "" {
		log.Fatal("SERVICE_NAME is required")
	}

	probes := []probe{
		{Name: "startup", URL: os.Getenv("STARTUP_URL")},
		{Name: "readiness", URL: os.Getenv("READINESS_URL")},
		{Name: "liveness", URL: os.Getenv("LIVENESS_URL")},
	}
	for _, p := range probes {
		if p.URL == "" {
			log.Fatalf("%s URL is required", p.Name)
		}
	}

	client := &http.Client{Timeout: 2 * time.Second}
	last := map[string]bool{}
	seen := map[string]bool{}
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for range ticker.C {
		for _, p := range probes {
			healthy, reason := check(context.Background(), client, p)
			if seen[p.Name] && last[p.Name] == healthy {
				continue
			}
			seen[p.Name] = true
			last[p.Name] = healthy
			event := stateEvent{
				Time: time.Now().UTC().Format(time.RFC3339), Service: service,
				Probe: p.Name, Healthy: healthy, Reason: reason,
			}
			if err := json.NewEncoder(os.Stdout).Encode(event); err != nil {
				log.Printf("health event encoding failed: %v", err)
			}
		}
	}
}
```

The example intentionally emits transitions rather than one log line every polling interval. That keeps logs useful while the gauge and counter remain the metric-side record. Production code should add the metric exporter and make delivery writes idempotent: retries must carry a stable notification identity so an uncertain acknowledgement cannot become a second send. Probe telemetry cannot repair an unsafe delivery protocol.

One caveat: an external observer can record that the app stopped reporting, but it cannot explain internal readiness. In-process telemetry has the opposite trade-off. It has richer context but disappears with the process. Your mileage may vary; pick the boundary that preserves the evidence your incident response actually needs.

## Which failure modes make health monitoring noisy?

The first is dependency coupling. A provider outage is not automatically a process outage. If every provider timeout makes liveness fail, Kubernetes becomes a restart amplifier. Keep dependency-specific failures in delivery metrics and logs, use readiness only when the service cannot safely accept more work, and reserve liveness for a process-level recovery condition.

The second is probe thundering herd. Many replicas checking the same dependency at the same instant can create load that looks like an outage. Prefer local checks for liveness, cache bounded dependency state for readiness when the contract permits it, and stagger any external checks. The exact schedule belongs in a measured runbook, not a copied YAML snippet.

The third is treating absence as success. A notification worker that is running may still have stopped consuming. Add an expected-event or heartbeat signal for scheduled work, with an owner and an expiry. Container health proves something about a process; it does not prove that a marketplace notification was accepted, retried, or delivered.

Recovery is where the design earns its keep. Suppose a provider starts timing out while three replicas continue answering liveness. Readiness can remain true if the service still has bounded durable buffering, while `notification_delivery_total{outcome="retryable"}` rises and a transition log records the provider state. If the buffer reaches its limit, readiness changes to false and the instance leaves traffic without being restarted. Once the provider recovers, the same state-change rule records recovery, the queue drains using stable notification identities, and the runbook can distinguish delayed delivery from duplicate delivery. A blanket liveness failure would have thrown away that sequence and produced a much less trustworthy story.

Finally, protect the evidence. Keep logs structured, redact notification content and personal data, and define retention and deletion behavior. GDPR Article 17 makes erasure a product and operations concern, not a dashboard afterthought. A log that contains a user's message or address needs a deletion strategy that a metric label cannot provide.

## How should a team choose a monitoring architecture for signal quality?

Choose the smallest architecture that answers the failure question, then test it by replaying failures in staging. A self-managed metrics stack gives control over labels, retention, and query cost, but the team owns collection and storage. A hosted stack removes some operational work, but it adds dependency and data-governance decisions. An outside-in uptime check sees user-visible reachability, while an internal probe sees process state. Scheduled-work heartbeats cover silence that neither sees.

The right comparison axis is evidence, not feature count. Ask who can detect the failure, what action follows, how the event is correlated, and how the record is removed when retention ends. Feature toggles add another useful discipline: separate evaluation from activation, record the reason for a change, and keep rollback explicit. The same habit applies to health signals. A probe should not silently change traffic or restart behavior without a documented contract.

This approach is unsuitable when the organization needs full distributed tracing, session replay, crash symbolization, or a managed paging and escalation service as a hard requirement. In that case, choose an architecture that supplies those capabilities and accept its cost and data controls. For a small SaaS whose immediate problem is distinguishing missed notifications from unhealthy containers, a narrow probe contract plus durable metrics, transition logs, and an external heartbeat is usually enough.

The runbook then stays short: identify the platform action, inspect the current readiness gauge, check failure and delivery counters for the same window, read the last state transition, and verify the expected-work heartbeat. If the process restarted, check duplicate-suppression keys and queue acknowledgements before replaying anything.

Small setup. Sharp boundary.

## Further reading

- https://kubernetes.io/docs/concepts/configuration/liveness-readiness-startup-probes/
- https://prometheus.io/docs/concepts/metric_types/
- https://martinfowler.com/articles/feature-toggles.html
- https://gdpr-info.eu/art-17-gdpr/
