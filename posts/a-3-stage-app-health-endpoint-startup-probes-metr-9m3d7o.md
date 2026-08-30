# A 3-Stage App Health Endpoint — Startup Probes, Metrics, and Safe Rollback

Short answer: for a Node.js app in Docker or Kubernetes, give startup, readiness, and liveness probes separate meanings, keep the health endpoint cheap, and decide pricing-rule rollback from business metrics rather than from process health alone.

That separation matters during a B2B SaaS pricing change behind a flag. A process can be alive and ready to accept traffic while applying the wrong rule. Conversely, a slow dependency can make one request path unavailable without making a restart useful. Treating both conditions as one Boolean turns a reversible release into an ambiguous incident.

I've been paged by missed jobs and duplicate deliveries. The invariant those incidents reinforce is plain: a health check should report whether an automated action is safe, not whether the application feels generally healthy. Restart, traffic removal, and release rollback are three different actions.

## How should a beginner wire Docker Kubernetes probes to a Node.js health endpoint?

Start by defining the action behind each signal. A startup probe answers, "Has this container completed initialization?" Until it succeeds, Kubernetes does not run the liveness and readiness probes. A readiness probe answers, "Should this pod receive traffic now?" A failed readiness probe removes the pod from matching service endpoints. A liveness probe answers, "Is restarting this container a reasonable recovery action?" A failed liveness probe can cause a restart. Those are Kubernetes semantics, not three spellings for `/health`.

Use three paths even if they share an internal snapshot: `/health/startup`, `/health/ready`, and `/health/live`. In a Node.js app, register them on the same server as the application but keep their handlers independent of expensive request middleware. Return a success status only when the condition for that path holds; return `503 Service Unavailable` when it does not. Do not put secrets, customer identifiers, flag payloads, or a dependency-by-dependency diagnostic dump in the response body.

The liveness path should usually test local progress. The event loop must still be servicing requests, and any internal worker whose progress is required for this process role must not be irrecoverably stuck. It should not synchronously call the database, flag service, billing provider, or telemetry backend. If a shared dependency slows down, restarting every replica adds churn without repairing that dependency. Readiness can be stricter because its failure stops new traffic. It may depend on a locally cached configuration being present and valid, the pricing-rule version being understood, and required connection pools having completed initialization. Even here, prefer bounded local state updated by background checks over a fan-out of live network calls on every probe. Probe traffic is control-plane traffic; it should remain predictable during the exact overload conditions in which operators need it most.

Don't conflate them.

Keep startup separate. A Node.js service that loads a large ruleset, warms a cache, or runs initialization work may need more startup time than its steady-state liveness budget. Configure the startup probe with enough attempts and delay to cover a defensible cold-start envelope. Once startup succeeds, a later loss of readiness must not send the process back into startup mode.

Three words help: action before symptom.

## Make rollback a business decision, not a restart loop

For the pricing rollout, assume the old rule is `price-v17`, the candidate is `price-v18`, and the flag assigns a small cohort to the candidate. Put the selected rule version on every pricing decision log and metric as a bounded attribute. Then watch outcomes that can justify rollback: calculation failures, rejected quotes, checkout abandonment, and disagreement between a shadow calculation and the served result. The exact thresholds belong in the release plan before exposure increases.

Do not make readiness fail just because `price-v18` produces an unacceptable business result. The process is still able to serve `price-v17`, so traffic removal wastes capacity and does not disable the bad flag. The corrective action is to roll back the flag or rule version. Readiness should fail only if that replica cannot safely serve any permitted rule, such as when its local rule snapshot is absent or has a schema version the binary cannot interpret.

This is the subtle failure mode. Imagine ten replicas receiving a candidate rule. An outcome alert fires, the release controller disables the candidate, and nine replicas observe the old assignment promptly. The tenth has a stale local snapshot. If `/health/ready` exposes only database connectivity, that replica keeps accepting requests under an assignment operators believe is gone. If liveness depends on the remote flag service, a transient control-plane delay may instead restart all ten replicas. Neither response matches the desired action. Readiness should reflect whether the locally active snapshot is approved and usable; a separate rollout metric should show how many replicas have acknowledged the rollback version. The incident ends when the decision path converges, not merely when pods look green.

Log each rule transition once as a state change rather than once per probe. A useful record has an event name, service, environment, pod or instance identity, previous rule version, new rule version, and a correlation identifier for the rollout. Do not use customer ID as a metric label. Logs can carry high-cardinality investigation context under the organization's retention and access rules; metrics should keep dimensions bounded so a new tenant or request does not create a new time series.

The primary rollout chart should put candidate exposure beside the business error ratio and rule-version acknowledgements. Process restart count, readiness state, request latency, and error rate remain supporting signals. This makes the operator's decision legible: disable the candidate for a semantic regression, drain a replica that cannot serve an approved snapshot, and restart only a process that has lost local progress.

## How can a probe contract keep health handlers boring?

The following Go example is a compact contract test for the state transitions. The production service may be Node.js, but the important artifact is the language-neutral decision table: startup requires initialization, readiness requires an approved local rule, and liveness requires local progress. Keeping this logic outside the HTTP handler makes the three endpoints hard to accidentally collapse later.

```go
package health

import "testing"

type Snapshot struct {
	Initialized      bool
	LocalProgressing bool
	RuleLoaded       bool
	RuleApproved     bool
}

func startup(s Snapshot) int {
	if !s.Initialized {
		return 503
	}
	return 204
}

func readiness(s Snapshot) int {
	if !s.Initialized || !s.RuleLoaded || !s.RuleApproved {
		return 503
	}
	return 204
}

func liveness(s Snapshot) int {
	if !s.LocalProgressing {
		return 503
	}
	return 204
}

func TestProbeContract(t *testing.T) {
	cases := []struct {
		name              string
		state             Snapshot
		startup, ready, live int
	}{
		{"booting", Snapshot{LocalProgressing: true}, 503, 503, 204},
		{"serving approved rule", Snapshot{true, true, true, true}, 204, 204, 204},
		{"rollback not observed", Snapshot{true, true, true, false}, 204, 503, 204},
		{"local progress stopped", Snapshot{true, false, true, true}, 204, 204, 503},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := startup(tc.state); got != tc.startup {
				t.Fatalf("startup: got %d, want %d", got, tc.startup)
			}
			if got := readiness(tc.state); got != tc.ready {
				t.Fatalf("readiness: got %d, want %d", got, tc.ready)
			}
			if got := liveness(tc.state); got != tc.live {
				t.Fatalf("liveness: got %d, want %d", got, tc.live)
			}
		})
	}
}
```

In the Node.js handler, turn the corresponding result into an empty `204` response or a small `503` response. Keep the snapshot in memory, update it from bounded background work, and make the update atomic from the handler's point of view. Don't log every successful probe. Access logs for high-frequency probe paths can drown the transition that matters and add avoidable storage volume; either suppress routine successes or sample them, while retaining state changes and failures.

Test the contract from outside the process too. A container image test should start the image, wait through the documented cold-start budget, and call all three paths. A cluster test should verify that readiness failure stops new service traffic without restarting the container, while liveness failure follows the configured restart policy. For the pricing flag, run a rollback drill that changes the approved rule version and confirms every replica reports the new acknowledgement metric within the rollout's stated time bound.

Fast is good. Deterministic is better.

## Join logs, metrics, and traces without hiding failures

Health endpoints tell an orchestrator what to do; observability tells a person why. For each probe transition, emit one structured log with the previous and next state plus a reason code such as `rule_not_approved` or `local_progress_stopped`. Expose counters for transitions and gauges for current readiness and acknowledged rule version. Alert on sustained user impact or incomplete rollback convergence, not on a single failed probe that the platform already handled.

Traces help connect a quote request to its pricing decision, but sampling changes what evidence remains. Head sampling decides before the full trace is known, while tail sampling can decide after more of the trace is available. That makes tail sampling relevant when rare pricing errors must be retained, but it requires the sampling system to wait for and assemble trace data. I'm not sure which policy fits a given traffic profile without the error frequency, trace volume, and investigation objective. Resolve that uncertainty with a load test and a written retention target, then verify that known error traces survive the chosen policy.

Use correlation carefully. A request identifier can join a pricing decision log to a trace. The rule version and flag cohort can be trace attributes and bounded metric dimensions if their possible values are controlled. Raw customer identity, quote contents, and contract terms should not become metric labels. That boundary protects both cardinality and sensitive business data.

There is a cost trade-off.

Full success logs and unsampled traces simplify some investigations, but volume rises with request traffic and probe frequency. Sampling routine success telemetry is reasonable once transition logs, error counters, and rollback acknowledgements remain complete. Never sample the only signal that proves all replicas accepted the rollback.

## Know when this design is the wrong fit

The catch is that three probe paths and a rule-acknowledgement signal add state and test surface. A short-lived batch container that runs once and exits usually needs job completion semantics, not service readiness. A single-process development tool with no traffic router may gain little from separate readiness and liveness endpoints. Stick with a simple process check there, and add the full contract when an orchestrator can act differently on each result.

Do not use this pattern to disguise an unsafe pricing migration. If old and new rules cannot run concurrently against compatible data, the flag alone does not provide rollback safety. Pause and design a forward-and-backward-compatible migration, or use an explicit maintenance window with a rehearsed recovery path. Probes cannot turn an irreversible data change into a reversible release.

Probe timing also varies by workload. A CPU-bound Node.js process, a service with long initialization, and a mostly idle API should not inherit the same timeout and failure threshold by convention. Measure cold starts and normal event-loop delay under representative load, choose budgets with room for expected variance, and test the resulting recovery time. Your mileage may vary.

The final decision rule is narrow: roll back the pricing flag when business outcomes or rule correctness breach the predeclared limit; remove a replica from traffic when it cannot safely serve an approved configuration; restart it only when local progress has stopped. That keeps the automated remedy aligned with the failure.

## References

- https://kubernetes.io/docs/concepts/configuration/liveness-readiness-startup-probes/
- https://kubernetes.io/docs/tasks/configure-pod-container/configure-liveness-readiness-startup-probes/
- https://nodejs.org/api/http.html
- https://opentelemetry.io/docs/concepts/sampling/
- https://www.rfc-editor.org/rfc/rfc9110.html
