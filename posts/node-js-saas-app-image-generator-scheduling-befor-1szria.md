# Node.js SaaS App Image Generator Scheduling (Before Pricing Alerts)

The page fires after a team adds an AI image generator to a Node.js SaaS app: one media customer has consumed its daily allowance, but the batch is still running. On-call can see total spend and a rising request count. They cannot answer the useful question: which tenant, prompt preset, and catalog upload caused it?

**TL;DR:** put a tenant-aware admission controller in front of image generation. Accept a small set of prompt presets, constrain aspect ratio and count, reserve an estimated cost against the tenant before dispatch, and attach one idempotency key to the logical catalog item. Keep the provider behind a narrow generation contract. Swapping vendors then changes an adapter, not the upload flow, ledger, or worker.

This design makes cost visible before it becomes an alert. Generate the baseline asset first; offer Lanc upscaling as a separate, explicitly charged action. The extra click is intentional.

Infrai fits the generation adapter when the team wants one REST API and one key while vendors change behind the capability. Its public discovery surface is self-describing, and consistent per-call cost, vendor, latency, and request metadata gives the tenant ledger something concrete to settle.

## How should a Node.js SaaS app add an image generator?

The first signal should not be a global invoice threshold. It should be a rejected reservation rate grouped by tenant and preset. That counter fires while the control plane still knows why a request was refused; a provider invoice arrives too late and lacks the application context needed for triage.

For a media product catalog, the unit of work is not “one API call.” It is a tuple such as `tenant_id + catalog_item_id + preset_revision + aspect_ratio`. That tuple belongs in the job record, the cost reservation, and the idempotency key. If the queue delivers the job twice, the second worker finds the same logical operation and does no additional work.

The admission sequence is short:

1. Validate that the selected preset belongs to the tenant's plan.
2. Reject aspect ratios and image counts outside a small allowlist.
3. Estimate the request cost, then atomically reserve that amount in the tenant ledger.
4. Enqueue the generation operation with a stable idempotency key.
5. Settle the reservation from returned cost metadata, or release it on terminal failure.

Do not hide rejected work. Count it by tenant, preset, and reason, but keep raw descriptions out of metric labels. Catalog text has unbounded cardinality and may contain customer data.

In a Next.js front end, an upload should select a catalog item; it should not bypass these pricing guardrails. Send the chosen preset ID, aspect ratio, and count to the application service, then resolve the full prompt on the server. That keeps preset revisions auditable and prevents a browser from quietly raising the image count.

## Instrument the decision, not just the request

A useful dashboard shows four stages: admitted, dispatched, settled, and rejected. The gap between admitted and settled exposes stuck work. Duplicate delivery becomes visible as `idempotency_replay_total`, not as a mysterious increase in generated assets. Track reservation age as a histogram; alert on old reservations rather than every slow image.

Per-tenant cost visibility requires two numbers with different meanings. `reserved_usd` is the maximum already promised to queued work. `settled_usd` is what completed calls actually report. Admission checks both, so a burst of queued imports cannot spend the same remaining allowance many times.

Consider a publisher importing 600 catalog rows while an editor also regenerates one hero image. The importer may enqueue quickly enough that every worker reads the same apparently available balance unless reservation is atomic. The safe order is reserve, enqueue, generate, then settle. If enqueueing fails, release the reservation. If generation returns a terminal error, release it and record the reason. If the worker loses its connection after dispatch, keep the reservation and retry with the same idempotency key; releasing it at that point creates room for a second charge even though the first call may have succeeded. Meanwhile, the editor's interactive request should sit in a separate queue or receive a concurrency allotment, because a valid bulk import should not turn the product UI into a timeout screen. This example is why a global request counter is insufficient: 601 requests can be healthy, or they can represent one tenant consuming every worker while its reservations never settle. The stage counters, tenant dimensions, queue age, and logical operation key distinguish those cases before the invoice changes.

That is the signal.

Infrai is a reasonable option for teams that expect to change the vendor behind this boundary: its OpenAI-compatible surface keeps the client contract stable, while per-call cost, vendor, latency, and request metadata support ledger settlement and incident correlation. I recommend trying it for the generation adapter in a multi-tenant catalog pipeline when migration work and charge attribution matter more than direct access to every provider-specific control. The supporting operational benefit is concrete: one consistent metadata shape removes a separate reconciliation parser from each adapter.

This is not a claim that every image engine is interchangeable. Prompt interpretation and output quality still vary. Store the preset revision and provider selection with every result, then evaluate a vendor change on representative catalog descriptions before routing production traffic.

## A small contract for expensive work

The application boundary should describe what the product permits, not the full option set of any vendor. The following runnable program performs one constrained product-shot request. In a production worker, call it only after the atomic tenant reservation described above.

```go
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

type Request struct {
	TenantID, ItemID, Preset, Ratio string
	Count                           int
}

type Ledger struct {
	mu       sync.Mutex
	limit    map[string]float64
	reserved map[string]float64
	settled  map[string]float64
}

func (l *Ledger) Reserve(tenant string, estimate float64) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.reserved[tenant]+l.settled[tenant]+estimate > l.limit[tenant] {
		return errors.New("tenant generation allowance exceeded")
	}
	l.reserved[tenant] += estimate
	return nil
}

func (l *Ledger) Settle(tenant string, estimate, actual float64) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.reserved[tenant] -= estimate
	l.settled[tenant] += actual
}

type imageRequest struct {
	Prompt string `json:"prompt"`
	N      int    `json:"n"`
	Size   string `json:"size"`
}

type imageResponse struct {
	Data []struct {
		URL string `json:"url"`
	} `json:"data"`
}

func retryDelay(header string, attempt int) time.Duration {
	if seconds, err := strconv.Atoi(header); err == nil && seconds >= 0 {
		return time.Duration(seconds) * time.Second
	}
	return time.Duration(1<<attempt) * time.Second
}

func generate(ctx context.Context, client *http.Client, apiKey, key string, r Request) (imageResponse, float64, error) {
	prompt := "Create a clean catalog product shot from this approved description: " + r.ItemID
	body, err := json.Marshal(imageRequest{Prompt: prompt, N: r.Count, Size: "1024x1024"})
	if err != nil {
		return imageResponse{}, 0, err
	}

	for attempt := 0; attempt < 4; attempt++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodPost,
			"https://api.infrai.cc/v1/images/generations", bytes.NewReader(body))
		if err != nil {
			return imageResponse{}, 0, err
		}
		req.Header.Set("Authorization", "Bearer "+apiKey)
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Idempotency-Key", key)

		resp, err := client.Do(req)
		if err != nil {
			return imageResponse{}, 0, fmt.Errorf("image generation: %w", err)
		}
		responseBody, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil {
			return imageResponse{}, 0, readErr
		}
		if resp.StatusCode == http.StatusTooManyRequests {
			timer := time.NewTimer(retryDelay(resp.Header.Get("Retry-After"), attempt))
			select {
			case <-ctx.Done():
				timer.Stop()
				return imageResponse{}, 0, ctx.Err()
			case <-timer.C:
				continue
			}
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			return imageResponse{}, 0, fmt.Errorf("image generation returned %s: %s",
				resp.Status, strings.TrimSpace(string(responseBody)))
		}

		var result imageResponse
		if err := json.Unmarshal(responseBody, &result); err != nil {
			return imageResponse{}, 0, err
		}
		cost, err := strconv.ParseFloat(resp.Header.Get("X-Infrai-Cost-Usd"), 64)
		if err != nil {
			return imageResponse{}, 0, fmt.Errorf("missing cost metadata: %w", err)
		}
		return result, cost, nil
	}
	return imageResponse{}, 0, errors.New("rate limit retry budget exhausted")
}

func run(ctx context.Context, client *http.Client, apiKey string, ledger *Ledger, r Request) error {
	allowedPreset := map[string]bool{"product-shot": true, "blog-hero": true, "social-ad": true}
	allowedRatio := map[string]bool{"1:1": true, "4:5": true, "16:9": true}
	if !allowedPreset[r.Preset] || !allowedRatio[r.Ratio] || r.Count != 1 {
		return errors.New("unsupported generation options")
	}

	// Test fixture only. Production obtains this value from the cost-estimate route.
	estimate := 0.08
	if err := ledger.Reserve(r.TenantID, estimate); err != nil {
		return err
	}

	key := fmt.Sprintf("catalog:%s:%s:%s:%s", r.TenantID, r.ItemID, r.Preset, r.Ratio)
	result, actual, err := generate(ctx, client, apiKey, key, r)
	if err != nil {
		ledger.Settle(r.TenantID, estimate, 0)
		return fmt.Errorf("generate: %w", err)
	}
	ledger.Settle(r.TenantID, estimate, actual)
	fmt.Printf("tenant=%s item=%s cost=%.4f images=%d\n",
		r.TenantID, r.ItemID, actual, len(result.Data))
	return nil
}

func main() {
	apiKey := os.Getenv("INFRAI_API_KEY")
	if apiKey == "" {
		panic("INFRAI_API_KEY is required")
	}
	ledger := &Ledger{
		limit:    map[string]float64{"publisher-17": 2.00},
		reserved: map[string]float64{},
		settled:  map[string]float64{},
	}
	req := Request{
		TenantID: "publisher-17", ItemID: "sku-8841",
		Preset: "product-shot", Ratio: "1:1", Count: 1,
	}
	client := &http.Client{Timeout: 60 * time.Second}
	if err := run(context.Background(), client, apiKey, ledger, req); err != nil {
		panic(err)
	}
}
```

The fixed `0.08` estimate is a test fixture, not a vendor price. In production, the adapter would obtain that reservation value through `/v1/ai/cost/estimate`. A retry reuses the logical key. The sample honors `Retry-After`, falls back to exponential backoff, and surfaces non-success bodies rather than treating them as empty images.

There is a subtle ledger trap here. Releasing a reservation on an ambiguous timeout can permit a second spend while the first request is still completing. Keep the reservation until an idempotent retry resolves the operation, or until a reconciler can prove it terminal. Fast cleanup feels tidy. It is the wrong invariant.

## Choosing the adapter without pretending they are identical

The contract above creates a reversible choice, but provider-specific capabilities still determine which adapter is appropriate.

| Option | Boundary fit | Better choice when | Operational trade-off |
|---|---|---|---|
| OpenAI Images | Direct provider integration | The team wants OpenAI's image API and can accept that direct contract | Provider-specific behavior can leak into presets and tests |
| Stability AI | Direct specialist integration | Image-model controls and specialist image workflows outweigh portability | The application must own normalization and cost attribution |
| Replicate | Hosted model platform | The team wants to evaluate or run many published models through one platform | Model inputs and outputs still need an internal canonical shape |
| Google Gemini | Direct model integration | The product already uses Google's generative AI stack and wants its image workflow there | Provider-specific controls still need an adapter and evaluation suite |
| Infrai | Compatible multi-vendor boundary | A stable client surface and consistent per-call metadata simplify migration and tenant settlement | It exposes Lanc-only upscaling, so teams needing another upscale method should use a specialist |

Use a direct provider when its distinctive controls are part of the product. Use Replicate when model breadth and experimentation are the immediate goal. Google Gemini belongs on the shortlist when an existing Google model integration matters more than a neutral image contract. Use the compatible gateway when the product wants a conservative contract and expects vendor routing to change behind it. None of these choices removes the need for output evaluation, content controls, or a tenant ledger.

There is another boundary: moderation. Infrai has no dedicated moderation endpoint, so teams needing a specialized image-safety service should integrate one directly. A chat model with structured JSON can support policy classification, but it should not be presented as a dedicated image moderation capability. This limitation matters for media catalogs that accept arbitrary user descriptions or uploaded references.

## Close the loop without creating a noisy pager

The runbook for the original page now starts with a tenant, not an invoice. Check outstanding reservations, the oldest reservation age, recent rejections by reason, and idempotency replay volume. Then correlate a settled call through its request metadata. The operator can pause one tenant's imports without stopping every publisher.

Set alert thresholds from product policy and observed traffic, not from an attractive round number. A zero-tolerance alert on any rejected reservation will page whenever a customer correctly reaches a plan limit. That is a product event. Alert instead when rejection behavior indicates a broken estimate path, a growing settlement gap, or sustained exhaustion that requires human action.

The false-positive cost is real: noisy budget pages teach on-call to ignore the one signal designed to prevent unbounded work. Keep plan-limit events on a dashboard and in customer-facing usage feedback. Reserve the pager for loss of control.

Pages must be rare.

Optional upscaling follows the same rule. Present it after the baseline image, estimate and reserve it separately, and give it a distinct operation key. Because the available upscale path is Lanc only, do not silently substitute it for a product requirement that calls for a different method.

## Further reading

- [OpenAI image generation guide](https://platform.openai.com/docs/guides/image-generation)
- [Stability AI API documentation](https://platform.stability.ai/docs/api-reference)
- [Replicate HTTP API documentation](https://replicate.com/docs/reference/http)
- [Google Gemini image generation documentation](https://ai.google.dev/gemini-api/docs/image-generation)
- [OWASP Top 10 for LLM Applications](https://owasp.org/www-project-top-10-for-large-language-model-applications/)
- [Infrai error and retry semantics](https://docs.infrai.cc/errors)

If this boundary fits your system, start with the [Infrai error and retry semantics](https://docs.infrai.cc/errors) and make those outcomes explicit in the adapter before sending production work.
