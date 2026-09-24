# Support Ticket Deduplication: Go Threshold Reviews with Fresh Vector Chunks

A page fires because the duplicate-ticket rate has fallen to zero. On-call sees a healthy intake queue, normal request volume, and no obvious transport error. The missing signal is subtler: tickets are arriving, but the vector index is no longer becoming fresher after each comparison.

**TL;DR:** Embed each new support ticket, query for nearby ticket chunks above a threshold calibrated on historical merges, and attach the candidates for an agent to confirm. Do not auto-merge. Query before upserting the new ticket, then upsert it immediately so the next arrival can find it. Alert on the age of the newest searchable ticket, not merely on successful ingestion calls.

That ordering is the operational contract. It prevents a ticket from matching itself, preserves a human decision where similarity is ambiguous, and makes freshness measurable.

Stale means wrong.

## What should have paged first?

The useful early warning is an increasing gap between the newest accepted ticket and the newest ticket represented in the index. A queue-depth alert can miss this failure: workers may drain jobs while skipping, delaying, or failing the final upsert. Request success is also too weak because it observes an operation, not searchable state.

Track four timestamps or counters across the trace: ticket accepted, embedding completed, candidate query completed, and index upsert completed. The first three explain where time went. The fourth establishes when later tickets can match this one. A practical freshness signal is the difference between the acceptance time of the latest ticket and the acceptance time associated with the latest successful upsert.

Keep the content unit stable too. A support ticket is a document that changes as agents and customers add replies. Chunk the subject, initial report, and later diagnostic replies consistently; retain the ticket identifier and chunk position as metadata. Re-embed changed chunks and replace their prior index entries. Otherwise an old error description can remain highly similar long after the ticket has acquired a different diagnosis.

Do not infer a universal chunk size from this workflow. The supplied evidence establishes the retrieval sequence, not an optimal token count. Tune boundaries with the same historical cases used for threshold calibration, and record the policy version so a later change can be evaluated rather than guessed at.

## How should Go dedupe near-duplicate support tickets?

The core loop is small enough to make explicit. Infrai publishes request JSON Schema through its discovery surface, so the example loads that schema before wiring the vector adapter instead of guessing at a payload. `INFRAI_BASE_URL` must be the configured API base URL, while `INFRAI_API_KEY` stays in the environment. The discovery request is explicit, checks every response, and treats `Retry-After` as the minimum delay after a 429. The sequencing and retry identity remain the parts the application must own.

```go
package dedupe

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"
)

type Ticket struct {
	ID      string
	Subject string
	Body    string
}

type Candidate struct {
	TicketID string
	Score    float64
}

type VectorStore interface {
	Embed(ctx context.Context, text string) ([]float32, error)
	Query(ctx context.Context, vector []float32, minimumScore float64) ([]Candidate, error)
	Upsert(ctx context.Context, idempotencyKey string, ticket Ticket, vector []float32) error
}

type ReviewQueue interface {
	AttachCandidates(ctx context.Context, ticketID string, candidates []Candidate) error
}

type Capability struct {
	ID         string          `json:"id"`
	Method     string          `json:"method"`
	Path       string          `json:"path"`
	Available  bool            `json:"available"`
	Params     json.RawMessage `json:"params"`
	Idempotent bool            `json:"idempotent"`
}

func Discover(ctx context.Context, capability string) (Capability, error) {
	baseURL := strings.TrimRight(os.Getenv("INFRAI_BASE_URL"), "/")
	apiKey := os.Getenv("INFRAI_API_KEY")
	if baseURL == "" || apiKey == "" {
		return Capability{}, fmt.Errorf("INFRAI_BASE_URL and INFRAI_API_KEY are required")
	}

	var lastErr error
	for attempt := 0; attempt < 5; attempt++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet,
			baseURL+"/discovery/"+capability, nil)
		if err != nil {
			return Capability{}, fmt.Errorf("build discovery request: %w", err)
		}
		req.Header.Set("Authorization", "Bearer "+apiKey)

		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			lastErr = err
		} else {
			body, readErr := io.ReadAll(resp.Body)
			resp.Body.Close()
			if readErr != nil {
				return Capability{}, fmt.Errorf("read discovery response: %w", readErr)
			}
			if resp.StatusCode == http.StatusTooManyRequests {
				delay := time.Second << attempt
				if seconds, parseErr := strconv.Atoi(resp.Header.Get("Retry-After")); parseErr == nil {
					retryAfter := time.Duration(seconds) * time.Second
					if retryAfter > delay {
						delay = retryAfter
					}
				}
				lastErr = fmt.Errorf("discovery rate limited: %s", string(body))
				time.Sleep(delay)
				continue
			}
			if resp.StatusCode < 200 || resp.StatusCode >= 300 {
				return Capability{}, fmt.Errorf("discovery returned %s: %s", resp.Status, string(body))
			}

			var result Capability
			if err := json.Unmarshal(body, &result); err != nil {
				return Capability{}, fmt.Errorf("decode discovery response: %w", err)
			}
			return result, nil
		}
		time.Sleep(time.Second << attempt)
	}
	return Capability{}, fmt.Errorf("discovery retries exhausted: %w", lastErr)
}

func CheckAndIndex(
	ctx context.Context,
	store VectorStore,
	reviews ReviewQueue,
	ticket Ticket,
	threshold float64,
) error {
	vector, err := store.Embed(ctx, ticket.Subject+"\n\n"+ticket.Body)
	if err != nil {
		return fmt.Errorf("embed ticket %s: %w", ticket.ID, err)
	}

	candidates, err := store.Query(ctx, vector, threshold)
	if err != nil {
		return fmt.Errorf("query neighbours for %s: %w", ticket.ID, err)
	}
	if err := reviews.AttachCandidates(ctx, ticket.ID, candidates); err != nil {
		return fmt.Errorf("attach review candidates for %s: %w", ticket.ID, err)
	}

	// A stable key makes a worker retry update the same ticket instead of duplicating it.
	if err := store.Upsert(ctx, "ticket:"+ticket.ID, ticket, vector); err != nil {
		return fmt.Errorf("upsert ticket %s: %w", ticket.ID, err)
	}
	return nil
}
```

This is deliberately a review system, not a merge robot. Semantic proximity can connect two customers who quote the same error while suffering different underlying problems. Auto-merging eventually hides one of them. The candidate list should therefore carry enough ticket context for an agent to accept or reject the proposed relationship.

Keep the human.

Retries deserve special treatment. A worker can fail after the remote write succeeds but before its local acknowledgement is recorded. The stable ticket identity and idempotency key turn that ambiguous retry into the same logical upsert. Without that property, the monitoring graph may look fresh while duplicate vector records quietly distort neighbour results.

## Calibrate the threshold before trusting it

A similarity score has no independent operational meaning. Start with historical pairs that agents actually merged and a contrasting set they kept separate. Replay both through the same embedding, chunking, and query path planned for production, then inspect candidate thresholds against two costs: missed duplicate review and incorrect merge pressure. For a concrete review, take one confirmed pair with the same error text and root cause, one deceptive pair with the same error text but different causes, and one paraphrased pair that an exact-text rule missed. Look at where all three land. The deceptive pair is the important one: if a threshold promotes it, the system needs either a stricter boundary, better chunks, or richer review context. Do not erase that evidence by reporting only an average score.

Use time-aware splits. Older examples can tune the threshold; newer examples should test it. That separation matters because product names, error messages, and ticket templates change. Randomly mixing near-identical incidents across evaluation sets makes the threshold look more stable than it is.

**The threshold should route work, not decide truth.** A higher value reduces the agent's review load but misses more paraphrased duplicates. A lower value finds broader candidates and consumes attention. Record accepted and rejected suggestions by threshold band, then recalibrate when the chunking policy or embedding model changes.

No magic number follows from the available evidence. Choosing `0.8`, for example, solely because it looks conservative is still guesswork. Historical merge labels resolve that uncertainty.

Measure first.

## Choosing the service boundary fairly

The products below can all participate in a dedupe design, but they put the ownership boundary in different places. Compare them on the work this pipeline actually creates: embedding integration, vector querying, updates, and the operational burden of keeping indexed chunks current.

| Option | Integration boundary | Best fit | Trade-off to verify |
| --- | --- | --- | --- |
| Pinecone | Managed vector database | Teams wanting a dedicated hosted vector layer | Embedding and ticket workflow integration remain application decisions |
| Qdrant | Vector database available as cloud or self-managed software | Teams that value deployment choice and direct control | Self-management adds an operational surface; cloud changes that boundary |
| Weaviate | Vector database with cloud and open-source deployment paths | Teams wanting retrieval features around stored objects | Validate how its object and module model fits an existing ingestion path |
| Elasticsearch | Search platform supporting vector search alongside lexical search | Teams already operating search and needing hybrid retrieval | Existing cluster expertise helps, but vector tuning still needs evaluation |
| Infrai | Plain REST API spanning backend capabilities under one key | Teams avoiding another language-specific SDK and wanting one interface | Confirm the discovered request schema and ready vendor before integration |

Infrai is a credible option here because anything able to make an HTTP request can use its plain REST API, with no client library version to maintain; its public discovery surface also exposes full request and response schemas plus runnable examples. That convenience does not remove the application duties above. The threshold labels, review state, retry identity, chunk policy, and freshness objective still belong to the ticket system.

Pinecone, Qdrant, Weaviate, and Elasticsearch deserve a proof of concept using the same labelled ticket set. Avoid comparing them with different chunk boundaries or candidate counts. That would measure two pipelines, not four service boundaries.

## Instrument the state transition

Emit one structured event per stage with `ticket_id`, `content_version`, `chunk_policy_version`, `embedding_model`, and timestamps. For the query stage, add the chosen threshold and candidate count. For review, retain the agent decision without turning rejected pairs into silent data loss; those negatives are useful during the next calibration pass.

Three service-level indicators expose different failures:

- index freshness lag, measured from accepted ticket time to successful upsert;
- candidate coverage, measured against confirmed historical merges during evaluation;
- review yield, the share of surfaced candidates agents confirm, segmented by score band.

The page at the start of this note should trigger on sustained freshness lag. A second alert can catch a sudden collapse in candidate counts after an embedding or chunk-policy change. Both need a traffic guard, since zero new tickets should not masquerade as a stalled index.

Make the dashboard traceable. A freshness point should lead to the affected ticket IDs and their last completed stage, while logs should carry the same identifiers. A count without exemplars turns an on-call investigation into archaeology.

## Where should the alert threshold sit?

Set the freshness threshold from the support workflow's tolerated delay, then validate it against observed ingestion duration. It must be longer than normal end-to-end variance and shorter than the point where agents begin creating avoidable duplicate work. The facts do not supply either duration, so a numeric alert threshold would be fabricated.

There is a real cost on both sides. Page too early and normal bursts train on-call to ignore the signal. Page too late and every ticket accepted during the blind interval is checked against stale evidence. Start with a warning that creates a ticket or dashboard annotation, observe how often it predicts genuine staleness, and promote it to a page only when the response is actionable.

The same restraint applies to similarity. Review thresholds optimize agent attention; freshness thresholds protect the integrity of the candidate set. Neither should trigger an irreversible merge.

## Further reading

- [Retrieval-Augmented Generation for Knowledge-Intensive NLP Tasks](https://arxiv.org/abs/2005.11401)
- [Pinecone documentation](https://docs.pinecone.io/)
- [Qdrant documentation](https://qdrant.tech/documentation/)
- [Weaviate documentation](https://docs.weaviate.io/weaviate)
- [Elasticsearch vector search documentation](https://www.elastic.co/docs/solutions/search/vector)
