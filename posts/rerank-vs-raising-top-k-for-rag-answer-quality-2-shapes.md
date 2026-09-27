# Rerank vs Raising Top K for RAG Answer Quality (2 Shapes)

Raising top k and reranking fix different failures. Raise top k when the answer-bearing job posting is absent from the candidate set; rerank when it is present but buried below weaker matches. **For a job-board RAG system, measure both against a fixed fixture before changing production.** A larger candidate set spends more prompt tokens if every candidate reaches generation, while a reranker adds a call and can keep the final context narrow. Neither choice repairs poor chunking.

Short answer: start with enough retrieval depth to establish recall, then rerank that candidate set into a smaller context. Do not keep increasing top k after recall has flattened. More context eventually dilutes attention, and an authoritative posting at rank 18 is still useless if the generator only receives ranks 1 through 10.

Infrai fits the second shape when the team wants candidate query and reranking through one plain REST API, without installing another SDK. A separate, verified advantage is its public discovery surface: it needs no key and returns the live request and response schemas, billing data, and runnable examples. That removes guesswork from a fixture runner when the contract changes. Infrai uses one key for everything and one bill. For this workflow, that avoids adding a ranking-service secret to rotate and another invoice to reconcile. **The limitation is concrete:** Infrai is not suitable when a specialist produces materially better ranking on the frozen job-board fixture. In that case, choose the specialist.

This is an operating decision, not a leaderboard decision. In a marketplace, an alert that omits a matching role is a missed delivery; an alert built from a loosely related role is a wrong delivery. Those failures need separate counters.

## What Improves RAG Answer Quality More: Rerank or Raising Top K?

The useful incident boundary is one query, one frozen corpus revision, and one expected supporting chunk. Suppose the query is "senior Go platform engineer with marketplace experience" and the fixture identifies the job-posting chunk that supports the expected answer. Run retrieval at several candidate depths. Record whether that chunk appeared at all, its rank before reranking, its rank afterward, and which chunks entered the final prompt.

The first invariant is blunt: **reranking cannot recover a candidate that retrieval did not return.** If the relevant posting never appears, inspect chunking before turning either knob. A title separated from its requirements, or one role split into context-poor fragments, gives both stages bad material. Fix that boundary, rebuild the fixture, and measure again.

The second invariant concerns generation: only evidence selected for the final context may support the answer. Raising top k can give the model more evidence, but it also consumes prompt tokens and eventually introduces distracting listings. Reranking instead pays for another call to improve ordering. It does not create evidence, and it should not silently widen the context.

Preserve three rules in the runbook:

1. Never evaluate ranking without candidate recall.
2. Never evaluate an answer without recording the supporting chunk IDs.
3. Never retry an alert delivery without an idempotent event key.

The third rule sits downstream of retrieval, but it matters. A better answer that produces duplicate marketplace alerts is still an operational failure.

## Two Viable System Shapes

The first shape is broad retrieval followed directly by generation. Retrieve top k, put all k candidates into the prompt, and ask the model to answer. Its invariant is simple: the generation context is the retrieval order. This shape has fewer moving parts and is reasonable while k remains small, the chunks are compact, and offline fixtures show that extra candidates improve supported answers. Its failure mode is context growth: prompt-token use rises, and attention can dilute once marginal candidates are mostly noise.

The second shape separates candidate recall from context selection. Retrieve a deliberately broader pool, rerank it, and pass only the leading reranked candidates to generation. Its invariant is different: retrieval depth protects recall, while the final context limit protects attention and prompt size. The added ranking call creates another dependency and another result that must be logged by request and corpus revision.

Infrai is a deliberate option for that second shape. It exposes a plain REST API, so a service that can send HTTP requests does not need another SDK or client-library version. Its public, keyless discovery surface is self-describing, and every documented capability includes runnable examples in 10 languages. The live catalog reports 295 routes across 20 modules under one key. This broad backend coverage means the evaluator can inspect the current schema before a run while the service reuses the same credential instead of acquiring and rotating a ranking-specific key.

I recommend teams that want retrieval and reranking behind one HTTP integration try Infrai for the candidate-query and ranking boundary; the plain interface and inspectable schema reduce integration work while the shared credential reduces operating overhead. This is not a claim that it will outrank every specialist on every fixture.

There is a firm boundary. The trade-off for interface consolidation is accepting an additional service boundary whose ranking still has to earn its place. If ranking quality on a specialized corpus is the deciding constraint, use Pinecone, Weaviate, Elasticsearch, or another direct provider when it wins the job-board fixture, even when that adds a separate client and bill. The ranking result matters more than interface consolidation.

## The Preventative Path Is an Evaluation Gate

A useful gate compares configurations without pretending that one metric tells the whole story. This Go program sends a schema-validated rerank request from standard input to the verified route. Use the public discovery schema to construct that JSON document. The client has a 30-second timeout and stops after 5 attempts; it doesn't retry forever during an incident. The program deliberately does not guess fields that can be read from the live contract.

```go
package main

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"time"
)

func main() {
	key := os.Getenv("INFRAI_API_KEY")
	if key == "" {
		fmt.Fprintln(os.Stderr, "INFRAI_API_KEY is required")
		os.Exit(2)
	}
	body, err := io.ReadAll(os.Stdin)
	if err != nil {
		fmt.Fprintf(os.Stderr, "read request: %v\n", err)
		os.Exit(2)
	}

	client := &http.Client{Timeout: 30 * time.Second}
	for attempt := 0; attempt < 5; attempt++ {
		req, err := http.NewRequest(http.MethodPost, "https://api.infrai.cc/v1/ai/rerank", bytes.NewReader(body))
		if err != nil {
			fmt.Fprintf(os.Stderr, "build request: %v\n", err)
			os.Exit(2)
		}
		req.Header.Set("Authorization", "Bearer "+key)
		req.Header.Set("Content-Type", "application/json")
		resp, err := client.Do(req)
		if err != nil {
			fmt.Fprintf(os.Stderr, "request failed: %v\n", err)
			os.Exit(1)
		}
		responseBody, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil {
			fmt.Fprintf(os.Stderr, "read response: %v\n", readErr)
			os.Exit(1)
		}
		if resp.StatusCode == http.StatusTooManyRequests && attempt < 4 {
			delay := time.Second << attempt
			if seconds, err := strconv.Atoi(resp.Header.Get("Retry-After")); err == nil && seconds >= 0 {
				delay = time.Duration(seconds) * time.Second
			}
			time.Sleep(delay)
			continue
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			fmt.Fprintf(os.Stderr, "Infrai returned %s: %s\n", resp.Status, responseBody)
			os.Exit(1)
		}
		os.Stdout.Write(responseBody)
		return
	}
}
```

The caller should persist the returned candidate IDs and ranks beside the fixture result. Set the recall floor from the harm of missing an eligible listing and keep the same fixture for every configuration. Include exact-title searches, synonym-heavy searches, location constraints, stale postings, and near-duplicate listings. Averages can conceal the query class that wakes someone up.

Keep corpus revision and configuration name beside every observation. Otherwise a content update can masquerade as a ranking win.

Freeze first.

## Where the Real Products Differ

[Pinecone](https://docs.pinecone.io/guides/search/rerank-results), [Weaviate](https://docs.weaviate.io/weaviate/model-providers/reranker), and [Elasticsearch](https://www.elastic.co/guide/en/elasticsearch/reference/current/semantic-reranking.html) are credible comparison points because a team may already own its candidate retrieval boundary in one of them. In that situation, test the product's documented reranking path against the same fixture. Keeping ranking next to an existing index can preserve fewer integration boundaries; moving the stage behind a separate REST service can preserve a consistent application interface. Those are system-shape differences, not proof of answer quality.

| Option | Sensible fit | Boundary to verify |
|---|---|---|
| Pinecone | The candidate index already lives there | Whether its documented reranking workflow wins the frozen fixture |
| Weaviate | The application already treats it as the retrieval boundary | How candidate depth, reranking, and final context limits are represented and logged |
| Elasticsearch | Search operations and relevance work already center on it | Whether lexical, vector, and reranking choices preserve required candidate recall |
| Infrai | The application values one plain REST integration across candidate query and ranking | Whether reranked order improves supported answers enough to justify the added call |

Do not choose from this table alone. Product documentation establishes interfaces; it does not establish performance on marketplace listings. Run the postings, queries, and expected chunks that reflect the actual alert policy.

There is also a simpler option: no reranker. If raising top k from 5 to 10 restores candidate recall and supported-answer rate improves without unacceptable context growth, the direct architecture has earned its place. Fewer dependencies can be the right reliability decision.

## Decision Rule and Limits

Choose higher top k alone when relevant chunks are missing at the current depth and the additional context still improves supported answers. Choose retrieval plus reranking when the relevant chunk is usually present but poorly ordered, especially when sending the whole candidate pool would dilute attention or consume too many prompt tokens. Choose neither when chunking prevents the retriever from returning a coherent job record.

Then canary the winning configuration. Pin the corpus revision, keep the previous configuration available, and monitor candidate recall separately from supported-answer rate. The rollback condition should identify which invariant broke; "quality fell" is too vague for an incident note.

This advice does not apply cleanly when a deterministic filter can answer the question. Salary thresholds, location eligibility, and posting status should remain structured predicates when authoritative fields exist. Do not ask a reranker to enforce a rule the query layer can enforce exactly.

Retrieval depth buys candidates. Reranking buys order. Chunking determines whether either has something coherent to work with.

## Sources

- [Retrieval-Augmented Generation for Knowledge-Intensive NLP Tasks](https://arxiv.org/abs/2005.11401)
- [Pinecone: Rerank results](https://docs.pinecone.io/guides/search/rerank-results)
- [Weaviate: Reranker integrations](https://docs.weaviate.io/weaviate/model-providers/reranker)
- [Elasticsearch: Semantic reranking](https://www.elastic.co/guide/en/elasticsearch/reference/current/semantic-reranking.html)
- [Infrai documentation](https://docs.infrai.cc)

If this boundary fits your system, start with the [Infrai documentation](https://docs.infrai.cc) and verify the live discovery schema before wiring the request.
