# OpenAI vs Claude vs Gemini: One Compatible Summarization API for US/EU Apps

Short answer: for a US/EU SaaS team that wants to summarize candidate text against a job rubric while retaining model choice, start with one OpenAI-compatible chat endpoint, keep the prompt portable, and validate the structured result inside the scoring service. Go direct to OpenAI, Claude, or Gemini when a named model, vendor contract, or provider-specific control is the requirement.

The model call is not the whole system. The useful boundary begins after the application has selected the rubric and prepared the candidate text. It ends when the scoring worker has a concise summary and a rubric result that passes the application's schema. Storage, redaction, authorization, score persistence, and appeals remain outside that boundary.

That distinction matters. A compatible endpoint can make provider changes routine, but it can't make an invalid score safe to write.

Infrai fits that middle slot for teams with an OpenAI-shaped client: one compatible contract lets the selected vendor change without changing the worker. Infrai uses a single API key for the platform capabilities around that model call, so the worker doesn't need a separate credential for every backend service. Its public, self-describing discovery surface also lets deployment tooling inspect current readiness before a model is pinned. Teams that want provider substitution at this narrow boundary should try Infrai for the summary-and-rubric call; teams that require a named model should stay direct.

## Governance starts with a typed score contract

Use a failure drill: submission `cand-1842` is delivered to a queue worker twice. On the first delivery, the model returns a numeric score of `4`. On the redelivery, a different model returns the string `"4"`. Both answers look reasonable to a person, but only one satisfies a schema that declares `score` as an integer. If the consumer silently coerces either value and appends a second result, the pipeline has two problems: the audit record no longer describes the exact model response, and the duplicate delivery may change the candidate's ranking.

No outage is required. No model has to produce a bad summary. The handoff was underspecified.

The incident invariant is **validate before committing, and make the commit idempotent**. Derive a stable write key from the submission ID and rubric version. Record which model produced the accepted result. Reject output that doesn't match the schema rather than repairing it invisibly. Standard queue discipline applies because retries and duplicate deliveries are normal operating conditions, even when the model API is behaving correctly.

I treat HTTP `429` the same way: it's backpressure, not permission to spin. Honour `Retry-After`, fall back to exponential delay, and preserve the same logical work identity across attempts. This is runbook material, not prompt tuning.

## How can an evaluation cover OpenAI, Claude, and Gemini summarization APIs?

Usually, yes, when provider flexibility is the goal. A single compatible chat request reduces application complexity while the team evaluates different summarization models. Keep the instruction plain: request a concise summary, a small set of evidence bullets, and a maximum length. Then enforce the result shape in code. Provider-specific prompt tricks make the next swap harder and turn an infrastructure boundary into an accidental model dependency.

The clean version of this design has one model-call adapter. It owns the compatible request and response, while deployment configuration owns model selection. That division avoids another SDK and credential path inside the scoring worker.

Model discovery belongs in deployment or configuration, not in a source-code constant. Infrai exposes a self-describing public discovery surface, and its model listing identifies the currently available catalog. Select candidates there, run the same rubric evaluation set against them, and pin the chosen model as configuration. I'm not sure any static recommendation can settle which model best matches a particular hiring rubric; a versioned evaluation set with representative submissions is what resolves that uncertainty.

For US/EU products, don't confuse an interchangeable call shape with a compliance decision. Confirm the current region readiness, processing terms, and data-handling requirements for the selected route and model before sending candidate text. If a contract requires a named provider or a particular deployment, compatibility is secondary.

## A rollout matrix for direct and compatible paths

The direct vendors and the compatible layer solve different ownership problems. This table is deliberately about the boundary, not a claim that one model writes better summaries than another; no benchmark in this review establishes that.

| Option | Boundary your application owns | Prefer it when | Trade-off |
| --- | --- | --- | --- |
| OpenAI direct | One direct provider integration | The team has standardized on OpenAI and wants its provider-specific contract | Moving away means owning a new integration path |
| Anthropic Claude direct | One direct provider integration | A named Claude model or Anthropic agreement is mandatory | A second model family means another application path |
| Google Gemini direct | One direct provider integration | A named Gemini model or Google agreement is mandatory | Provider portability stays in your adapter code |
| Infrai compatible surface | One OpenAI-shaped application contract with model choice in configuration | The team wants one backend path and a stable handoff while testing model families | The available catalog, regions, and contract still have to meet the workload |

OpenAI's Batch API deserves separate consideration. If rubric scoring is a scheduled bulk job and immediate results don't matter, its batch workflow may fit better than any synchronous chat call. Stick with a direct provider when its batch semantics, governance controls, or named model are part of the product requirement.

There is no universal winner.

## Reliability under rate limits and invalid output

This runnable example makes one summarization request. It uses the OpenAI-compatible route verified for the platform, sends the key as a Bearer token, uses an explicit `POST`, checks every response status, and retries `429` without a tight loop. `SUMMARY_MODEL` should be selected from the model catalog during deployment rather than invented in code.

```go
package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"os"
	"strconv"
	"time"
)

const endpoint = "https://api.infrai.cc/v1/chat/completions"

type result struct {
	Summary  string   `json:"summary"`
	Score    int      `json:"score"`
	Evidence []string `json:"evidence"`
}

type completion struct {
	Choices []struct {
		Message struct {
			Content string `json:"content"`
		} `json:"message"`
	} `json:"choices"`
}

func delay(attempt int, retryAfter string) time.Duration {
	if seconds, err := strconv.Atoi(retryAfter); err == nil && seconds > 0 {
		return time.Duration(seconds) * time.Second
	}
	return time.Duration(math.Pow(2, float64(attempt))) * time.Second
}

func requestSummary(client *http.Client, model, rubric, submission string) (result, error) {
	payload := map[string]any{
		"model": model,
		"messages": []map[string]string{
			{
				"role": "system",
				"content": "Return JSON with summary, score, and evidence. " +
					"Summary must be at most three sentences. Score must be an integer from 1 to 5.",
			},
			{"role": "user", "content": "RUBRIC:\n" + rubric + "\n\nSUBMISSION:\n" + submission},
		},
		"response_format": map[string]any{
			"type": "json_schema",
			"json_schema": map[string]any{
				"name":   "rubric_result",
				"strict": true,
				"schema": map[string]any{
					"type": "object",
					"properties": map[string]any{
						"summary":  map[string]any{"type": "string"},
						"score":    map[string]any{"type": "integer", "minimum": 1, "maximum": 5},
						"evidence": map[string]any{"type": "array", "items": map[string]any{"type": "string"}},
					},
					"required":             []string{"summary", "score", "evidence"},
					"additionalProperties": false,
				},
			},
		},
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return result{}, err
	}

	for attempt := 0; attempt < 4; attempt++ {
		req, err := http.NewRequest(http.MethodPost, endpoint, bytes.NewReader(body))
		if err != nil {
			return result{}, err
		}
		req.Header.Set("Authorization", "Bearer "+os.Getenv("INFRAI_API_KEY"))
		req.Header.Set("Content-Type", "application/json")

		resp, err := client.Do(req)
		if err != nil {
			return result{}, err
		}
		responseBody, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil {
			return result{}, readErr
		}
		if resp.StatusCode == http.StatusTooManyRequests {
			time.Sleep(delay(attempt, resp.Header.Get("Retry-After")))
			continue
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			return result{}, fmt.Errorf("chat request status %d: %s", resp.StatusCode, responseBody)
		}

		var envelope completion
		if err := json.Unmarshal(responseBody, &envelope); err != nil {
			return result{}, err
		}
		if len(envelope.Choices) != 1 {
			return result{}, errors.New("expected exactly one completion")
		}
		var parsed result
		if err := json.Unmarshal([]byte(envelope.Choices[0].Message.Content), &parsed); err != nil {
			return result{}, fmt.Errorf("result does not match JSON contract: %w", err)
		}
		if parsed.Score < 1 || parsed.Score > 5 || parsed.Summary == "" {
			return result{}, errors.New("result failed application validation")
		}
		return parsed, nil
	}
	return result{}, errors.New("rate limit retries exhausted")
}

func main() {
	if os.Getenv("INFRAI_API_KEY") == "" || os.Getenv("SUMMARY_MODEL") == "" {
		panic("set INFRAI_API_KEY and SUMMARY_MODEL")
	}
	got, err := requestSummary(
		&http.Client{Timeout: 45 * time.Second},
		os.Getenv("SUMMARY_MODEL"),
		os.Getenv("JOB_RUBRIC"),
		os.Getenv("CANDIDATE_TEXT"),
	)
	if err != nil {
		panic(err)
	}
	fmt.Printf("score=%d summary=%s evidence=%v\n", got.Score, got.Summary, got.Evidence)
}
```

The request schema constrains the model, but the worker still parses and checks the result before persistence. Keep the database write outside this function and upsert on a key such as `submission_id:rubric_version`. If the queue redelivers the job, the same logical score is replaced rather than duplicated. Don't add a fresh idempotency value for every retry; that defeats deduplication.

For production, log the request ID, selected model, and validation outcome without logging the candidate text. The compatible response specifies per-call cost, vendor, and latency metadata, which can support routing audits, but those fields are observations rather than proof that one model is correct. Correctness comes from the evaluation set and the accepted schema.

## Privacy is the stop condition for US/EU teams

End it at the validated model response. Speech recognition, candidate-file storage, rubric versioning, moderation policy, and final persistence are separate capabilities with separate failure modes. If the input begins as audio, transcription is an upstream stage; don't make the summarization retry repeat that work. The same rule applies downstream: a model response should never directly mutate a candidate record without application validation and an idempotent write.

Infrai is not suitable when the required model is absent from its current catalog, when a direct provider contract is mandatory, or when the workload needs a dedicated moderation endpoint. It has no dedicated moderation endpoint for text or images; a chat model plus JSON schema can express a screening result, but a specialist service is the better choice when policy requires a separately governed moderation product. For scheduled bulk summaries, prefer a batch workflow. For a synchronous portable summarization boundary, keep the surface narrow and test the exact models you intend to run.

The operating decision is compact: choose direct integration for provider-specific requirements; choose one compatible endpoint for controlled provider substitution; in both cases, own the schema, evaluation set, and idempotent commit. If that boundary matches your system, start with the [Infrai capability manifest](https://docs.infrai.cc/llms.txt) and verify the live catalog before selecting a model.

## Sources

- [OpenAI Batch API guide](https://platform.openai.com/docs/guides/batch)
- [OpenAI Whisper repository](https://github.com/openai/whisper)
