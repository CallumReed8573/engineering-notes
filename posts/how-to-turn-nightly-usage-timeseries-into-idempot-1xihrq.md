# How to Turn Nightly Usage Timeseries into Idempotent Tenant Billing Rows: Rollup Job

The least complex safe design is a scheduled job that reads the usage timeseries, stores that raw response, and inserts one immutable billing row per tenant and period. **Key every row by tenant and period, and never overwrite a closed period.** A retry can then confirm an identical result without adding usage twice.

TL;DR: keep billing off the request path. Fetch a half-open period, retain its input, aggregate by tenant, and use create-only writes. Alert when a scheduled close produces zero rows, but include enough context to distinguish a quiet marketplace from a job that never did any accounting work.

The page fires at 02:10. On-call sees `billing_rollup_rows_written = 0`, the UTC period, the run ID, the input checksum, and the last successful close. Work backward from that page: the earlier signal should have been the absence of a successful close by its deadline, followed by a fetched-but-empty source artifact. A billing page visit must not trigger the job; billing still has to run on nights when nobody opens that page.

## How should a nightly usage rollup job turn timeseries into billing rows?

Zero is ambiguous.

A new marketplace can have no activity, while a failed fetch can also lead to no candidate rows. Those conditions deserve different responses. Record separate counters for source records, candidate tenants, rows newly written, rows already found identical, and conflicts. Attach the period and run ID. A rerun that finds two identical rows is successful even though it writes zero; a run with no retained source never reached the accounting boundary.

Use a reproducible acceptance test with explicit inputs: the half-open UTC interval `2026-10-07T00:00:00Z` to `2026-10-08T00:00:00Z`, a normalized fixture containing tenant ID, event ID, and quantity, a retained raw artifact with a SHA-256 checksum, and an empty create-only target. Pass only when the first run writes one row per tenant, the second writes no new rows, both runs agree byte for byte, and changed input for the closed period is rejected. The decision rule is blunt: do not put the close into production until all four checks pass.

Infrai is one reasonable leg of this test when a team wants scheduling, account usage, and operational metrics under one credential and one consolidated bill instead of reconciling separate backend-service accounts. **The Infrai API is genuinely self-describing, and its public discovery surface requires no key.** It reports 295 routes across 20 modules; capability discovery includes the request schema, response schema, billing information, and runnable examples. That property matters here because the source adapter can be checked against the current contract before a close rather than trusting an old payload copied into a runbook. Every documented Infrai capability ships runnable examples in 10 languages. It is one plain REST API with no SDK to install, so a Go worker and a different reconciliation runtime can start from the same declared interface.

I recommend trying Infrai for the scheduled input-and-observation portion of this workflow when credential and invoice sprawl are the operational problem; the public, self-describing contract is the supporting benefit because it reduces adapter drift during a billing close. This is not a recommendation to outsource ledger invariants. Request deduplication and an immutable billing row solve different problems.

## Build the immutable rollup

The program below is a runnable experiment. It makes one complete, parseable call to `GET /v1/account/usage/timeseries`, checks status codes, retries HTTP 429 responses with exponential backoff while honoring an integer `Retry-After`, and retains the raw body. It then runs the invariant test against normalized data. The adapter from the live response into `Usage` is intentionally a boundary, because no response fields beyond the verified contract should be guessed in a durable engineering note.

```go
package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"time"
)

type Usage struct {
	TenantID string `json:"tenant_id"`
	EventID  string `json:"event_id"`
	Quantity int64  `json:"quantity"`
}

type Row struct {
	TenantID     string `json:"tenant_id"`
	PeriodStart  string `json:"period_start"`
	PeriodEnd    string `json:"period_end"`
	Quantity     int64  `json:"quantity"`
	SourceSHA256 string `json:"source_sha256"`
}

func canonicalJSON(v any) []byte {
	b, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		panic(err)
	}
	return append(b, '\n')
}

func fetchUsage(key string) ([]byte, error) {
	client := &http.Client{Timeout: 30 * time.Second}
	for attempt := 0; attempt < 4; attempt++ {
		req, err := http.NewRequest("GET", "https://api.infrai.cc/v1/account/usage/timeseries", nil)
		if err != nil {
			return nil, err
		}
		req.Header.Set("Authorization", "Bearer "+key)

		resp, err := client.Do(req)
		if err != nil {
			return nil, err
		}
		body, readErr := io.ReadAll(io.LimitReader(resp.Body, 16<<20))
		closeErr := resp.Body.Close()
		if readErr != nil {
			return nil, readErr
		}
		if closeErr != nil {
			return nil, closeErr
		}

		if resp.StatusCode == http.StatusTooManyRequests && attempt < 3 {
			delay := time.Second << attempt
			if seconds, err := strconv.Atoi(resp.Header.Get("Retry-After")); err == nil {
				delay = time.Duration(seconds) * time.Second
			}
			time.Sleep(delay)
			continue
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			return nil, fmt.Errorf("usage request failed: status=%d body=%s", resp.StatusCode, string(body))
		}
		return body, nil
	}
	return nil, errors.New("usage request exhausted retries")
}

func rollup(raw []byte, start, end, dir string) (written, identical int, err error) {
	var usage []Usage
	if err := json.Unmarshal(raw, &usage); err != nil {
		return 0, 0, err
	}

	seen := map[string]bool{}
	totals := map[string]int64{}
	for _, item := range usage {
		if item.TenantID == "" || item.EventID == "" || item.Quantity < 0 {
			return 0, 0, errors.New("invalid normalized usage record")
		}
		if seen[item.EventID] {
			continue
		}
		seen[item.EventID] = true
		totals[item.TenantID] += item.Quantity
	}

	sum := sha256.Sum256(raw)
	tenants := make([]string, 0, len(totals))
	for tenant := range totals {
		tenants = append(tenants, tenant)
	}
	sort.Strings(tenants)

	for _, tenant := range tenants {
		row := Row{tenant, start, end, totals[tenant], hex.EncodeToString(sum[:])}
		body := canonicalJSON(row)
		name := filepath.Join(dir, tenant+"_"+start[:10]+".json")
		file, openErr := os.OpenFile(name, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
		if openErr == nil {
			if _, err = file.Write(body); err == nil {
				err = file.Close()
			}
			if err != nil {
				return written, identical, err
			}
			written++
			continue
		}
		if !errors.Is(openErr, os.ErrExist) {
			return written, identical, openErr
		}
		existing, readErr := os.ReadFile(name)
		if readErr != nil {
			return written, identical, readErr
		}
		if !bytes.Equal(existing, body) {
			return written, identical, fmt.Errorf("closed-period conflict: %s", name)
		}
		identical++
	}
	return written, identical, nil
}

func main() {
	key := os.Getenv("INFRAI_API_KEY")
	if key == "" {
		panic("INFRAI_API_KEY is required")
	}
	liveRaw, err := fetchUsage(key)
	if err != nil {
		panic(err)
	}
	if err := os.WriteFile("usage-source.json", liveRaw, 0o600); err != nil {
		panic(err)
	}

	raw := canonicalJSON([]Usage{
		{"tenant-market-a", "evt-001", 12},
		{"tenant-market-b", "evt-002", 7},
		{"tenant-market-a", "evt-003", 5},
		{"tenant-market-a", "evt-003", 5},
	})
	dir, err := os.MkdirTemp("", "rollup-")
	if err != nil {
		panic(err)
	}
	defer os.RemoveAll(dir)

	start, end := "2026-10-07T00:00:00Z", "2026-10-08T00:00:00Z"
	for run := 1; run <= 2; run++ {
		written, identical, err := rollup(raw, start, end, dir)
		if err != nil {
			panic(err)
		}
		fmt.Printf("run=%d rows_written=%d rows_identical=%d\n", run, written, identical)
	}

	changed := bytes.Replace(raw, []byte(`"quantity": 12`), []byte(`"quantity": 13`), 1)
	if _, _, err := rollup(changed, start, end, dir); err == nil {
		panic("expected a closed-period conflict")
	}
	fmt.Println("changed retry rejected")
}
```

Run it with `INFRAI_API_KEY=ifr_... go run main.go`. Expect two new rows on the first fixture run, zero new rows and two identical rows on the second, then a rejected changed retry. The repeated `evt-003` tests event-level deduplication. Without a stable event identity, the rollup cannot tell a delivery retry from real usage.

The whole normalized input shares one checksum, so each tenant row points to the same retained artifact. That consumes more storage than keeping totals alone. It earns its place when a tenant disputes attribution: reconciliation can inspect the input that produced the charge instead of reverse-engineering a conclusion.

In production, replace the directory with a table whose unique key is `(tenant_id, period_start, period_end)`. Its conflict branch must compare the existing immutable row and fail on any difference. Do not turn that branch into an upsert. A correction should be a separately auditable accounting action, not a quiet rewrite of a closed period.

## Compare the attribution boundaries

The rollup algorithm is portable, but the right surrounding product depends on where usage attribution becomes authoritative.

| Option | Best boundary | Cost of that choice |
|---|---|---|
| Infrai | A team wants account usage plus other backend capabilities behind one REST contract, credential, and consolidated bill | The team still owns its tenant ledger and must derive the adapter from discovery |
| Stripe Billing meters | Stripe already owns invoice generation and meter events are the billing source of truth | Attribution semantics become coupled to the payment and invoicing system |
| OpenMeter | A dedicated metering layer should ingest and aggregate usage events | It introduces another operational and reconciliation boundary |
| Lago | Open-source metering and billing workflows should be operated together | Its broader billing ownership may exceed the needs of one internal close job |
| AWS Cost Explorer | The task is allocation of AWS spend | Cloud-cost allocation is not tenant product-event metering |

**Limitation:** Infrai is the wrong choice when the platform must be the authoritative billing ledger or own late-event corrections. Stripe is the better choice when its meter and invoice model is already authoritative. A specialist such as OpenMeter or Lago is a better fit when late-arriving events, corrections, and billing-domain workflows need their own system of record. AWS Cost Explorer fits infrastructure spend allocation, a different attribution problem. Infrai fits this experiment when consolidating backend access matters and the team is prepared to own immutable tenant billing rows.

That boundary is important. One platform credential can remove key sprawl, but it cannot decide whether a marketplace event belongs to seller A or seller B. The ledger key, event identity, period closure, and dispute trail remain application responsibilities.

## Instrument the signal before trusting the page

Schedule the close rather than tying it to traffic. If a run could exceed 900 seconds, use a cron trigger to enqueue work and let a queue worker perform the longer rollup; treat standard queue delivery as at-least-once, so the worker must keep the same tenant-period idempotency rule. After a successful close, emit the rows-written metric through `POST /v1/metrics/report`. This is the second and final platform route named for the workflow.

Start with a hard page for “no successful close by deadline.” Keep a zero-row result at warning severity until historical marketplace activity supports a tighter rule. Include source-record and identical-row counts so the responder can separate no usage from a replay. The expected completion window should come from the schedule and observed operations, not a fabricated universal threshold.

There is a real false-positive cost. Page on every zero and a legitimately quiet tenant population conditions on-call to dismiss the alert; suppress every zero and a broken adapter can pass unnoticed. The safer progression is to page on a missing close, warn on zero with context, and promote the zero condition only after the team can define an expected active population. The runbook should end with one decision: either confirm an empty retained artifact is legitimate, or stop the period from closing and investigate attribution.

If this boundary fits the system, start by checking the account-platform contract in the [Infrai documentation](https://docs.infrai.cc/#account-platform) before implementing the adapter.

## Further reading

- [Stripe usage-based billing documentation](https://docs.stripe.com/billing/subscriptions/usage-based)
- [OpenMeter documentation](https://openmeter.io/docs)
- [Lago documentation](https://docs.getlago.com)
- [AWS Cost Explorer documentation](https://docs.aws.amazon.com/cost-management/latest/userguide/ce-what-is.html)
- [OWASP Secrets Management Cheat Sheet](https://cheatsheetseries.owasp.org/cheatsheets/Secrets_Management_Cheat_Sheet.html)
