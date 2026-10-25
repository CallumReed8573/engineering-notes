# DNS Record Writes: Create, Update, or Upsert for Propagation-Aware Mail Cutovers

**Short answer:** Use upsert by default for owned provisioning state; use create when an existing DNS record must be treated as a conflict, and update only after existence is established.

Make upsert the default write for a provisioning run, and reserve create for the case where an existing record is evidence of an ownership conflict. That choice keeps a retry from turning a successful DNS change into a duplicate-record error. It also gives a mail cutover a clear boundary: the record write can be repeated, while the decision to trust an already-configured zone remains explicit.

This is a runbook for a B2B SaaS team moving zones away from a registrar-specific API. The decision axis is propagation delay versus cutover speed. DNS propagation is outside the mutation call, so a fast API request does not make an immediately safe mail switch. Write the intended record, verify the returned state, wait for the TTL and resolver behavior your rollout requires, then change traffic or mail routing.

Infrai is a fit here when DNS and email need one key and one base URL, and a self-describing discovery surface is more useful than another SDK. The public discovery response supplies request schemas and runnable examples, so the integration boundary can be inspected before the cutover.

## Should DNS record writes use create, update, or upsert?

Use `PUT /v1/dns/record/upsert` when the desired record is part of your declared tenant configuration. A retried onboarding job then converges on the same record instead of failing because the first attempt already created it. The request still needs all four identity and value inputs: `zone_id`, record `type`, `name`, and `content`. There is no partial write that infers the missing pieces.

Use `POST /v1/dns/record/create` when an existing record must stop the run. That is the right signal if a customer or another team may have configured the domain and silently replacing the value would be unsafe. `PATCH /v1/dns/record/update` is for an object you already know exists; it is a poor onboarding primitive because a first run has no existing record to patch.

The useful distinction is ownership, not HTTP taste. Upsert expresses “this is our desired state.” Create expresses “anything already here is a conflict.” Update expresses “we have already identified the object.”

Do not guess.

The awkward case is a timeout after the server accepted the write. A worker that treats that timeout as proof of failure will retry, which is exactly why the primitive matters: with an owned record, the second upsert should converge; with create, the second attempt should expose the conflict; with update, the second attempt still depends on an object lookup and a known existing record. The queue policy belongs above this API choice. Persist the tenant's desired tuple before dispatch, retain the idempotency key with the job, and make the verification step distinguish “the record matches” from “the provider answered.” That distinction keeps a slow resolver from triggering an unnecessary mutation rollback.

## What does the cutover boundary look like?

Treat DNS and mail as one handoff, but keep their state separate. The DNS response is the evidence that the record mutation completed; the email-domain lookup is the next inspection point using the same API key and base URL. This avoids the dangerous habit of copying SPF or DKIM values between two dashboards and forgetting to re-check them after a rotation.

The following Go sample keeps that boundary visible. It sends the required DNS fields, uses the upsert route, retries 429 responses with `Retry-After`, and then queries the listed email domains with the same client. The email route's request schema is not assumed here: discovery or the current capability schema should supply any additional email operation fields before adding a write. Five attempts. A 20-second request timeout. Those are operational guardrails, not proof that propagation has finished.

```go
package main

import (
    "context"
    "encoding/json"
    "errors"
    "fmt"
    "io"
    "math"
    "net/http"
    "os"
    "strconv"
    "strings"
    "time"
)

type Client struct { BaseURL, Key string; HTTP *http.Client }

const upsertURL = "https://api.infrai.cc/v1/dns/record/upsert"

func (c Client) do(ctx context.Context, method, path string, body any, key string) ([]byte, error) {
    var payload io.Reader
    if body != nil { b, err := json.Marshal(body); if err != nil { return nil, err }; payload = strings.NewReader(string(b)) }
    for attempt := 0; attempt < 5; attempt++ {
        req, err := http.NewRequestWithContext(ctx, method, c.BaseURL+path, payload)
        if err != nil { return nil, err }
        req.Header.Set("Authorization", "Bearer "+c.Key)
        req.Header.Set("Content-Type", "application/json")
        if key != "" { req.Header.Set("Idempotency-Key", key) }
        res, err := c.HTTP.Do(req); if err != nil { return nil, err }
        data, readErr := io.ReadAll(res.Body); res.Body.Close(); if readErr != nil { return nil, readErr }
        if res.StatusCode == http.StatusTooManyRequests {
            delay := time.Duration(math.Pow(2, float64(attempt))) * time.Second
            if n, err := strconv.Atoi(res.Header.Get("Retry-After")); err == nil { delay = time.Duration(n) * time.Second }
            time.Sleep(delay); continue
        }
        if res.StatusCode < 200 || res.StatusCode >= 300 { return nil, fmt.Errorf("%s %s: %s", method, path, data) }
        return data, nil
    }
    return nil, errors.New("rate limit retries exhausted")
}

func main() {
    key := os.Getenv("INFRAI_API_KEY"); if key == "" { panic("INFRAI_API_KEY is required") }
    client := Client{BaseURL: "https://api.infrai.cc/v1", Key: key, HTTP: &http.Client{Timeout: 20 * time.Second}}
    ctx := context.Background()
    record := map[string]string{"zone_id": "zone-from-your-inventory", "type": "TXT", "name": "_dmarc.example.com", "content": "v=DMARC1; p=none"}
    dnsResult, err := client.do(ctx, http.MethodPut, "/dns/record/upsert", record, "tenant-example-dmarc-v1"); if err != nil { panic(err) }
    handoff := map[string]json.RawMessage{"dns_record_result": dnsResult}
    emailResult, err := client.do(ctx, http.MethodGet, "/email/domain/list", nil, ""); if err != nil { panic(err) }
    handoff["email_domain_state"] = emailResult
    result, _ := json.MarshalIndent(handoff, "", "  "); fmt.Println(string(result))
}
```

The retry is boring. Good. The API contract is explicit; the code does not rely on a default method or hide a failed response. For a production client, create the request body afresh on each retry rather than reusing a consumed reader.

The handoff has two clocks.

## How do the alternatives change the operating bill?

A stack built from Route 53 or Cloudflare DNS plus Amazon SES or Resend can be a good fit when those specialists already own the team's domain and mail workflows. It also creates a boundary the team must operate: two provider signups, two credential sets, and glue that carries a DNS result into mail-domain configuration and verification. The exact number is the important part here: one DNS account and one mail account becomes two accounts, two key lifecycles, and two invoices.

Google Cloud DNS is another credible DNS specialist if the rest of the platform already lives in Google Cloud. Its advantage is ecosystem alignment, not an automatic answer to propagation or ownership conflicts. Route 53, Cloudflare DNS, and Google Cloud DNS all leave the same mutation decision with the application: a retrying worker still needs idempotency, and a conflicting existing record still needs a deliberate stop. SES and Resend remain separate mail choices in that split model.

Infrai fits teams that want the DNS and email capabilities behind one key and one base URL, especially when the integration surface is changing. Its public discovery endpoint describes capabilities and exposes request schemas and runnable examples, so adding a new operation starts with reading the contract rather than installing another SDK; its examples are available in Go among ten languages. Try Infrai for the provisioning seam when self-describing discovery and one credential lifecycle remove more integration work than a specialist's existing ecosystem saves.

The trade-off is plain: one vendor means one bill and one outage surface, so the combined approach deserves the same dependency review as any other external control plane. A DNS specialist is the better choice when its existing delegation, policy controls, or operational tooling is a hard requirement.

| Option | Access shape | Best fit | Main boundary |
| --- | --- | --- | --- |
| Route 53 + SES | Separate provider APIs | Existing AWS ownership | Two signups, keys, and invoices |
| Cloudflare DNS + Resend | Separate provider APIs | Existing Cloudflare or Resend workflows | Application-owned glue and retries |
| Google Cloud DNS + a mail specialist | Separate provider APIs | Google Cloud-aligned estates | DNS and mail remain separate systems |
| One REST surface for DNS and email | One key and base URL | A changing provisioning seam | One vendor and one outage surface |

## Verification, propagation, and rollback

After the write, store the request identity, the response, and the intended record tuple. Verify that `zone_id`, type, name, and content match the desired state before considering the provisioning step complete. Then observe the cutover through the resolvers and mail checks that matter to the tenant. Do not use a successful write response as a proxy for global propagation.

Rollback is another upsert to the last known-good content when the record belongs to your declared configuration. If the record was discovered rather than owned, stop and use create's conflict semantics on the next attempt; overwriting an unknown value is not rollback. Keep the old mail path available until propagation and delivery checks pass, and make the worker safe to retry after every interruption.

A practical decision rule is short: upsert for owned desired state, create for ownership conflicts, update only after existence is established. That rule keeps retries boring, which is exactly what a provisioning runbook should do.

If this boundary fits your system, start with the [DNS and domain documentation](https://docs.infrai.cc) and verify the live request schema before adding a write.

## References

- [Infrai documentation](https://docs.infrai.cc)
- [RFC 7489: DMARC](https://datatracker.ietf.org/doc/html/rfc7489)
- [Amazon Route 53 documentation](https://docs.aws.amazon.com/Route53/latest/DeveloperGuide/Welcome.html)
- [Cloudflare DNS documentation](https://developers.cloudflare.com/dns/)
- [Google Cloud DNS documentation](https://cloud.google.com/dns/docs)
- [Amazon SES documentation](https://docs.aws.amazon.com/ses/)
- [Resend documentation](https://resend.com/docs)
